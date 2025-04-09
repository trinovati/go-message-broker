package rabbitmq

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	dto_pkg "github.com/trinovati/go-message-broker/v3/dto"
)

/*
Infinite loop consuming the queue linked to RabbitMQConsumer object.
It sends the data through channel found in Deliveries() method of RabbitMQConsumer object.

Use only in goroutines, otherwise the system will be forever blocked in the infinite loop trying to push into the channel.

It will open a goroutine to keep maintenance of the connection/channel.
It will open a goroutine as a worker for acknowledges.

In case of the connection/channel comes down, it prepares for consuming as soon as the connection/channel is up again.

Calling BreakConsume() method will close the nested goroutines and the ConsumeForever will return.
*/
func (consumer *RabbitMQConsumer) ConsumeForever(ctx context.Context) {
	consumer.consumerCtx, consumer.consumerCancel = context.WithCancel(context.Background())

	consumeChannelSignal := make(chan bool)
	incomingDeliveryChannel, err := consumer.prepareLoopingConsumer(ctx)
	if err != nil {
		consumer.logger.ErrorContext(ctx, "error preparing consumer", slog.Any("error", err), consumer.logGroup)
		os.Exit(1)
	}

	go consumer.amqpChannelMonitor(consumer.consumerCtx, consumeChannelSignal)
	go consumer.acknowledgeWorker(consumer.consumerCtx)

	for {
		select {
		case <-consumeChannelSignal:
			consumer.channel.WaitForChannel(ctx)
			incomingDeliveryChannel, err = consumer.prepareLoopingConsumer(ctx)
			if err != nil {
				consumer.logger.ErrorContext(ctx, "error preparing consumer", slog.Any("error", err), consumer.logGroup)
				os.Exit(1)
			}

			consumeChannelSignal <- true

		case delivery := <-incomingDeliveryChannel:
			if delivery.Body == nil {
				continue
			}

			messageId := strconv.FormatUint(delivery.DeliveryTag, 10)

			consumer.DeliveryMap.Store(messageId, delivery)
			consumer.DeliveryChannel <- dto_pkg.BrokerDelivery{
				Id:           messageId,
				Header:       delivery.Headers,
				Body:         delivery.Body,
				Acknowledger: consumer.AcknowledgeChannel,
			}
		case <-consumer.consumerCtx.Done():
			return
		}
	}
}

func (consumer *RabbitMQConsumer) acknowledgeWorker(ctx context.Context) {
	for {
		select {
		case acknowledge := <-consumer.AcknowledgeChannel:
			consumer.Acknowledge(acknowledge)
		case <-ctx.Done():
			return
		}
	}
}

/*
Prepare the consumer in case of the channel coming down.
*/
func (consumer *RabbitMQConsumer) amqpChannelMonitor(ctx context.Context, consumeChannelSignal chan bool) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			if consumer.channel.IsChannelDown() || consumer.channel.Connection().IsConnectionDown() {
				consumeChannelSignal <- false
				<-consumeChannelSignal

			} else {
				time.Sleep(250 * time.Millisecond)
				continue
			}
		}
	}
}

/*
Will create a connection, prepare channels, declare queue and exchange case needed.

If an error occurs, it will restart and retry all the process until the consumer is fully prepared.

Return a channel of incoming deliveries.
*/
func (consumer *RabbitMQConsumer) prepareLoopingConsumer(ctx context.Context) (incomingDeliveryChannel <-chan amqp.Delivery, err error) {
	for tolerance := 0; tolerance >= 5 || consumer.AlwaysRetry; tolerance++ {
		consumer.channel.WaitForChannel(ctx)

		err = consumer.PrepareQueue(ctx)
		if err != nil {
			consumer.logger.ErrorContext(ctx, "error preparing queue", slog.Any("error", err), consumer.logGroup)
			time.Sleep(time.Second)
			continue
		}

		if consumer.channel.IsChannelDown() {
			consumer.logger.WarnContext(ctx, "channel dropped before preparing consume")
			time.Sleep(time.Second)
			continue
		}

		incomingDeliveryChannel, err = consumer.channel.Channel.Consume(consumer.Queue.Name, "", false, false, false, false, nil)
		if err != nil {
			consumer.logger.ErrorContext(ctx, "error producing consume channel", slog.Any("error", err), consumer.logGroup)
			time.Sleep(time.Second)
			continue
		}

		return incomingDeliveryChannel, nil
	}

	if err == nil {
		err = fmt.Errorf("could not start consuming at queue %s due to unknown reason for consumer %s at channel id %s and connection id %s", consumer.Queue.Name, consumer.Name, consumer.channel.ChannelId, consumer.channel.Connection().ConnectionId)
	}

	return nil, err
}
