// this rabbitmq package is adapting the amqp091-go lib
package rabbitmq

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
	dto_pkg "github.com/trinovati/go-message-broker/v2/dto"
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
	consumer.consumerCtx, consumer.consumerCancel = context.WithCancel(ctx)

	consumeChannelSignal := make(chan bool)
	incomingDeliveryChannel, err := consumer.prepareLoopingConsumer()
	if err != nil {
		log.Panic(errors.Wrap(err, "error preparing consumer queue"))
	}

	go consumer.amqpChannelMonitor(consumer.consumerCtx, consumeChannelSignal)
	go consumer.acknowledgeWorker(consumer.consumerCtx)

	for {
		select {
		case <-consumeChannelSignal:
			consumer.channel.WaitForChannel()
			incomingDeliveryChannel, err = consumer.prepareLoopingConsumer()
			if err != nil {
				log.Panic(errors.New("error preparing consumer queue"))
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
func (consumer *RabbitMQConsumer) prepareLoopingConsumer() (incomingDeliveryChannel <-chan amqp.Delivery, err error) {
	for tolerance := 0; tolerance >= 5 || consumer.AlwaysRetry; tolerance++ {
		consumer.channel.WaitForChannel()

		err = consumer.PrepareQueue()
		if err != nil {
			log.Printf("error from channel id %s preparing queue %s: %s\n", consumer.channel.ChannelId.String(), consumer.Queue.Name, err.Error())
			time.Sleep(time.Second)
			continue
		}

		if consumer.channel.IsChannelDown() {
			log.Printf("connection from channel id %s dropped before preparing consume for queue %s\n", consumer.channel.ChannelId.String(), consumer.Queue.Name)
			time.Sleep(time.Second)
			continue
		}

		incomingDeliveryChannel, err = consumer.channel.Channel.Consume(consumer.Queue.Name, "", false, false, false, false, nil)
		if err != nil {
			log.Printf("error from channel id %s producing consume channel for queue %s: %s\n", consumer.channel.ChannelId.String(), consumer.Queue.Name, err.Error())
			time.Sleep(time.Second)
			continue
		}

		return incomingDeliveryChannel, nil
	}

	if err == nil {
		err = errors.New(fmt.Sprintf("channel id %s could not prepare consumer for unknown reason for queue %s", consumer.channel.ChannelId.String(), consumer.Queue.Name))
	}

	return nil, err
}
