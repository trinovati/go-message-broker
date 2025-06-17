// this rabbitmq package is adapting the amqp091-go lib.
package rabbitmq

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	dto_broker "github.com/trinovati/go-message-broker/v3/pkg/dto"

	amqp "github.com/rabbitmq/amqp091-go"
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
	var err error
	var channelDropSignal chan struct{} = make(chan struct{}, 1)
	var channelUpSignal chan struct{} = make(chan struct{}, 1)
	var incomingDeliveryChannel <-chan amqp.Delivery
	var timer *time.Timer = time.NewTimer(10 * time.Second)
	defer timer.Stop()

	consumer.mutex.Lock()

	if consumer.isRunning {
		consumer.logger.WarnContext(ctx, "consumer is already running ConsumeForever", consumer.logGroup)
		consumer.mutex.Unlock()
		return
	}

	consumer.channelClosureNotify = consumer.channel.RegisterConsumer(consumer.Id)

	consumer.consumerCtx, consumer.consumerCancel = context.WithCancel(ctx)
	consumer.isRunning = true

	consumer.mutex.Unlock()

	go consumer.amqpChannelMonitor(consumer.consumerCtx, channelDropSignal, channelUpSignal)
	go consumer.acknowledgeWorker(consumer.consumerCtx)

	for {
		select {
		case <-consumer.consumerCtx.Done():
			consumer.logger.InfoContext(consumer.consumerCtx, "gracefully closing consumer due to context closure", consumer.logGroup)
			return

		default:
			incomingDeliveryChannel, err = consumer.prepareLoopingConsumer(consumer.consumerCtx)
			if err != nil {
				consumer.logger.ErrorContext(consumer.consumerCtx, "error preparing consumer", slog.Any("error", err), consumer.logGroup)
				continue
			}
		}

		break
	}

	for {
		select {
		case <-consumer.consumerCtx.Done():
			consumer.logger.InfoContext(consumer.consumerCtx, "gracefully closing consumer due to context closure", consumer.logGroup)
			return

		case delivery, ok := <-incomingDeliveryChannel:
			if !ok {
				consumer.logger.DebugContext(consumer.consumerCtx, "incoming deliveries channel has been closed", consumer.logGroup)
				break
			}

			if delivery.Body == nil {
				consumer.logger.DebugContext(consumer.consumerCtx, "nil body has reached consumer", consumer.logGroup)
				continue
			}

			messageId := strconv.FormatUint(delivery.DeliveryTag, 10)

			consumer.DeliveryMap.Store(messageId, delivery)
			consumer.DeliveryChannel <- dto_broker.BrokerDelivery{
				Id:           messageId,
				Header:       delivery.Headers,
				Body:         delivery.Body,
				Acknowledger: consumer.AcknowledgeChannel,
			}
			continue
		}

		consumer.logger.DebugContext(consumer.consumerCtx, "consumer have lost access to deliveries channel", consumer.logGroup)

		select {
		case channelDropSignal <- struct{}{}:
			consumer.mutex.Lock()
			consumer.isConsuming = false
			consumer.mutex.Unlock()

		default:
			consumer.logger.DebugContext(consumer.consumerCtx, "consumer has already signaled the dropped channel", consumer.logGroup)
		}

		for {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(250 * time.Millisecond)

			select {
			case <-consumer.consumerCtx.Done():
				consumer.logger.InfoContext(consumer.consumerCtx, "gracefully closing consumer due to context closure", consumer.logGroup)
				return

			case <-timer.C:
				continue

			case <-channelUpSignal:
				for {
					select {
					case <-consumer.consumerCtx.Done():
						consumer.logger.InfoContext(consumer.consumerCtx, "gracefully closing consumer due to context closure", consumer.logGroup)
						return

					default:
						incomingDeliveryChannel, err = consumer.prepareLoopingConsumer(consumer.consumerCtx)
						if err != nil {
							consumer.logger.ErrorContext(consumer.consumerCtx, "error preparing consumer", slog.Any("error", err), consumer.logGroup)
							continue
						}

						consumer.mutex.Lock()
						consumer.isConsuming = true
						consumer.mutex.Unlock()
					}

					break
				}
			}

			break
		}

		consumer.logger.DebugContext(consumer.consumerCtx, "consumer channel is healthy", consumer.logGroup)
		continue
	}
}

func (consumer *RabbitMQConsumer) acknowledgeWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			consumer.logger.InfoContext(ctx, "gracefully closing acknowledge worker due to context closure", consumer.logGroup)
			return

		case acknowledge := <-consumer.AcknowledgeChannel:
			err := consumer.Acknowledge(acknowledge)
			if err != nil {
				consumer.logger.ErrorContext(ctx, "error acknowledging message", slog.Any("error", err), consumer.logGroup)
			}
		}
	}
}

/*
Prepare the consumer in case of the channel coming down.
*/
func (consumer *RabbitMQConsumer) amqpChannelMonitor(ctx context.Context, channelDropNotify <-chan struct{}, channelUpSignal chan<- struct{}) {
	timer := time.NewTimer(500 * time.Millisecond) // The timer is needed due to c.connectionClosureNotify being transitory because RabbitMQConnection may change.
	defer timer.Stop()

	for {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(500 * time.Millisecond)

		select {
		case <-ctx.Done():
			consumer.logger.InfoContext(ctx, "gracefully closing monitoring consumer channel due to context closure", consumer.logGroup)
			consumer.BreakConsume(ctx)
			return

		case <-consumer.channelClosureNotify:
			consumer.logger.InfoContext(ctx, "channel has gracefully closed, consumer is closing as well", consumer.logGroup)
			consumer.BreakConsume(ctx)
			return

		case <-channelDropNotify:
			consumer.logger.DebugContext(ctx, "consumer channel has been closed, waiting for recreation", consumer.logGroup)

			for {
				select {
				case <-ctx.Done():
					consumer.logger.InfoContext(ctx, "gracefully closing monitoring consumer channel due to context closure", consumer.logGroup)
					return

				default:
					err := consumer.channel.WaitForChannel(ctx, true)
					if err != nil {
						consumer.logger.ErrorContext(ctx, "error waiting for channel: ", slog.Any("error", err), consumer.logGroup)
						time.Sleep(500 * time.Millisecond)
						continue
					}

					consumer.channelTimesCreated = consumer.channel.TimesCreated

					channelUpSignal <- struct{}{}
				}

				break
			}

		case <-timer.C:
			continue
		}
	}
}

/*
Will create a connection, prepare channels, declare queue and exchange case needed.

If an error occurs, it will restart and retry all the process until the consumer is fully prepared.

Return a channel of incoming deliveries.
*/
func (consumer *RabbitMQConsumer) prepareLoopingConsumer(ctx context.Context) (incomingDeliveryChannel <-chan amqp.Delivery, err error) {
	for {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("stop preparing consumer due to context closure")

		default:
			err = consumer.channel.WaitForChannel(ctx, true)
			if err != nil {
				return nil, fmt.Errorf("error waiting for channel: %w", err)
			}

			err = consumer.PrepareQueue(ctx, false)
			if err != nil {
				consumer.logger.ErrorContext(ctx, "error preparing queue", slog.Any("error", err), consumer.logGroup)
				time.Sleep(500 * time.Millisecond)
				break
			}

			incomingDeliveryChannel, err = consumer.channel.Channel.Consume(consumer.Queue.Name, consumer.Name, false, false, false, false, nil)
			if err != nil {
				consumer.logger.ErrorContext(ctx, "error producing consume channel", slog.Any("error", err), consumer.logGroup)
				time.Sleep(500 * time.Millisecond)
				break
			}

			return incomingDeliveryChannel, nil
		}
	}
}
