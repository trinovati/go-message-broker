package rabbitmq

import (
	"context"
	"fmt"

	constant_broker "github.com/trinovati/go-message-broker/v3/pkg/constant"

	amqp "github.com/rabbitmq/amqp091-go"
)

/*
This method is used to report to RabbitMQ the status of a received delivery.

Receive a dto as parameter that will point what actions this acknowledge should do.

First it checks if the delivery id actually exists in the records.
For it to exists it must have been delivered via ConsumeForever().
If found it will decide what to do based on Action field of the dto.

Actions may be success (remove from queue with positive ack), requeue (return the message to the origin queue) and
deadletter (remove from queue with negative ack and publish to another queue).

Deadletter action will lose the message in case of no Publisher have been staged for the consumer.
*/
func (consumer *RabbitMQConsumer) Acknowledge(
	ctx context.Context,
	id string,
	action constant_broker.AcknowledgeAction,
	header map[string]any,
	body []byte) (err error) {
	if ctx == nil {
		consumer.logger.Warn("acknowledge have received a nil logging context", consumer.logGroup)
	}

	mapObject, found := consumer.DeliveryMap.Load(id)
	if !found {
		return fmt.Errorf("not found message id %s from consumer %s of queue %s", id, consumer.Name, consumer.Queue.Name)
	}
	consumer.DeliveryMap.Delete(id)

	switch message := mapObject.(type) {
	case amqp.Delivery:
		switch action {
		case constant_broker.ACKNOWLEDGE_SUCCESS:
			err = message.Acknowledger.Ack(message.DeliveryTag, false)
			if err != nil {
				return fmt.Errorf("error positive acknowledging message id %s from consumer %s of queue %s: %w", id, consumer.Name, consumer.Queue.Name, err)
			}
		case constant_broker.ACKNOWLEDGE_REQUEUE:
			err = message.Acknowledger.Nack(message.DeliveryTag, false, true)
			if err != nil {
				return fmt.Errorf("error negative acknowledging with requeue message id %s from consumer %s of queue %s: %w", id, consumer.Name, consumer.Queue.Name, err)
			}
		case constant_broker.ACKNOWLEDGE_DEADLETTER:
			err = message.Acknowledger.Nack(message.DeliveryTag, false, false)
			if err != nil {
				return fmt.Errorf("error negative acknowledging with deadletter message id %s from consumer %s of queue %s: %w", id, consumer.Name, consumer.Queue.Name, err)
			}

			if consumer.deadletter != nil {
				err = consumer.deadletter.Publish(
					ctx,
					header,
					body,
				)
				if err != nil {
					return fmt.Errorf("error publishing to deadletter queue of consumer %s of queue %s: %w", consumer.Name, consumer.Queue.Name, err)
				}
			} else {
				consumer.logger.WarnContext(ctx, "DEADLETTER ACKNOWLEDGE WILL BE IGNORED DUE TO MISSING PUBLISHER AT CONSUMER", consumer.logGroup)
			}
		}

	default:
		return fmt.Errorf("message id %s have come with untreatable %T format from consumer %s of queue %s", id, mapObject, consumer.Name, consumer.Queue.Name)
	}

	return nil
}
