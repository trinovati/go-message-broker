// this rabbitmq package is adapting the amqp091-go lib
package rabbitmq

import (
	"fmt"
	"log"

	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/trinovati/go-message-broker/constants"
	dto_pkg "github.com/trinovati/go-message-broker/dto"
)

/*
This method is used to report to RabbitMQ the status of a received delivery.

Receive a dto as parameter that will point what actions this acknowledge should do.

First it checks if the delivery id actually exists in the records.
For it to exists it must have been delivered via ConsumeForever().
If found it will decide what to do based on Action field of dto.

Actions may be success (remove from queue with positive ack), requeue (return the message to the origin queue) and
deadletter (remove from queue with negative ack and publish to another queue).

Deadletter action will lose the message in case of no Publisher have been staged for the consumer.
*/
func (consumer *RabbitMQConsumer) Acknowledge(acknowledge dto_pkg.BrokerAcknowledge) (err error) {
	mapObject, found := consumer.DeliveryMap.Load(acknowledge.MessageId)
	if !found {
		return errors.New(fmt.Sprintf("failed loading message id %s from map from channel id %s at queue %s", acknowledge.MessageId, consumer.channel.ChannelId.String(), consumer.Queue.Name))
	}
	consumer.DeliveryMap.Delete(acknowledge.MessageId)

	switch message := mapObject.(type) {
	case amqp.Delivery:
		isMessageNotFound := message.Body == nil
		if isMessageNotFound {
			return errors.New(fmt.Sprintf("message id %s not found from channel %s at queue %s", acknowledge.MessageId, consumer.channel.ChannelId.String(), consumer.Queue.Name))
		}

		switch acknowledge.Action {
		case constants.ACKNOWLEDGE_SUCCESS:
			err = message.Acknowledger.Ack(message.DeliveryTag, false)
			if err != nil {
				return errors.Wrap(err, fmt.Sprintf("error positive acknowledging message from channel %s at queue %s", consumer.channel.ChannelId.String(), consumer.Queue.Name))
			}
		case constants.ACKNOWLEDGE_REQUEUE:
			err = message.Acknowledger.Nack(message.DeliveryTag, false, true)
			if err != nil {
				return errors.Wrap(err, fmt.Sprintf("error negative acknowledging message with requeue from channel %s at queue %s", consumer.channel.ChannelId.String(), consumer.Queue.Name))
			}
		case constants.ACKNOWLEDGE_DEADLETTER:
			err = message.Acknowledger.Nack(message.DeliveryTag, false, false)
			if err != nil {
				return errors.Wrap(err, fmt.Sprintf("error negative acknowledging message from channel %s at queue %s", consumer.channel.ChannelId.String(), consumer.Queue.Name))
			}

			if consumer.deadletter != nil {
				err = consumer.deadletter.Publish(acknowledge.Report)
				if err != nil {
					return errors.Wrap(err, fmt.Sprintf("error publishing to deadletter queue from the consumer %s of queue %s", consumer.Name, consumer.Queue.Name))
				}
			} else {
				log.Printf("WARNING    DEADLETTER ACKNOWLEDGE HAVE BEEN IGNORED DUE TO MISSING PUBLISHER AT CONSUMER: %s\n", consumer.Name)
			}
		}

	default:
		return errors.New(fmt.Sprintf("message id %s have come with untreatable %T format from channel %s at queue %s", acknowledge.MessageId, mapObject, consumer.channel.ChannelId.String(), consumer.Queue.Name))
	}

	return nil
}
