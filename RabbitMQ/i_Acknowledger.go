// this rabbitmq package is adapting the amqp091-go lib.
package rabbitmq

import (
	"encoding/base64"
	"encoding/json"
	"fmt"

	constant_broker "github.com/trinovati/go-message-broker/v3/pkg/constant"
	dto_broker "github.com/trinovati/go-message-broker/v3/pkg/dto"

	amqp "github.com/rabbitmq/amqp091-go"
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
func (consumer *RabbitMQConsumer) Acknowledge(acknowledge dto_broker.BrokerAcknowledge) (err error) {
	if acknowledge.LoggingCtx == nil {
		consumer.logger.Warn("acknowledge have received a nil logging context", consumer.logGroup)
	}

	mapObject, found := consumer.DeliveryMap.Load(acknowledge.MessageId)
	if !found {
		return fmt.Errorf("not found message id %s from consumer %s of queue %s at channel id %s and connection id %s", acknowledge.MessageId, consumer.Name, consumer.Queue.Name, consumer.channel.Id, consumer.channel.Connection().Id)
	}
	consumer.DeliveryMap.Delete(acknowledge.MessageId)

	switch message := mapObject.(type) {
	case amqp.Delivery:
		switch acknowledge.Action {
		case constant_broker.ACKNOWLEDGE_SUCCESS:
			err = message.Acknowledger.Ack(message.DeliveryTag, false)
			if err != nil {
				return fmt.Errorf("error positive acknowledging message id %s from consumer %s of queue %s at channel id %s and connection id %s: %w", acknowledge.MessageId, consumer.Name, consumer.Queue.Name, consumer.channel.Id, consumer.channel.Connection().Id, err)
			}
		case constant_broker.ACKNOWLEDGE_REQUEUE:
			err = message.Acknowledger.Nack(message.DeliveryTag, false, true)
			if err != nil {
				return fmt.Errorf("error negative acknowledging with requeue message id %s from consumer %s of queue %s at channel id %s and connection id %s: %w", acknowledge.MessageId, consumer.Name, consumer.Queue.Name, consumer.channel.Id, consumer.channel.Connection().Id, err)
			}
		case constant_broker.ACKNOWLEDGE_DEADLETTER:
			err = message.Acknowledger.Nack(message.DeliveryTag, false, false)
			if err != nil {
				return fmt.Errorf("error negative acknowledging with deadletter message id %s from consumer %s of queue %s at channel id %s and connection id %s: %w", acknowledge.MessageId, consumer.Name, consumer.Queue.Name, consumer.channel.Id, consumer.channel.Connection().Id, err)
			}

			if consumer.deadletter != nil {
				// adicionar function aqui para normalizar o report que é publicado
				acknowledge.Report.Body, err = normalizeMessage(acknowledge.Report.Body)
				if err != nil {
					return fmt.Errorf("DEADLETTER IGNORED ERROR IN NORMALIZE MESSAGE: %s", err)
				}
				err = consumer.deadletter.Publish(acknowledge.LoggingCtx, acknowledge.Report)
				if err != nil {
					return fmt.Errorf("error publishing to deadletter queue of consumer %s of queue %s: %w", consumer.Name, consumer.Queue.Name, err)
				}
			} else {
				if acknowledge.LoggingCtx == nil {
					consumer.logger.Warn("DEADLETTER ACKNOWLEDGE WILL BE IGNORED DUE TO MISSING PUBLISHER AT CONSUMER", consumer.logGroup)
				} else {
					consumer.logger.WarnContext(acknowledge.LoggingCtx, "DEADLETTER ACKNOWLEDGE WILL BE IGNORED DUE TO MISSING PUBLISHER AT CONSUMER", consumer.logGroup)
				}
			}
		}

	default:
		return fmt.Errorf("message id %s have come with untreatable %T format from consumer %s of queue %s at channel id %s and connection id %s", acknowledge.MessageId, mapObject, consumer.Name, consumer.Queue.Name, consumer.channel.Id, consumer.channel.Connection().Id)
	}

	return nil
}
func normalizeMessage(input []byte) ([]byte, error) {
	var raw map[string]interface{}

	
	if err := json.Unmarshal(input, &raw); err != nil {
		return input, nil 
	}

	
	standardFields := map[string]any{}

	dataFields := map[string]any{}
	standardKeys := map[string]bool{
		"timestamp": true,
		"service":   true,
		"error":     true,
		"message":   true, 
	}

	var inputMessageBase64 string

	for k, v := range raw {
		switch {
		case k == "message":
			
			msgBytes, err := json.Marshal(v)
			if err != nil {
				return nil, fmt.Errorf("failed to marshal message field: %w", err)
			}
			inputMessageBase64 = base64.StdEncoding.EncodeToString(msgBytes)

		case standardKeys[k]:
			standardFields[k] = v

		default:
			dataFields[k] = v
		}
	}

	
	dataBytes, err := json.Marshal(dataFields)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal data fields: %w", err)
	}
	dataBase64 := base64.StdEncoding.EncodeToString(dataBytes)


	result := map[string]interface{}{
		"timestamp":     standardFields["timestamp"],
		"service":       standardFields["service"],
		"error":         standardFields["error"],
		"input_message": inputMessageBase64,
		"data":          dataBase64,
	}


	output, err := json.Marshal(result)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal final result: %w", err)
	}



	return output, nil
}
