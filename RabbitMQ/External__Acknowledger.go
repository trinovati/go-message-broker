package rabbitmq

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

/*
Acknowledge a message, sinalizing the RabbitMQ server that the message can be eliminated of the queue, or requeued on a error queue.

success is the marker that the system could or couldn't handle the message.

messageId is the control that the broker use to administrate its messages.

optionalRoute is a string flag, path or destiny that the message broker will redirect the message.

motive is a commentary that can be anexed to the object as a sinalizer of errors or success.
*/
func (r *RabbitMQ) Acknowledge(success bool, motive string, messageId string, optionalRoute string) (err error) {
	errorFileIdentification := "RabbitMQ.go at Acknowledge()"

	splittedMessageId := strings.Split(messageId, "@")
	id := splittedMessageId[0]
	behaviourPosition, err := strconv.Atoi(splittedMessageId[1])
	if err != nil {
		log.Panic("in " + errorFileIdentification + " splitting message id '" + messageId + "'")
	}

	consumer := r.Behaviour[behaviourPosition].(*Consumer)

	mapObject, found := consumer.UnacknowledgedDeliveryMap.Load(id)
	if !found {
		return errors.New("in " + errorFileIdentification + " failed loading message id '" + id + "' from map")
	}
	consumer.UnacknowledgedDeliveryMap.Delete(messageId)

	switch message := mapObject.(type) {
	case amqp.Delivery:
		isMessageNotFound := message.Body == nil
		if isMessageNotFound {
			return errors.New("in " + errorFileIdentification + ": message id '" + messageId + "' not found")
		}

		switch success {
		case true:
			err = message.Acknowledger.Ack(message.DeliveryTag, false)
			if err != nil {
				return errors.New("error positive acknowlodging message in " + errorFileIdentification + ": " + err.Error())
			}

		case false:
			err = message.Acknowledger.Nack(message.DeliveryTag, false, false)
			if err != nil {
				return errors.New("error negative acknowlodging message in " + errorFileIdentification + ": " + err.Error())
			}

			var failureMap map[string]interface{} = map[string]interface{}{
				"message":          base64.StdEncoding.EncodeToString(message.Body),
				"acknowledge_time": time.Now().Format("2006-01-02T15:04:05Z07:00"),
				"motive":           motive,
			}

			failureMessage, err := json.Marshal(failureMap)
			if err != nil {
				return errors.New("error marshalling failure message in " + errorFileIdentification + ": " + err.Error())
			}

			var failedNotifyExchangeName string = ""
			var failedNotifyQueueName string = ""
			if optionalRoute != "" {
				splittedOptionalRoute := strings.Split(optionalRoute, "@")
				if len(splittedOptionalRoute) == 2 {
					failedNotifyExchangeName = splittedOptionalRoute[0]
					failedNotifyQueueName = splittedOptionalRoute[1]

				} else {
					log.Println("there was a optional route for failed message destiny, but it have come with unexpected format: '" + optionalRoute + "', publishing in standard queue")
				}
			}

			err = consumer.FailedMessagePublisher.Publish(string(failureMessage), failedNotifyExchangeName, failedNotifyQueueName)
			if err != nil {
				log.Println("error publishing to failure queue in " + errorFileIdentification + ": " + err.Error())
			}
		}

	default:
		return fmt.Errorf("in %s: message id '%s' have come with untreatable '%T' format ", errorFileIdentification, messageId, mapObject)
	}

	return nil
}
