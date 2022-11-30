package rabbitmq

import (
	"errors"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

/*
Acknowledge a message, sinalizing the RabbitMQ server that the message can be eliminated of the queue, or requeued on a error queue.

success is the marker that the system could or couldn't handle the message.

messageId is the control that the broker use to administrate its messages.

optionalRoute is a string flag, path or destiny that the message broker will redirect the message.

comment is a commentary that can be anexed to the object as a sinalizer of errors or success.
*/
func (r *RabbitMQ) Acknowledge(success bool, comment string, messageId string, optionalRoute string) (err error) {
	errorFileIdentification := "RabbitMQ.go at Acknowledge()"

	mapObject, found := r.ConsumeData.UnacknowledgedDeliveryMap.Load(messageId)
	if !found {
		return errors.New("in " + errorFileIdentification + " failed loading message id " + messageId + " from map")
	}
	r.ConsumeData.UnacknowledgedDeliveryMap.Delete(messageId)

	switch message := mapObject.(type) {
	case (amqp.Delivery):
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

			failureTime := time.Now().Format("2006-01-02 15:04:05Z07:00")
			failureMessage := `{"filure_time":"` + failureTime + `","error":"` + comment + `","message":"` + string(message.Body) + `"}`

			err := r.Publish(failureMessage, "", "")
			if err != nil {
				log.Println("error publishing to failure queue in " + errorFileIdentification + ": " + err.Error())
			}

		}

	default:
		return fmt.Errorf("in %s: message id '%s' have come with untreatable '%T' format ", errorFileIdentification, messageId, mapObject)
	}

	return nil
}
