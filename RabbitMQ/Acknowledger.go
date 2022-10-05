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
func (r *RabbitMQ) Acknowledge(success bool, messageId string, optionalRoute string, comment string) (err error) {
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

			notifyTime := time.Now().Format("2006-01-02 15:04:05Z07:00")
			notifyMessage := `{"error_time":"` + notifyTime + `","error":"` + comment + `","message":"` + string(message.Body) + `"}`

			// if RABBITMQ_SERVICE == RABBITMQ_RPC_SERVER || RABBITMQ_SERVICE == RABBITMQ_RPC_CLIENT {
			// 	err = r.RPCServerCallbackPublish(acknowledger.Comment, message.CorrelationId, message.ReplyTo)
			// 	if err != nil {
			// 		return errors.New("error publishing failed callback in " + errorFileIdentification + ": " + err.Error())
			// 	}
			// }

			rabbitmq := NewRabbitMQ().SharesConnectionWith(r).PopulatePublish(r.ConsumeData.ExchangeName, r.ConsumeData.ExchangeType, r.ConsumeData.ErrorNotificationQueueName, r.ConsumeData.ErrorNotificationQueueName)
			rabbitmq.Publish(notifyMessage, "")

			if err != nil {
				log.Println("error publishing to notify queue in " + errorFileIdentification + ": " + err.Error())
			}

		}

	default:
		return fmt.Errorf("in %s: message id '%s' have come with untreatable '%T' format ", errorFileIdentification, messageId, mapObject)
	}

	return nil
}
