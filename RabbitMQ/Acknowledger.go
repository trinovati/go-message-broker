package rabbitmq

import (
	"errors"
	"log"
	"sync"
	"time"

	"github.com/streadway/amqp"
	messagebroker "gitlab.com/aplicacao/trinovati-connector-message-brokers"
)

/*
Acknowledge a message, sinalizing the RabbitMQ server that the message can be eliminated of the queue, or requeued on a error queue.

success is the marker that the system could or couldn't handle the message.

messageId is the control that the broker use to administrate its messages.

optionalRoute is a string flag, path or destiny that the message broker will redirect the message.

comment is a commentary that can be anexed to the object as a sinalizer of errors or success.
*/
func (r *RabbitMQ) Acknowledge(success bool, comment string, messageId string, optionalRoute string) (err error) {
	acknowledger := messagebroker.NewMessageBrokerAcknowledger(success, comment, messageId, optionalRoute)

	errorFileIdentification := "RabbitMQ.go at Acknowledge()"
	var messagesMap *sync.Map
	var notifyQueueName string

	switch r.service {
	case RABBITMQ_RPC_SERVER:
		messagesMap = r.RemoteProcedureCallData.RPCServer.Consumer.MessagesMap
		notifyQueueName = r.RemoteProcedureCallData.RPCServer.Consumer.NotifyQueueName

	case RABBITMQ_RPC_CLIENT:
		messagesMap = r.RemoteProcedureCallData.RPCClient.Callback.MessagesMap
		notifyQueueName = r.RemoteProcedureCallData.RPCClient.Callback.NotifyQueueName

	case RABBITMQ_CLIENT:
		messagesMap = r.ConsumeData.MessagesMap
		notifyQueueName = r.ConsumeData.NotifyQueueName

	default:
		log.Panic("in " + errorFileIdentification + ": not implemented rabbitmq service '" + r.service + "' have been added to RabbitMQ struct")
	}

	mapObject, found := messagesMap.Load(acknowledger.MessageId)
	if !found {
		return errors.New("in " + errorFileIdentification + " failed loading message id " + acknowledger.MessageId + " from map")
	}
	messagesMap.Delete(acknowledger.MessageId)

	message := mapObject.(amqp.Delivery)
	isMessageNotFound := message.Body == nil
	if isMessageNotFound {
		return errors.New("in " + errorFileIdentification + " message id " + acknowledger.MessageId + " not found")
	}

	switch acknowledger.Success {
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

		notifyTime := time.Now().In(time.Local).Format("2006-01-02 15:04:05Z07:00")
		notifyMessage := `{"error_time":"` + notifyTime + `","error":"` + comment + `","message":"` + string(message.Body) + `"}`

		go r.publishNotify(notifyMessage, notifyQueueName)

		if RABBITMQ_SERVICE == RABBITMQ_RPC_SERVER || RABBITMQ_SERVICE == RABBITMQ_RPC_CLIENT {
			err = r.RPCServerCallbackPublish(acknowledger.Comment, message.CorrelationId, message.ReplyTo)
			if err != nil {
				return errors.New("error publishing failed callback in " + errorFileIdentification + ": " + err.Error())
			}
		}
	}

	return nil
}

/*
Publish a message on the error notify queue.
*/
func (r *RabbitMQ) publishNotify(message string, notifyQueueName string) {
	errorFileIdentification := "RabbitMQ.go at publishNotify()"

	newRabbitMQ := r.MakeCopy()
	newRabbitMQ.ConsumeData = nil

	notifyExchangeName := "notify"
	notifyExchangeType := "direct"
	notifyAccessKey := notifyQueueName

	newRabbitMQ.PopulatePublish(notifyExchangeName, notifyExchangeType, notifyQueueName, notifyAccessKey)

	err := newRabbitMQ.Publish(message, "")
	if err != nil {
		log.Println("error publishing to notify queue in " + errorFileIdentification + ": " + err.Error())
	}
}
