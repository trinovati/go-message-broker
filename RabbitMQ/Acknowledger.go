package rabbitmq

import (
	"errors"
	"gitlab.com/aplicacao/trinovati-connector-message-brokers"
	"log"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

/*
Acknowledge a message, sinalizing the RabbitMQ server that the message can be eliminated of the queue, or requeued on a error queue.
*/
func (r *RabbitMQ) Acknowledge(acknowledger messagebroker.ConsumedMessageAcknowledger) (err error) {
	errorFileIdentification := "RabbitMQ.go at Acknowledge()"
	var messagesMap *sync.Map
	var notifyQueueName string

	switch r.Service {
	case RABBITMQ_RPC_SERVER:
		messagesMap = r.RemoteProcedureCallData.RPCServer.Consumer.MessagesMap
		notifyQueueName = r.RemoteProcedureCallData.RPCServer.Consumer.NotifyQueueName

	case RABBITMQ_RPC_CLIENT:
		messagesMap = r.RemoteProcedureCallData.RPCClient.Callback.MessagesMap
		notifyQueueName = r.RemoteProcedureCallData.RPCClient.Callback.NotifyQueueName

	default:
		messagesMap = r.ConsumeData.MessagesMap
		notifyQueueName = r.ConsumeData.NotifyQueueName
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

		notifyTime := time.Now().Format("2006-01-02 15:04:05")

		go r.publishNotify(notifyTime+":"+string(message.Body), notifyQueueName)

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

	err := newRabbitMQ.Publish(message)
	if err != nil {
		log.Println("error publishing to notify queue in " + errorFileIdentification + ": " + err.Error())
	}
}
