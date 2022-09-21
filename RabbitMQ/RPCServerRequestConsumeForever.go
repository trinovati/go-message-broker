package rabbitmq

import (
	"errors"
	"log"
	"strconv"

	messagebroker "gitlab.com/aplicacao/trinovati-connector-message-brokers"
)

/*
Infinite loop consuming the queue linked to the RabbitMQ.RemoteProcedureCallData.RPCServer.Consumer object, preparing the data and sending it towards a channel into the system.

Use only in go routines, otherwise the system will be forever blocked in the infinite loop trying to push into the channel.
*/
func (r *RabbitMQ) RPCServerResquestConsumeForever() {
	errorFileIdentification := "RabbitMQ.go at RPCServerCallbackPublish()"

	queueMessages, closeNotifyChannel := r.RemoteProcedureCallData.RPCServer.Consumer.connectConsumer(r)

	for {
		select {
		case delivery := <-queueMessages:
			messageId := strconv.FormatUint(delivery.DeliveryTag, 10)
			r.RemoteProcedureCallData.RPCServer.Consumer.MessagesMap.Store(messageId, delivery)

			if delivery.CorrelationId == "" {
				err := errors.New("in " + errorFileIdentification + ": no correlation ID comming at message " + string(delivery.Body))
				r.Acknowledge(false, err.Error(), strconv.FormatUint(delivery.DeliveryTag, 10), "")
				continue
			}

			requestDataManager := messagebroker.NewRPCDataDto()
			requestDataManager.Data = delivery.Body
			requestDataManager.CorrelationId = delivery.CorrelationId
			requestDataManager.ResponseRoute = delivery.ReplyTo

			consumedMessage := messagebroker.NewMessageBrokerConsumedMessage()
			consumedMessage.MessageId = messageId
			consumedMessage.TransmissionData = requestDataManager

			r.RemoteProcedureCallData.RPCServer.Consumer.QueueConsumeChannel <- consumedMessage

		case notify := <-closeNotifyChannel:
			log.Println("Consumed queue '" + r.RemoteProcedureCallData.RPCServer.Consumer.QueueName + "' have closed with reason: '" + notify.Reason + "'")

			queueMessages, closeNotifyChannel = r.RemoteProcedureCallData.RPCServer.Consumer.connectConsumer(r)
		}
	}
}
