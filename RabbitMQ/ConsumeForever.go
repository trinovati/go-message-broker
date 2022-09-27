package rabbitmq

import (
	"log"
	"strconv"
	"time"

	messagebroker "gitlab.com/aplicacao/trinovati-connector-message-brokers"
)

/*
Infinite loop consuming the queue linked to the RabbitMQ.ConsumeData object, preparing the data and sending it towards a channel into the system.

Use only in go routines, otherwise the system will be forever blocked in the infinite loop trying to push into the channel.
*/
func (r *RabbitMQ) ConsumeForever() {
	queueMessages, closeNotifyChannel := r.ConsumeData.prepareConsumer(r)

	for {
		select {
		case delivery := <-queueMessages:
			messageId := strconv.FormatUint(delivery.DeliveryTag, 10)
			r.ConsumeData.MessagesMap.Store(messageId, delivery)

			consumedMessage := messagebroker.NewMessageBrokerConsumedMessage()
			consumedMessage.MessageId = messageId
			consumedMessage.TransmissionData = delivery.Body

			r.ConsumeData.QueueConsumeChannel <- consumedMessage

		case notify := <-closeNotifyChannel:
			log.Println("***ERROR*** Consumed stopped on queue '" + r.ConsumeData.QueueName + "', channel have closed with reason: '" + notify.Reason + "'")
			time.Sleep(2 * time.Second)

			queueMessages, closeNotifyChannel = r.ConsumeData.prepareConsumer(r)
		}
	}
}
