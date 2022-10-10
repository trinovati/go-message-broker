package rabbitmq

import (
	"log"
	"strconv"
	"time"

	messagebroker "gitlab.com/aplicacao/trinovati-connector-message-brokers"
)

/*
Infinite loop consuming the queue linked to the RabbitMQ.ConsumeData object, preparing the data and sending it towards a channel into the system.

Use only in goroutines, otherwise the system will be forever blocked in the infinite loop trying to push into the channel.

Safe to share amqp.Connection and amqp.Channel for assincronus concurent access.

Case the connection goes down, the service locks and only one instance is permitted to remake amqp.Channel.
*/
func (r *RabbitMQ) ConsumeForever() {
	r.Connection.semaphore.Lock()
	r.Channel.semaphore.Lock()
	incomingDeliveryChannel := r.prepareConsumer()
	r.Channel.semaphore.Unlock()
	r.Connection.semaphore.Unlock()

	updateAmqpChannel := make(chan bool)
	unlockChannel := make(chan bool)
	go r.amqpChannelMonitor(updateAmqpChannel, unlockChannel)

	for {
		select {
		case delivery := <-incomingDeliveryChannel:
			if delivery.Body == nil {
				continue
			}

			messageId := strconv.FormatUint(delivery.DeliveryTag, 10)
			r.ConsumeData.UnacknowledgedDeliveryMap.Store(messageId, delivery)

			consumedMessage := messagebroker.NewMessageBrokerConsumedMessage()
			consumedMessage.MessageId = messageId
			consumedMessage.TransmissionData = delivery.Body

			r.ConsumeData.OutgoingDeliveryChannel <- consumedMessage

		case <-updateAmqpChannel:
			completeError := "***ERROR*** Consume stopped on queue '" + r.ConsumeData.QueueName + "', channel have closed with reason: '"
			if r.Connection.lastConnectionError != nil {
				completeError += r.Connection.lastConnectionError.Reason
			}
			completeError += "'"
			log.Println(completeError)

			incomingDeliveryChannel = r.prepareConsumer()

			unlockChannel <- true
		}
	}
}

/*
Uses the semaphore at Channel and Connection object to check status of a shared channel with assincronus concurent access.

If any problem at channel is found, it locks both the semaphores and sends a signal to remake the amqp.Channel when connection is up.
*/
func (r *RabbitMQ) amqpChannelMonitor(updateAmqpChannel chan<- bool, unlockChannel <-chan bool) {
	for {
		if r.Channel.Channel.IsClosed() {
			r.Channel.semaphore.Lock()
			r.Connection.semaphore.Lock()

			updateAmqpChannel <- true

			<-unlockChannel

			r.Channel.semaphore.Unlock()
			r.Connection.semaphore.Unlock()

		} else {
			time.Sleep(500 * time.Millisecond)
			continue
		}
	}
}
