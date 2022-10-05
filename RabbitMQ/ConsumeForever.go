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
func (r *RabbitMQ) ConsumeForever(i int) {
	r.Connection.semaphore.Lock()
	incomingDeliveryChannel := r.prepareConsumer()
	r.Connection.semaphore.Unlock()

	connectionCheckChannel := make(chan bool)
	unlockChannel := make(chan bool)
	go r.connectionMonitor(connectionCheckChannel, unlockChannel, i)

	for {
		select {
		case delivery := <-incomingDeliveryChannel:
			if delivery.Body == nil {
				continue
			}

			log.Println("RECIEVED AT " + strconv.Itoa(i))

			messageId := strconv.FormatUint(delivery.DeliveryTag, 10)
			r.ConsumeData.UnacknowledgedDeliveryMap.Store(messageId, delivery)

			consumedMessage := messagebroker.NewMessageBrokerConsumedMessage()
			consumedMessage.MessageId = messageId
			consumedMessage.TransmissionData = delivery.Body

			r.ConsumeData.OutgoingDeliveryChannel <- consumedMessage

		case <-connectionCheckChannel:
			time.Sleep(time.Second)
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

func (r *RabbitMQ) connectionMonitor(connectionCheckChannel chan<- bool, unlockChannel <-chan bool, i int) {
	for {
		connectionId := r.ConnectionId

		// log.Println("CHECKING CONNECTION AT " + strconv.Itoa(i))
		r.Connection.semaphore.Lock()
		// log.Println(connectionId)
		// log.Println(r.Connection.UpdatedConnectionId)

		isConnectionIdOutdated := connectionId != r.Connection.UpdatedConnectionId

		if r.isConnectionDown() || isConnectionIdOutdated {
			log.Println("CHECKING CONNECTION SENDED TO CHANNEL FROM " + strconv.Itoa(i))
			connectionCheckChannel <- false
			<-unlockChannel
			r.Connection.semaphore.Unlock()
			continue

		} else {
			r.Connection.semaphore.Unlock()
			time.Sleep(500 * time.Millisecond)
		}
	}
}
