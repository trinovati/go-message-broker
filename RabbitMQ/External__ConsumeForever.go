package rabbitmq

import (
	"log"
	"strconv"
	"time"

	messagebroker "gitlab.com/aplicacao/trinovati-connector-message-brokers"
	"gitlab.com/aplicacao/trinovati-connector-message-brokers/RabbitMQ/config"
)

/*
Infinite loop consuming the queue linked to the RabbitMQ.ConsumeData object, preparing the data and sending it towards a channel into the system.

Use only in goroutines, otherwise the system will be forever blocked in the infinite loop trying to push into the channel.

Safe to share amqp.Connection and amqp.Channel for assincronus concurent access.

In case of the connections and/or channel comes down, it prepres for consuming as soon as the channel is up again.

CAUTION:
Keep in mind that if a behaviour other than consumer is passed, ConsumeForever() will panic the service.
*/
func (r *RabbitMQ) ConsumeForever() {
	errorFileIdentification := "RabbitMQ at ConsumeForever"

	var position int
	var behaviourType string

	for position, behaviourType = range r.BehaviourTypeMap {
		if behaviourType == config.RABBITMQ_CONSUMER_BEHAVIOUR {
			break
		} else if position == len(r.Behaviour)-1 {
			log.Panic("***ERROR*** in " + errorFileIdentification + ": no prepared consumer behaviour")
		}
	}

	consumer := r.Behaviour[position].(*Consumer)

	consumeChannelSinalizer := make(chan bool)
	incomingDeliveryChannel := consumer.prepareLoopingConsumer()

	go consumer.amqpChannelMonitor(consumeChannelSinalizer)

	consumer.FailedMessagePublisher.Channel.CreateChannel()
	consumer.FailedMessagePublisher.PreparePublishQueue()

	for {
		select {
		case <-consumeChannelSinalizer:
			consumer.Channel.WaitForChannel()
			incomingDeliveryChannel = consumer.prepareLoopingConsumer()

			consumeChannelSinalizer <- true

		case delivery := <-incomingDeliveryChannel:
			if delivery.Body == nil {
				continue
			}

			messageId := strconv.FormatUint(delivery.DeliveryTag, 10)
			consumer.UnacknowledgedDeliveryMap.Store(messageId, delivery)

			consumedMessage := messagebroker.NewMessageBrokerConsumedMessage()
			consumedMessage.MessageId = messageId + "@" + strconv.Itoa(position)
			consumedMessage.MessageData = delivery.Body

			consumer.OutgoingDeliveryChannel <- consumedMessage
		}
	}
}

/*
Prepare the consumer in case of the channel comming down.
*/
func (consumer *Consumer) amqpChannelMonitor(consumeChannelSinalizer chan bool) {
	for {
		if consumer.Channel.IsChannelDown() || consumer.Channel.Connection.IsConnectionDown() {
			consumeChannelSinalizer <- false
			<-consumeChannelSinalizer

		} else {
			time.Sleep(250 * time.Millisecond)
			continue
		}
	}
}
