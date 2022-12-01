package rabbitmq

import (
	"strconv"
	"time"

	messagebroker "gitlab.com/aplicacao/trinovati-connector-message-brokers"
)

/*
Infinite loop consuming the queue linked to the RabbitMQ.ConsumeData object, preparing the data and sending it towards a channel into the system.

Use only in goroutines, otherwise the system will be forever blocked in the infinite loop trying to push into the channel.

Safe to share amqp.Connection and amqp.Channel for assincronus concurent access.

In case of the connections and/or channel comes down, it prepres for consuming as soon as the channel is up again.
*/
func (r *RabbitMQ) ConsumeForever() {
	consumeChannelSinalizer := make(chan bool)
	incomingDeliveryChannel := r.ConsumeData.prepareLoopingConsumer()

	go r.ConsumeData.amqpChannelMonitor(consumeChannelSinalizer)

	r.PublishData.Channel.CreateChannel(r.Connection)
	r.PreparePublishQueue()

	for {
		select {
		case <-consumeChannelSinalizer:
			r.ConsumeData.Channel.WaitForChannel()
			incomingDeliveryChannel = r.ConsumeData.prepareLoopingConsumer()

			consumeChannelSinalizer <- true

		case delivery := <-incomingDeliveryChannel:
			if delivery.Body == nil {
				continue
			}

			messageId := strconv.FormatUint(delivery.DeliveryTag, 10)
			r.ConsumeData.UnacknowledgedDeliveryMap.Store(messageId, delivery)

			consumedMessage := messagebroker.NewMessageBrokerConsumedMessage()
			consumedMessage.MessageId = messageId
			consumedMessage.MessageData = delivery.Body

			r.ConsumeData.OutgoingDeliveryChannel <- consumedMessage
		}
	}
}

/*
Prepare the consumer in case of the channel comming down.
*/
func (c *RMQConsume) amqpChannelMonitor(consumeChannelSinalizer chan bool) {
	for {
		if !c.Channel.isOpen || c.Channel.isChannelDown() {
			consumeChannelSinalizer <- false
			<-consumeChannelSinalizer

		} else {
			time.Sleep(250 * time.Millisecond)
			continue
		}
	}
}
