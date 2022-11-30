package rabbitmq

import (
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	messagebroker "gitlab.com/aplicacao/trinovati-connector-message-brokers"
)

/*
Infinite loop consuming the queue linked to the RabbitMQ.ConsumeData object, preparing the data and sending it towards a channel into the system.

Use only in goroutines, otherwise the system will be forever blocked in the infinite loop trying to push into the channel.

Safe to share amqp.Connection and amqp.Channel for assincronus concurent access.

In case of the connections and/or channel comes down, it prepres for consuming as soon as the channel is up again.
*/
func (r *RabbitMQ) ConsumeForever() {
	incomingDeliveryChannel := new(<-chan amqp.Delivery)
	*incomingDeliveryChannel = r.ConsumeData.prepareConsumer()

	go r.ConsumeData.amqpChannelMonitor(incomingDeliveryChannel)

	r.PublishData.Channel.CreateChannel(r.Connection)

	for delivery := range *incomingDeliveryChannel {
		if delivery.Body == nil {
			continue
		}

		messageId := strconv.FormatUint(delivery.DeliveryTag, 10)
		r.ConsumeData.UnacknowledgedDeliveryMap.Store(messageId, delivery)

		consumedMessage := messagebroker.NewMessageBrokerConsumedMessage()
		consumedMessage.MessageId = messageId
		consumedMessage.TransmissionData = delivery.Body

		r.ConsumeData.OutgoingDeliveryChannel <- consumedMessage
	}
}

/*
Prepare the consumer in case of the channel comming down.
*/
func (c *RMQConsume) amqpChannelMonitor(incomingDeliveryChannel *(<-chan amqp.Delivery)) {
	for {
		if c.Channel.isChannelDown() {
			c.Channel.WaitForChannel()

			*incomingDeliveryChannel = c.prepareConsumer()

		} else {
			time.Sleep(500 * time.Millisecond)
			continue
		}
	}
}
