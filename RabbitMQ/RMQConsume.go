package rabbitmq

import (
	"errors"
	"log"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

/*
Object that holds all information needed for consuming from RabbitMQ queue.
*/
type RMQConsume struct {
	Channel                   *ChannelData
	OutgoingDeliveryChannel   chan interface{}
	UnacknowledgedDeliveryMap *sync.Map
	ExchangeName              string
	ExchangeType              string
	QueueName                 string
	AccessKey                 string
	Qos                       int
	PurgeBeforeStarting       bool
}

/*
Builds a new object that holds all information needed for consuming from RabbitMQ queue.
*/
func newRMQConsume() *RMQConsume {
	return &RMQConsume{
		OutgoingDeliveryChannel:   nil,
		UnacknowledgedDeliveryMap: &sync.Map{},
	}
}

/*
Insert data into the object used for RabbitMQ queue consume.
*/
func (c *RMQConsume) populate(exchangeName string, exchangeType string, QueueName string, AccessKey string, qos int, purgeBeforeStarting bool, outgoingDeliveryChannel chan interface{}) {
	c.ExchangeName = exchangeName
	c.ExchangeType = exchangeType
	c.QueueName = QueueName
	c.AccessKey = AccessKey
	c.Qos = qos
	c.PurgeBeforeStarting = purgeBeforeStarting
	c.OutgoingDeliveryChannel = outgoingDeliveryChannel
}

/*
Will create a connection, prepare channels, declare queue and exchange case needed.

If an error occurs, it will restart and retry all the process until the consumer is fully prepared.

Return a channel of incoming deliveries.
*/
func (c *RMQConsume) prepareLoopingConsumer() (incomingDeliveryChannel <-chan amqp.Delivery) {
	errorFileIdentification := "RMQConsume.go at prepareLoopingConsumer()"

	for {
		c.Channel.WaitForChannel()

		err := c.prepareConsumeQueue()
		if err != nil {
			log.Println("***ERROR*** preparing queue in " + errorFileIdentification + ": " + err.Error())
			time.Sleep(time.Second)
			continue
		}

		if c.Channel.isChannelDown() {
			log.Println("***ERROR*** in " + errorFileIdentification + ": connection dropped before preparing consume, trying again soon")
			time.Sleep(time.Second)
			continue
		}

		incomingDeliveryChannel, err := c.Channel.Channel.Consume(c.QueueName, "", false, false, false, false, nil)
		if err != nil {
			log.Println("***ERROR*** producing consume channel in " + errorFileIdentification + ": " + err.Error())
			time.Sleep(time.Second)
			continue
		}

		return incomingDeliveryChannel
	}
}

/*
Prepare a queue linked to RabbitMQ channel for consuming.

In case of unexistent exchange, it will create the exchange.

In case of unexistent queue, it will create the queue.

In case of queue not beeing binded to any exchange, it will bind it to a exchange.

It will set Qos to the channel.

It will purge the queue before consuming case ordered to.
*/
func (c *RMQConsume) prepareConsumeQueue() (err error) {
	errorFileIdentification := "RMQConsume.go at prepareConsumeQueue()"

	if c.Channel.isChannelDown() {
		completeError := "in " + errorFileIdentification + ": channel dropped before declaring exchange"
		return errors.New(completeError)
	}

	err = c.Channel.Channel.ExchangeDeclare(c.ExchangeName, c.ExchangeType, true, false, false, false, nil)
	if err != nil {
		return errors.New("error creating RabbitMQ exchange in " + errorFileIdentification + ": " + err.Error())
	}

	if c.Channel.isChannelDown() {
		completeError := "in " + errorFileIdentification + ": channel dropped before declaring queue"
		return errors.New(completeError)
	}

	queue, err := c.Channel.Channel.QueueDeclare(c.QueueName, true, false, false, false, nil)
	if err != nil {
		return errors.New("error creating queue in " + errorFileIdentification + ": " + err.Error())
	}

	if queue.Name != c.QueueName {
		return errors.New("in " + errorFileIdentification + ": created queue name '" + queue.Name + "' and expected queue name '" + c.QueueName + "' are diferent")
	}

	if c.Channel.isChannelDown() {
		completeError := "in " + errorFileIdentification + ": channel dropped before queue binding, trying again soon"
		return errors.New(completeError)
	}

	err = c.Channel.Channel.QueueBind(c.QueueName, c.AccessKey, c.ExchangeName, false, nil)
	if err != nil {
		return errors.New("error binding queue in " + errorFileIdentification + ": " + err.Error())
	}

	err = c.Channel.Channel.Qos(c.Qos, 0, false)
	if err != nil {
		return errors.New("error qos a channel, limiting the maximum message queue can hold, in " + errorFileIdentification + ": " + err.Error())
	}

	if c.PurgeBeforeStarting {
		if c.Channel.isChannelDown() {
			completeError := "in " + errorFileIdentification + ": channel dropped purging queue"
			return errors.New(completeError)
		}

		_, err = c.Channel.Channel.QueuePurge(c.QueueName, true)
		if err != nil {
			return errors.New("error purging a channel queue in " + errorFileIdentification + ": " + err.Error())
		}
	}

	return nil
}
