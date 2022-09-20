package rabbitmq

import (
	"errors"
	"log"
	"sync"
	"time"
	"trinovati-message-handler/services/connector/messagebroker"

	"github.com/streadway/amqp"
)

/*
Object that holds information needed for consuming from RabbitMQ.
*/
type RMQConsume struct {
	Channel             *amqp.Channel
	ExchangeName        string
	ExchangeType        string
	QueueName           string
	AccessKey           string
	NotifyQueueName     string
	Qos                 int
	PurgeBeforeStarting bool
	QueueConsumeChannel chan<- messagebroker.ConsumedMessage
	MessagesMap         *sync.Map
}

/*
Create a new object that can hold all information needed to consume from a RabbitMQ queue.
*/
func newRMQConsume(queueConsumeChannel chan<- messagebroker.ConsumedMessage) *RMQConsume {
	return &RMQConsume{
		QueueConsumeChannel: queueConsumeChannel,
	}
}

/*
Insert data into the object used for RabbitMQ queue consume.
*/
func (c *RMQConsume) populate(exchangeName string, exchangeType string, QueueName string, AccessKey string, qos int, purgeBeforeStarting bool) {
	c.ExchangeName = exchangeName
	c.ExchangeType = exchangeType
	c.QueueName = QueueName
	c.AccessKey = AccessKey
	c.NotifyQueueName = "_" + exchangeName + "__failed_messages"
	c.Qos = qos
	c.PurgeBeforeStarting = purgeBeforeStarting

	c.MessagesMap = &sync.Map{}
}

/*
Will create a connection, prepare channels, declare queue and exchange case needed.

Return channels for consuming messages and for checking the connection.
*/
func (c *RMQConsume) connectConsumer(rabbitmq *RabbitMQ) (queueMessages <-chan amqp.Delivery, closeNotifyChannel chan *amqp.Error) {
	errorFileIdentification := "RMQConsume.go at prepareChannel()"

	for {
		rabbitmq.Connect()

		err := c.prepareChannel(rabbitmq)
		if err != nil {
			log.Println("***ERROR*** preparing channel in " + errorFileIdentification + ": " + err.Error())
			time.Sleep(time.Second)
			continue
		}

		err = c.prepareQueue(rabbitmq)
		if err != nil {
			log.Println("***ERROR*** preparing queue in " + errorFileIdentification + ": " + err.Error())
			time.Sleep(time.Second)
			continue
		}

		messageChannel, err := c.Channel.Consume(c.QueueName, "", false, false, false, false, nil)
		if err != nil {
			log.Println("***ERROR*** producing consume channel in " + errorFileIdentification + ": " + err.Error())
			time.Sleep(time.Second)
			continue
		}

		closeNotifyChannel = make(chan *amqp.Error)
		rabbitmq.Connection.NotifyClose(closeNotifyChannel)

		return messageChannel, closeNotifyChannel
	}
}

/*
Prepare a channel linked to RabbitMQ connection for publishing.

In case of unexistent exchange, it will create the exchange.
*/
func (c *RMQConsume) prepareChannel(rabbitMQ *RabbitMQ) (err error) {
	errorFileIdentification := "RMQConsume.go at prepareChannel()"

	c.Channel, err = rabbitMQ.Connection.Channel()
	if err != nil {
		return errors.New("error creating a channel linked to RabbitMQ in " + errorFileIdentification + ": " + err.Error())
	}

	err = c.Channel.ExchangeDeclare(c.ExchangeName, c.ExchangeType, true, false, false, false, nil)
	if err != nil {
		return errors.New("error creating RabbitMQ exchange in " + errorFileIdentification + ": " + err.Error())
	}

	return nil
}

/*
Prepare a queue linked to RabbitMQ channel for consuming.

In case of unexistent queue, it will create the queue.

In case of queue not beeing binded to any exchange, it will bind it to a exchange.

It will set Qos to the channel.

It will purge the queue before consuming case ordered to.
*/
func (c *RMQConsume) prepareQueue(rabbitMQ *RabbitMQ) (err error) {
	errorFileIdentification := "RMQConsume.go at prepareQueue()"

	queue, err := c.Channel.QueueDeclare(c.QueueName, true, false, false, false, nil)
	if err != nil {
		return errors.New("error creating queue in " + errorFileIdentification + ": " + err.Error())
	}

	if queue.Name != c.QueueName {
		return errors.New("created queue name and expected queue name are diferent in " + errorFileIdentification + "")
	}

	err = c.Channel.QueueBind(c.QueueName, c.AccessKey, c.ExchangeName, false, nil)
	if err != nil {
		return errors.New("error binding queue in " + errorFileIdentification + ": " + err.Error())
	}

	err = c.Channel.Qos(c.Qos, 0, false)
	if err != nil {
		return errors.New("error qos a channel, limiting the maximum message queue can hold, in " + errorFileIdentification + ": " + err.Error())
	}

	if c.PurgeBeforeStarting {
		_, err = c.Channel.QueuePurge(c.QueueName, true)
		if err != nil {
			return errors.New("error purging a channel queue in " + errorFileIdentification + ": " + err.Error())
		}
	}

	return nil
}
