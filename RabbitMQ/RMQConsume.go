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
	OutgoingDeliveryChannel    chan<- interface{}
	UnacknowledgedDeliveryMap  *sync.Map
	ExchangeName               string
	ExchangeType               string
	QueueName                  string
	AccessKey                  string
	ErrorNotificationQueueName string
	Qos                        int
	PurgeBeforeStarting        bool
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
func (c *RMQConsume) populate(exchangeName string, exchangeType string, QueueName string, AccessKey string, qos int, purgeBeforeStarting bool, outgoingDeliveryChannel chan<- interface{}) {
	c.ExchangeName = exchangeName
	c.ExchangeType = exchangeType
	c.QueueName = QueueName
	c.AccessKey = AccessKey
	c.ErrorNotificationQueueName = "_" + exchangeName + "__failed_messages"
	c.Qos = qos
	c.PurgeBeforeStarting = purgeBeforeStarting
	c.OutgoingDeliveryChannel = outgoingDeliveryChannel
}

/*
Will create a connection, prepare channels, declare queue and exchange case needed.

If an error occurs, it will restart and retry all the process until the consumer is fully prepared.

Return a channel of incoming deliveries.
*/
func (r *RabbitMQ) prepareConsumer() (incomingDeliveryChannel <-chan amqp.Delivery) {
	errorFileIdentification := "RMQConsume.go at prepareChannel()"

	for {
		UpdatedConnectionId := r.Connection.UpdatedConnectionId

		err := r.prepareChannel()
		if err != nil {
			log.Println("***ERROR*** preparing channel in " + errorFileIdentification + ": " + err.Error())
			time.Sleep(time.Second)
			continue
		}

		err = r.ConsumeData.prepareQueue(r)
		if err != nil {
			log.Println("***ERROR*** preparing queue in " + errorFileIdentification + ": " + err.Error())
			time.Sleep(time.Second)
			continue
		}

		if r.isConnectionDown() {
			log.Println("***ERROR*** in " + errorFileIdentification + ": connection dropped before preparing consume, trying again soon")
			time.Sleep(time.Second)
			continue
		}

		incomingDeliveryChannel, err := r.Channel.Channel.Consume(r.ConsumeData.QueueName, "", false, false, false, false, nil)
		if err != nil {
			log.Println("***ERROR*** producing consume channel in " + errorFileIdentification + ": " + err.Error())
			time.Sleep(time.Second)
			continue
		}

		r.ConnectionId = UpdatedConnectionId

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
func (c *RMQConsume) prepareQueue(rabbitmq *RabbitMQ) (err error) {
	errorFileIdentification := "RMQConsume.go at prepareQueue()"

	if rabbitmq.isConnectionDown() {
		completeError := "in " + errorFileIdentification + ": connection dropped before declaring exchange, trying again soon"
		return errors.New(completeError)
	}

	err = rabbitmq.Channel.Channel.ExchangeDeclare(c.ExchangeName, c.ExchangeType, true, false, false, false, nil)
	if err != nil {
		return errors.New("error creating RabbitMQ exchange in " + errorFileIdentification + ": " + err.Error())
	}

	if rabbitmq.isConnectionDown() {
		completeError := "in " + errorFileIdentification + ": connection dropped before declaring queue, trying again soon"
		return errors.New(completeError)
	}

	queue, err := rabbitmq.Channel.Channel.QueueDeclare(c.QueueName, true, false, false, false, nil)
	if err != nil {
		return errors.New("error creating queue in " + errorFileIdentification + ": " + err.Error())
	}

	if queue.Name != c.QueueName {
		return errors.New("in " + errorFileIdentification + ": created queue name '" + queue.Name + "' and expected queue name '" + c.QueueName + "' are diferent")
	}

	if rabbitmq.isConnectionDown() {
		completeError := "in " + errorFileIdentification + ": connection dropped before queue binding, trying again soon"
		return errors.New(completeError)
	}

	err = rabbitmq.Channel.Channel.QueueBind(c.QueueName, c.AccessKey, c.ExchangeName, false, nil)
	if err != nil {
		return errors.New("error binding queue in " + errorFileIdentification + ": " + err.Error())
	}

	err = rabbitmq.Channel.Channel.Qos(c.Qos, 0, false)
	if err != nil {
		return errors.New("error qos a channel, limiting the maximum message queue can hold, in " + errorFileIdentification + ": " + err.Error())
	}

	if c.PurgeBeforeStarting {
		if rabbitmq.isConnectionDown() {
			completeError := "in " + errorFileIdentification + ": connection dropped purging queue, trying again soon"
			return errors.New(completeError)
		}

		_, err = rabbitmq.Channel.Channel.QueuePurge(c.QueueName, true)
		if err != nil {
			return errors.New("error purging a channel queue in " + errorFileIdentification + ": " + err.Error())
		}
	}

	return nil
}
