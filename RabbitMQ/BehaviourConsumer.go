package rabbitmq

import (
	"errors"
	"log"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	channel "gitlab.com/aplicacao/trinovati-connector-message-brokers/RabbitMQ/channel"
	"gitlab.com/aplicacao/trinovati-connector-message-brokers/RabbitMQ/config"
	connection "gitlab.com/aplicacao/trinovati-connector-message-brokers/RabbitMQ/connection"
	"gitlab.com/aplicacao/trinovati-connector-message-brokers/RabbitMQ/dto"
	"gitlab.com/aplicacao/trinovati-connector-message-brokers/RabbitMQ/interfaces"
)

/*
Object that holds all information needed for consuming from RabbitMQ queue.
*/
type Consumer struct {
	BehaviourType             string
	Channel                   *channel.ChannelData
	OutgoingDeliveryChannel   chan interface{}
	UnacknowledgedDeliveryMap *sync.Map
	ExchangeName              string
	ExchangeType              string
	QueueName                 string
	AccessKey                 string
	Qos                       int
	PurgeBeforeStarting       bool
	FailedMessagePublisher    *Publisher
}

/*
Builds a new object that holds all information needed for consuming from RabbitMQ queue.

CAUTION:
Keep in mind that FailedMessagePublisher object will be filled with standard failed messages queue data used by Acknowledge() to store failed messages.
You can use FailedMessagePublisher.Populate() afterwards for customization of failed messages destination queue.

CAUTION:
Keep in mind that FailedMessagePublisher will open a different connection with rabbitmq to avoid issues with publishings and deliveries.
*/
func NewConsumer() *Consumer {
	failedMessagePublisher := NewPublisher()

	return &Consumer{
		BehaviourType:             config.RABBITMQ_CONSUMER_BEHAVIOUR,
		Channel:                   channel.NewChannelData(),
		OutgoingDeliveryChannel:   nil,
		UnacknowledgedDeliveryMap: nil,
		FailedMessagePublisher:    failedMessagePublisher,
	}
}

/*
Change the address the service will try to connect.

CAUTION:
Keep in mind that FailedMessagePublisher object will be filled with the same connection string, if you need to point filed
messages to any other server, you should change its address directly.

CAUTION:
Keep in mind that FailedMessagePublisher will open a different connection with rabbitmq to avoid issues with publishings and deliveries.
*/
func (consumer *Consumer) WithConnectionString(connectionString string) interfaces.Behaviour {
	consumer.Channel.Connection.SetConnectionString(connectionString)

	consumer.FailedMessagePublisher.Channel.Connection.SetConnectionString(connectionString)

	return consumer
}

func (consumer *Consumer) GetConnection() *connection.ConnectionData {
	return consumer.Channel.Connection
}

/*
Will make both objects share the same connection information for assincronus access.

CAUTION:
Keep in mind that FailedMessagePublisher will not change its connection, and should always have a isolated connection.
*/
func (consumer *Consumer) SharesConnectionWith(behaviour interfaces.Behaviour) interfaces.Behaviour {
	consumer.Channel.Connection = behaviour.GetConnection()

	return consumer
}

func (consumer *Consumer) CloseConnection() {
	consumer.Channel.Connection.CloseConnection()
}

func (consumer *Consumer) CreateChannel() interfaces.Behaviour {
	consumer.Channel.CreateChannel()

	consumer.FailedMessagePublisher.CreateChannel()

	return consumer
}

/*
Will make both objects share the same channel and connection information for assincronus access.

CAUTION:
Keep in mind that FailedMessagePublisher will not change its channel, and should always have a isolated channel.
*/
func (consumer *Consumer) SharesChannelWith(behaviour interfaces.Behaviour) interfaces.Behaviour {
	consumer.Channel = behaviour.GetChannel()

	consumer.FailedMessagePublisher.CreateChannel()

	return consumer
}

func (consumer *Consumer) GetChannel() *channel.ChannelData {
	return consumer.Channel
}

func (consumer *Consumer) CloseChannel() {
	consumer.Channel.CloseChannel()
}

/*
Populate the object for a consume behaviour.

The consumed messages will be sended to the channel passed to queueConsumeChannel.
*/
func (consumer *Consumer) Populate(behaviourDto *dto.BehaviourDto) interfaces.Behaviour {
	consumer.ExchangeName = behaviourDto.ExchangeName
	consumer.ExchangeType = behaviourDto.ExchangeType
	consumer.QueueName = behaviourDto.QueueName
	consumer.AccessKey = behaviourDto.AccessKey
	consumer.Qos = behaviourDto.Qos
	consumer.PurgeBeforeStarting = behaviourDto.PurgeBeforeStarting
	consumer.OutgoingDeliveryChannel = behaviourDto.OutgoingDeliveryChannel
	consumer.UnacknowledgedDeliveryMap = behaviourDto.UnacknowledgedDeliveryMap

	failedMessagePublisherExchangeName := "failed"
	failedMessagePublisherExchangeType := "direct"
	failedMessagePublisherQueueName := "_" + consumer.ExchangeName + "__failed_messages"
	failedMessagePublisherAccessKey := failedMessagePublisherQueueName

	publisherBehaviourDto := dto.NewBehaviourDto().FillPublisherData(failedMessagePublisherExchangeName, failedMessagePublisherExchangeType, failedMessagePublisherQueueName, failedMessagePublisherAccessKey)
	consumer.FailedMessagePublisher.Populate(publisherBehaviourDto)

	return consumer
}

/*
Will copy data not linked to connection or channels to the object. Besides the effect of PopulateConsume(), both objects will share the UnacknowledgedDeliveryMap.

CAUTION:
Keep in mind that if a behaviour other than consumer is passed, there will be no changes.
*/
func (consumer *Consumer) GetPopulatedDataFrom(behaviour interfaces.Behaviour) interfaces.Behaviour {
	if consumer.BehaviourType != behaviour.GetBehaviourType() {
		return consumer
	}

	consumer.Populate(behaviour.BehaviourToBehaviourDto())

	return consumer
}

func (consumer *Consumer) BehaviourToBehaviourDto() *dto.BehaviourDto {
	behaviourDto := dto.NewBehaviourDto()
	behaviourDto.FillConsumerData(consumer.ExchangeName, consumer.ExchangeType, consumer.QueueName, consumer.AccessKey, consumer.Qos, consumer.PurgeBeforeStarting, consumer.OutgoingDeliveryChannel, consumer.UnacknowledgedDeliveryMap)

	return behaviourDto
}

func (consumer *Consumer) GetBehaviourType() string {
	return consumer.BehaviourType
}

/*
Will create a connection, prepare channels, declare queue and exchange case needed.

If an error occurs, it will restart and retry all the process until the consumer is fully prepared.

Return a channel of incoming deliveries.
*/
func (c *Consumer) prepareLoopingConsumer() (incomingDeliveryChannel <-chan amqp.Delivery) {
	errorFileIdentification := "Consumer.go at prepareLoopingConsumer()"

	for {
		c.Channel.WaitForChannel()

		err := c.prepareConsumeQueue()
		if err != nil {
			log.Println("***ERROR*** preparing queue in " + errorFileIdentification + ": " + err.Error())
			time.Sleep(time.Second)
			continue
		}

		if c.Channel.IsChannelDown() {
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
func (c *Consumer) prepareConsumeQueue() (err error) {
	errorFileIdentification := "Consumer.go at prepareConsumeQueue()"

	if c.Channel.IsChannelDown() {
		completeError := "in " + errorFileIdentification + ": channel dropped before declaring exchange"
		return errors.New(completeError)
	}

	err = c.Channel.Channel.ExchangeDeclare(c.ExchangeName, c.ExchangeType, true, false, false, false, nil)
	if err != nil {
		return errors.New("error creating RabbitMQ exchange in " + errorFileIdentification + ": " + err.Error())
	}

	if c.Channel.IsChannelDown() {
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

	if c.Channel.IsChannelDown() {
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
		if c.Channel.IsChannelDown() {
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
