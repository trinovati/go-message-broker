package rabbitmq

import (
	"errors"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

/*
Object containing methods to prepare a consumer or publisher to RabbitMQ service, operating as client, RPC client or RPC server.
*/
type RabbitMQ struct {
	Connection  *ConnectionData
	service     string
	ConsumeData *RMQConsume
	PublishData *RMQPublish
}

/*
Build an object containing methods to prepare a consumer or publisher to RabbitMQ service.

terminateOnConnectionError defines if, at any moment, the connections fail or comes down, the service will panic or retry connection.

By default, the object will try to access the environmental variable RABBITMQ_SERVER for connection purpose, in case of unexistent, it will use 'amqp://guest:guest@localhost:5672/' address.

By default, the object will try to access the environmental variable RABBITMQ_SERVICE for behaviour purpose, in case of unexistent, it will use 'client' behaviour.
*/
func NewRabbitMQ() *RabbitMQ {
	return &RabbitMQ{
		Connection:  newConnectionData(),
		service:     RABBITMQ_CLIENT,
		ConsumeData: nil,
		PublishData: nil,
	}
}

/*
Change the address the service will try to connect.
*/
func (r *RabbitMQ) WithServerAddress(serverAddress string) *RabbitMQ {
	r.Connection.serverAddress = serverAddress

	return r
}

/*
Configure the object to kill the program at any problem with RabbitMQ server.
*/
func (r *RabbitMQ) WithTerminateOnConnectionError(terminate bool) *RabbitMQ {
	r.Connection.terminateOnConnectionError = terminate

	return r
}

/*
Will make both objects share the same connection information and server interaction semaphore for assincronus access.
*/
func (r *RabbitMQ) SharesConnectionWith(rabbitmq *RabbitMQ) *RabbitMQ {
	errorFileIdentification := "RabbitMQ.go at SharesConnectionWith()"

	connectionExists := rabbitmq.Connection != nil && rabbitmq.Connection.Connection != nil

	if connectionExists {
		r.Connection = rabbitmq.Connection

	} else {
		log.Println("in " + errorFileIdentification + ": WARNING!!! shared connection is nil pointer")
	}

	return r
}

/*
Will make both objects share the same channel, connection information and server interaction.
*/
func (r *RabbitMQ) SharesConsumeChannelWith(rabbitmq *RabbitMQ) *RabbitMQ {
	errorFileIdentification := "RabbitMQ.go at SharesConsumeChannelWith()"

	consumerExists := rabbitmq.ConsumeData != nil
	if consumerExists {
		channelExists := rabbitmq.ConsumeData.Channel != nil

		if channelExists {
			r.SharesConnectionWith(rabbitmq)

			r.ConsumeData.Channel = rabbitmq.ConsumeData.Channel

		} else {
			log.Println("in " + errorFileIdentification + ": WARNING!!! shared channel is nil pointer")
		}
	} else {
		log.Println("in " + errorFileIdentification + ": WARNING!!! rabbitmq object has no consumer")
	}

	return r
}

/*
Will make both objects share the same channel, connection information and server interaction.
*/
func (r *RabbitMQ) SharesPublishChannelWith(rabbitmq *RabbitMQ) *RabbitMQ {
	errorFileIdentification := "RabbitMQ.go at SharesPublishChannelWith()"

	consumerExists := rabbitmq.PublishData != nil
	if consumerExists {
		channelExists := rabbitmq.PublishData.Channel != nil

		if channelExists {
			r.SharesConnectionWith(rabbitmq)

			r.PublishData.Channel = rabbitmq.PublishData.Channel

		} else {
			log.Println("in " + errorFileIdentification + ": WARNING!!! shared channel is nil pointer")
		}
	} else {
		log.Println("in " + errorFileIdentification + ": WARNING!!! rabbitmq object has no publisher")
	}

	return r
}

/*
Will copy data not linked to connection or channels to the object.

In case of PublishData, this have the same effect as PopulatePublish().

In case of ConsumeData, besides the effect of PopulateConsume(), both objects will share the UnacknowledgedDeliveryMap.
*/
func (r *RabbitMQ) GetPopulatedDataFrom(rabbitmq *RabbitMQ) *RabbitMQ {
	r.service = rabbitmq.service

	if rabbitmq.ConsumeData != nil {
		r.ConsumeData = &RMQConsume{
			UnacknowledgedDeliveryMap: rabbitmq.ConsumeData.UnacknowledgedDeliveryMap,
			OutgoingDeliveryChannel:   rabbitmq.ConsumeData.OutgoingDeliveryChannel,
			ExchangeName:              rabbitmq.ConsumeData.ExchangeName,
			ExchangeType:              rabbitmq.ConsumeData.ExchangeType,
			QueueName:                 rabbitmq.ConsumeData.QueueName,
			AccessKey:                 rabbitmq.ConsumeData.AccessKey,
			Qos:                       rabbitmq.ConsumeData.Qos,
			PurgeBeforeStarting:       rabbitmq.ConsumeData.PurgeBeforeStarting,
		}
	}

	if rabbitmq.PublishData != nil {
		r.PublishData = &RMQPublish{
			ExchangeName: rabbitmq.PublishData.ExchangeName,
			ExchangeType: rabbitmq.PublishData.ExchangeType,
			QueueName:    rabbitmq.PublishData.QueueName,
			AccessKey:    rabbitmq.PublishData.AccessKey,
		}
	}

	return r
}

/*
Populate the object for a consume behaviour.

NEVER USE THE SAME OBJECT FOR CONSUME AND PUBLISHING.
Keep in mind that PublishData object will be filled with standard failed messages queue data used by Acknowledge() to store failed messages.
You can use PopulatePublish() afterwards for customization of failed messages destination queue.

The consumed messages will be sended to the channel passed to queueConsumeChannel.
*/
func (r *RabbitMQ) PopulateConsume(exchangeName string, exchangeType string, queueName string, accessKey string, qos int, purgeBeforeStarting bool, queueConsumeChannel chan<- interface{}) *RabbitMQ {
	if r.ConsumeData == nil {
		r.ConsumeData = newRMQConsume()
		r.ConsumeData.Channel = newChannelData()
	}

	r.ConsumeData.populate(exchangeName, exchangeType, queueName, accessKey, qos, purgeBeforeStarting, queueConsumeChannel)

	defaultFailedMessagesExchangeName := "failed"
	defaultFailedMessagesExchangeType := "direct"
	defaultFailedMessagesQueueName := "_" + exchangeName + "__failed_messages"
	defaultFailedMessagesAccessKey := defaultFailedMessagesQueueName
	r.PopulatePublish(defaultFailedMessagesExchangeName, defaultFailedMessagesExchangeType, defaultFailedMessagesQueueName, defaultFailedMessagesAccessKey)

	return r
}

/*
Populate the object for a publish behaviour.
*/
func (r *RabbitMQ) PopulatePublish(exchangeName string, exchangeType string, queueName string, accessKey string) *RabbitMQ {
	if r.PublishData == nil {
		r.PublishData = newRMQPublish()
		r.PublishData.Channel = newChannelData()
	}

	r.PublishData.populate(exchangeName, exchangeType, queueName, accessKey)

	return r
}

/*
Delete a queue and a exchange, thinked to use at tests.

safePassword asserts that you're sure of it.
*/
func DeleteQueueAndExchange(channel *amqp.Channel, queueName string, exchangeName string, safePassword string) (err error) {
	if safePassword == "doit" {
		_, err = channel.QueueDelete(queueName, false, false, false)
		if err != nil {
			return errors.New("can't delete queue: " + err.Error())
		}

		err = channel.ExchangeDelete(exchangeName, false, false)
		if err != nil {
			return errors.New("can't delete exchange: " + err.Error())
		}

	} else {
		return errors.New("can't delete: you seem not sure of it")
	}

	return nil
}
