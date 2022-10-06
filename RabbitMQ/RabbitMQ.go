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
	ConnectionId uint64
	Connection   *ConnectionData
	Channel      *Channel
	service      string
	ConsumeData  *RMQConsume
	PublishData  *RMQPublish
}

/*
Object used to reference a amqp.Channel address.

Since the connection have a intimate relation with the address of amqp.Channel, it could not be moved to another memory position for
shared channel purposes, so all shared channels points toward this object, and in case of channel remake, this object will point towards it.
*/
type Channel struct {
	Channel *amqp.Channel
}

/*
Build a object containing methods to prepare a consumer or publisher to RabbitMQ service.

terminateOnConnectionError defines if, at any moment, the connections fail or comes down, the service will panic or retry connection.

By default, the object will try to access the environmental variable RABBITMQ_SERVER for connection purpose, in case of unexistent, it will use 'amqp://guest:guest@localhost:5672/' address.

By default, the object will try to access the environmental variable RABBITMQ_SERVICE for behaviour purpose, in case of unexistent, it will use 'client' behaviour.
*/
func NewRabbitMQ() *RabbitMQ {
	return &RabbitMQ{
		ConnectionId: 0,
		Connection:   newConnectionData(),
		Channel:      &Channel{Channel: nil},
		service:      RABBITMQ_CLIENT,
		ConsumeData:  nil,
		PublishData:  nil,
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
		r.ConnectionId = rabbitmq.ConnectionId

	} else {
		log.Println("in " + errorFileIdentification + ": WARNING!!! shared connection is nil pointer")
	}

	return r
}

/*
Will make both objects share the same channel, connection information, and server interaction semaphore for assincronus access.
*/
func (r *RabbitMQ) SharesChannelWith(rabbitmq *RabbitMQ) *RabbitMQ {
	errorFileIdentification := "RabbitMQ.go at SharesChannelWith()"

	channelExists := rabbitmq.Channel != nil
	connectionExists := rabbitmq.Connection != nil && rabbitmq.Connection.Connection != nil

	if channelExists {
		if connectionExists {
			r.SharesConnectionWith(rabbitmq)

			r.Channel = rabbitmq.Channel

		} else {
			log.Println("in " + errorFileIdentification + ": WARNING!!! shared connection is nil pointer")
		}

	} else {
		log.Println("in " + errorFileIdentification + ": WARNING!!! shared channel is nil pointer")
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
			UnacknowledgedDeliveryMap:  rabbitmq.ConsumeData.UnacknowledgedDeliveryMap,
			OutgoingDeliveryChannel:    rabbitmq.ConsumeData.OutgoingDeliveryChannel,
			ExchangeName:               rabbitmq.ConsumeData.ExchangeName,
			ExchangeType:               rabbitmq.ConsumeData.ExchangeType,
			QueueName:                  rabbitmq.ConsumeData.QueueName,
			AccessKey:                  rabbitmq.ConsumeData.AccessKey,
			ErrorNotificationQueueName: rabbitmq.ConsumeData.ErrorNotificationQueueName,
			Qos:                        rabbitmq.ConsumeData.Qos,
			PurgeBeforeStarting:        rabbitmq.ConsumeData.PurgeBeforeStarting,
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

The messages will be sended at the channel passed to queueConsumeChannel.
*/
func (r *RabbitMQ) PopulateConsume(exchangeName string, exchangeType string, queueName string, accessKey string, qos int, purgeBeforeStarting bool, queueConsumeChannel chan<- interface{}) *RabbitMQ {
	if r.ConsumeData == nil {
		r.ConsumeData = newRMQConsume()
	}

	r.ConsumeData.populate(exchangeName, exchangeType, queueName, accessKey, qos, purgeBeforeStarting, queueConsumeChannel)

	return r
}

/*
Populate the object for a publish behaviour.
*/
func (r *RabbitMQ) PopulatePublish(exchangeName string, exchangeType string, queueName string, accessKey string) *RabbitMQ {
	if r.PublishData == nil {
		r.PublishData = newRMQPublish()
	}

	r.PublishData.populate(exchangeName, exchangeType, queueName, accessKey)

	return r
}

/*
Prepare a channel linked to RabbitMQ connection for publishing.

It puts the channel in confirm mode, so any publishing done will have a response from the server.
*/
func (r *RabbitMQ) prepareChannel() (err error) {
	errorFileIdentification := "RabbitMQ.go at prepareChannel()"

	if r.Channel == nil || r.Channel.Channel == nil || r.Channel.Channel.IsClosed() {

		if r.isConnectionDown() {
			completeError := "in " + errorFileIdentification + ": connection dropped before creating channel, trying again soon"
			return errors.New(completeError)
		}

		channel, err := r.Connection.Connection.Channel()
		if err != nil {
			return errors.New("error creating a channel linked to RabbitMQ in " + errorFileIdentification + ": " + err.Error())
		}
		r.Channel.Channel = channel

		if r.isConnectionDown() {
			return errors.New("in " + errorFileIdentification + ": connection dropped before configuring publish channel, trying again soon")
		}

		err = r.Channel.Channel.Confirm(false)
		if err != nil {
			return errors.New("error configuring channel with Confirm() protocol in " + errorFileIdentification + ": " + err.Error())
		}

	}

	return nil
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
