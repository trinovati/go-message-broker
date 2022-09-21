package rabbitmq

import (
	"errors"

	messagebroker "gitlab.com/aplicacao/trinovati-connector-message-brokers"

	"github.com/streadway/amqp"
)

/*
Object containing methods to prepare a consumer or publisher to RabbitMQ service, operating as client, RPC client or RPC server.
*/
type RabbitMQ struct {
	Service                 string
	Connection              *amqp.Connection
	serverAddress           string
	ConsumeData             *RMQConsume
	PublishData             *RMQPublish
	RemoteProcedureCallData *RMQRemoteProcedureCall
	semaphore               messagebroker.SemaphoreManager
}

/*
Build a object containing methods to prepare a consumer or publisher to RabbitMQ service, operating as client, RPC client or RPC server.

By default, the object will try to access the environmental variable RABBITMQ_SERVER for connection purpose, in case of unexistent, it will use 'amqp://guest:guest@localhost:5672/' address.

By default, the object will try to access the environmental variable RABBITMQ_SERVICE for behaviour purpose, in case of unexistent, it will use 'client' behaviour.
*/
func NewRabbitMQ(semaphore messagebroker.SemaphoreManager) *RabbitMQ {
	return &RabbitMQ{
		Service:                 RABBITMQ_SERVICE,
		serverAddress:           RABBITMQ_SERVER,
		ConsumeData:             nil,
		PublishData:             nil,
		RemoteProcedureCallData: nil,
		semaphore:               semaphore,
	}
}

/*
Change the address the service will try to connect.
*/
func (r *RabbitMQ) ChangeServerAddress(serverAddress string) {
	r.serverAddress = serverAddress
}

/*
Change the behaviour of the service.
*/
func (r *RabbitMQ) ChangeService(service string) {
	r.Service = service
}

/*
Populate the object, preparing for a RABBITMQ_CLIENT behaviour consume.
*/
func (r *RabbitMQ) PopulateConsume(exchangeName string, exchangeType string, queueName string, accessKey string, qos int, purgeBeforeStarting bool, queueConsumeChannel chan<- interface{}) {
	r.ConsumeData = newRMQConsume(queueConsumeChannel)

	r.ConsumeData.populate(exchangeName, exchangeType, queueName, accessKey, qos, purgeBeforeStarting)
}

/*
Populate the object, preparing for a RABBITMQ_CLIENT behaviour publish.
*/
func (r *RabbitMQ) PopulatePublish(exchangeName string, exchangeType string, queueName string, accessKey string) {
	r.PublishData = newRMQPublish()

	r.PublishData.populate(exchangeName, exchangeType, queueName, accessKey)
}

/*
Populate the object, preparing for a RABBITMQ_RPC_CLIENT behaviour.
*/
func (r *RabbitMQ) PopulateRPCClient(RPCExchangeName string, RPCExchangeType string, RPCQueueName string, RPCAccessKey string, callbackExchangeName string, callbackExchangeType string, callbackQueueName string, CallbackAccessKey string, tagProducerManager messagebroker.TagProducerManager) {
	if r.RemoteProcedureCallData == nil {
		r.RemoteProcedureCallData = newRMQRemoteProcedureCall()
	}

	r.RemoteProcedureCallData.RPCClient = newRPCClient()

	r.RemoteProcedureCallData.RPCClient.populate(RPCExchangeName, RPCExchangeType, RPCQueueName, RPCAccessKey, callbackExchangeName, callbackExchangeType, callbackQueueName, CallbackAccessKey, tagProducerManager)
}

/*
Populate the object, preparing for a RABBITMQ_RPC_SERVER behaviour.
*/
func (r *RabbitMQ) PopulateRPCServer(RPCExchangeName string, RPCExchangeType string, RPCQueueName string, RPCAccessKey string, RPCQos int, RPCPurgeBeforeStarting bool, callbackExchangeName string, callbackExchangeType string, callbackQueueName string, callbackAccessKey string, RPCQueueConsumeChannel chan<- interface{}) {
	if r.RemoteProcedureCallData == nil {
		r.RemoteProcedureCallData = newRMQRemoteProcedureCall()
	}

	r.RemoteProcedureCallData.RPCServer = newRPCServer()

	r.RemoteProcedureCallData.RPCServer.populate(RPCExchangeName, RPCExchangeType, RPCQueueName, RPCAccessKey, RPCQos, RPCPurgeBeforeStarting, callbackExchangeName, callbackExchangeType, callbackQueueName, callbackAccessKey, RPCQueueConsumeChannel)
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
