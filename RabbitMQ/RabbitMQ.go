package rabbitmq

import (
	messagebroker "gitlab.com/aplicacao/trinovati-connector-message-brokers"

	"github.com/streadway/amqp"
)

type RabbitMQ struct {
	Service                 string
	Connection              *amqp.Connection
	serverAddress           string
	ConsumeData             *RMQConsume
	PublishData             *RMQPublish
	RemoteProcedureCallData *RMQRemoteProcedureCall
	semaphore               messagebroker.SemaphoreManager
}

func NewRabbitMQ(service string, semaphore messagebroker.SemaphoreManager) *RabbitMQ {
	return &RabbitMQ{
		Service:                 service,
		serverAddress:           RABBITMQ_SERVER,
		ConsumeData:             nil,
		PublishData:             nil,
		RemoteProcedureCallData: nil,
		semaphore:               semaphore,
	}
}

func (r *RabbitMQ) PopulateConsume(exchangeName string, exchangeType string, queueName string, accessKey string, qos int, purgeBeforeStarting bool, queueConsumeChannel chan<- interface{}) {
	r.ConsumeData = newRMQConsume(queueConsumeChannel)

	r.ConsumeData.populate(exchangeName, exchangeType, queueName, accessKey, qos, purgeBeforeStarting)
}

func (r *RabbitMQ) PopulatePublish(exchangeName string, exchangeType string, queueName string, accessKey string) {
	r.PublishData = newRMQPublish()

	r.PublishData.populate(exchangeName, exchangeType, queueName, accessKey)
}

func (r *RabbitMQ) PopulateRPCClient(RPCExchangeName string, RPCExchangeType string, RPCQueueName string, RPCAccessKey string, callbackExchangeName string, callbackExchangeType string, callbackQueueName string, CallbackAccessKey string, tagProducerManager messagebroker.TagProducerManager) {
	if r.RemoteProcedureCallData == nil {
		r.RemoteProcedureCallData = newRMQRemoteProcedureCall()
	}

	r.RemoteProcedureCallData.RPCClient = newRPCClient()

	r.RemoteProcedureCallData.RPCClient.populate(RPCExchangeName, RPCExchangeType, RPCQueueName, RPCAccessKey, callbackExchangeName, callbackExchangeType, callbackQueueName, CallbackAccessKey, tagProducerManager)
}

func (r *RabbitMQ) PopulateRPCServer(RPCExchangeName string, RPCExchangeType string, RPCQueueName string, RPCAccessKey string, RPCQos int, RPCPurgeBeforeStarting bool, callbackExchangeName string, callbackExchangeType string, callbackQueueName string, callbackAccessKey string, RPCQueueConsumeChannel chan<- interface{}) {
	if r.RemoteProcedureCallData == nil {
		r.RemoteProcedureCallData = newRMQRemoteProcedureCall()
	}

	r.RemoteProcedureCallData.RPCServer = newRPCServer()

	r.RemoteProcedureCallData.RPCServer.populate(RPCExchangeName, RPCExchangeType, RPCQueueName, RPCAccessKey, RPCQos, RPCPurgeBeforeStarting, callbackExchangeName, callbackExchangeType, callbackQueueName, callbackAccessKey, RPCQueueConsumeChannel)
}
