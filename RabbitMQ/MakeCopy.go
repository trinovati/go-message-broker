package rabbitmq

import (
	"sync"
)

/*
Make a copy of the RabbitMQ, generating a pointer to another object containing the some of information.

Keeps connection pointer, config variables.

Discards channels, maps.
*/
func (r *RabbitMQ) MakeCopy() *RabbitMQ {
	newRmqData := NewRabbitMQ(r.terminanteOnConnectionError)
	newRmqData.ChangeServerAddress(r.serverAddress)
	newRmqData.ChangeService(r.service)

	*newRmqData.Connection = *r.Connection

	if r.ConsumeData != nil {
		newRmqData.ConsumeData = newRMQConsume(r.ConsumeData.QueueConsumeChannel)

		newRmqData.ConsumeData.Channel = nil
		newRmqData.ConsumeData.MessagesMap = &sync.Map{}

		newRmqData.ConsumeData.ExchangeName = r.ConsumeData.ExchangeName
		newRmqData.ConsumeData.ExchangeType = r.ConsumeData.ExchangeType
		newRmqData.ConsumeData.QueueName = r.ConsumeData.QueueName
		newRmqData.ConsumeData.AccessKey = r.ConsumeData.AccessKey
		newRmqData.ConsumeData.NotifyQueueName = r.ConsumeData.NotifyQueueName
		newRmqData.ConsumeData.Qos = r.ConsumeData.Qos
		newRmqData.ConsumeData.PurgeBeforeStarting = r.ConsumeData.PurgeBeforeStarting
	}

	if r.PublishData != nil {
		newRmqData.PublishData = newRMQPublish()
		newRmqData.PublishData.Channel = nil
		newRmqData.PublishData.ExchangeName = r.PublishData.ExchangeName
		newRmqData.PublishData.ExchangeType = r.PublishData.ExchangeType
		newRmqData.PublishData.QueueName = r.PublishData.QueueName
		newRmqData.PublishData.AccessKey = r.PublishData.AccessKey
	}

	if r.RemoteProcedureCallData != nil {
		newRmqData.RemoteProcedureCallData = newRMQRemoteProcedureCall()

		if r.RemoteProcedureCallData.RPCServer != nil {
			newRmqData.RemoteProcedureCallData.RPCServer = newRPCServer()

			if r.RemoteProcedureCallData.RPCServer.Consumer != nil {
				newRmqData.RemoteProcedureCallData.RPCServer.Consumer = newRMQConsume(r.RemoteProcedureCallData.RPCServer.Consumer.QueueConsumeChannel)
				newRmqData.RemoteProcedureCallData.RPCServer.Consumer.Channel = nil
				newRmqData.RemoteProcedureCallData.RPCServer.Consumer.MessagesMap = &sync.Map{}

				newRmqData.RemoteProcedureCallData.RPCServer.Consumer.ExchangeName = r.RemoteProcedureCallData.RPCServer.Consumer.ExchangeName
				newRmqData.RemoteProcedureCallData.RPCServer.Consumer.ExchangeType = r.RemoteProcedureCallData.RPCServer.Consumer.ExchangeType
				newRmqData.RemoteProcedureCallData.RPCServer.Consumer.QueueName = r.RemoteProcedureCallData.RPCServer.Consumer.QueueName
				newRmqData.RemoteProcedureCallData.RPCServer.Consumer.AccessKey = r.RemoteProcedureCallData.RPCServer.Consumer.AccessKey
				newRmqData.RemoteProcedureCallData.RPCServer.Consumer.NotifyQueueName = r.RemoteProcedureCallData.RPCServer.Consumer.NotifyQueueName
				newRmqData.RemoteProcedureCallData.RPCServer.Consumer.Qos = r.RemoteProcedureCallData.RPCServer.Consumer.Qos
				newRmqData.RemoteProcedureCallData.RPCServer.Consumer.PurgeBeforeStarting = r.RemoteProcedureCallData.RPCServer.Consumer.PurgeBeforeStarting
			}

			if r.RemoteProcedureCallData.RPCServer.Callback != nil {
				newRmqData.RemoteProcedureCallData.RPCServer.Callback = newRMQPublish()
				newRmqData.RemoteProcedureCallData.RPCServer.Callback.Channel = nil
				newRmqData.RemoteProcedureCallData.RPCServer.Callback.ExchangeName = r.RemoteProcedureCallData.RPCServer.Callback.ExchangeName
				newRmqData.RemoteProcedureCallData.RPCServer.Callback.ExchangeType = r.RemoteProcedureCallData.RPCServer.Callback.ExchangeType
				newRmqData.RemoteProcedureCallData.RPCServer.Callback.QueueName = r.RemoteProcedureCallData.RPCServer.Callback.QueueName
				newRmqData.RemoteProcedureCallData.RPCServer.Callback.AccessKey = r.RemoteProcedureCallData.RPCServer.Callback.AccessKey
			}
		}

		if r.RemoteProcedureCallData.RPCClient != nil {
			newRmqData.RemoteProcedureCallData.RPCClient = newRPCClient()

			if r.RemoteProcedureCallData.RPCClient.Publisher != nil {
				newRmqData.RemoteProcedureCallData.RPCClient.Publisher = newRMQPublish()
				newRmqData.RemoteProcedureCallData.RPCClient.Publisher.Channel = nil
				newRmqData.RemoteProcedureCallData.RPCClient.Publisher.ExchangeName = r.RemoteProcedureCallData.RPCClient.Publisher.ExchangeName
				newRmqData.RemoteProcedureCallData.RPCClient.Publisher.ExchangeType = r.RemoteProcedureCallData.RPCClient.Publisher.ExchangeType
				newRmqData.RemoteProcedureCallData.RPCClient.Publisher.QueueName = r.RemoteProcedureCallData.RPCClient.Publisher.QueueName
				newRmqData.RemoteProcedureCallData.RPCClient.Publisher.AccessKey = r.RemoteProcedureCallData.RPCClient.Publisher.AccessKey

				newRmqData.RemoteProcedureCallData.RPCClient.TagProducerManager = r.RemoteProcedureCallData.RPCClient.TagProducerManager
			}

			if r.RemoteProcedureCallData.RPCClient.Callback != nil {
				newRmqData.RemoteProcedureCallData.RPCClient.Callback = newRMQConsume(nil)
				newRmqData.RemoteProcedureCallData.RPCClient.Callback.Channel = nil
				newRmqData.RemoteProcedureCallData.RPCClient.Callback.MessagesMap = &sync.Map{}

				newRmqData.RemoteProcedureCallData.RPCClient.Callback.ExchangeName = r.RemoteProcedureCallData.RPCClient.Callback.ExchangeName
				newRmqData.RemoteProcedureCallData.RPCClient.Callback.ExchangeType = r.RemoteProcedureCallData.RPCClient.Callback.ExchangeType
				newRmqData.RemoteProcedureCallData.RPCClient.Callback.QueueName = r.RemoteProcedureCallData.RPCClient.Callback.QueueName
				newRmqData.RemoteProcedureCallData.RPCClient.Callback.AccessKey = r.RemoteProcedureCallData.RPCClient.Callback.AccessKey
				newRmqData.RemoteProcedureCallData.RPCClient.Callback.NotifyQueueName = r.RemoteProcedureCallData.RPCClient.Callback.NotifyQueueName
				newRmqData.RemoteProcedureCallData.RPCClient.Callback.Qos = r.RemoteProcedureCallData.RPCClient.Callback.Qos
				newRmqData.RemoteProcedureCallData.RPCClient.Callback.PurgeBeforeStarting = r.RemoteProcedureCallData.RPCClient.Callback.PurgeBeforeStarting
			}
		}
	}

	return newRmqData
}
