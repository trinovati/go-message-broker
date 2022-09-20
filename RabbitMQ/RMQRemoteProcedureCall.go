package rabbitmq

import (
	"trinovati-message-handler/services/connector/messagebroker"
)

/*
Object that holds the structs for Remote Procedure Call Client and Server.
*/
type RMQRemoteProcedureCall struct {
	RPCServer *RPCServer
	RPCClient *RPCClient
}

/*
Create an object that holds the structs for Remote Procedure Call Client and Server.
*/
func newRMQRemoteProcedureCall() *RMQRemoteProcedureCall {
	return &RMQRemoteProcedureCall{}
}

/*
Object that holds information needed by a Remote Procedure Call Client.
*/
type RPCClient struct {
	Publisher          *RMQPublish
	Callback           *RMQConsume
	TagProducerManager messagebroker.TagProducerManager
}

/*
Create an object that holds information needed by a Remote Procedure Call Client.
*/
func newRPCClient() *RPCClient {
	return &RPCClient{}
}

/*
Populate a Remote Procedure Call Client with information needed for publishing a RPC request and consuming a RPC response.
*/
func (r *RPCClient) populate(RPCExchangeName string, RPCExchangeType string, RPCQueueName string, RPCAccessKey string, callbackExchangeName string, callbackExchangeType string, callbackQueueName string, callbackAccessKey string, tagProducerManager messagebroker.TagProducerManager) {
	r.Publisher = newRMQPublish()
	r.Publisher.populate(RPCExchangeName, RPCExchangeType, RPCQueueName, RPCAccessKey)

	r.Callback = newRMQConsume(nil)
	r.Callback.populate(callbackExchangeName, callbackExchangeType, callbackQueueName, callbackAccessKey, 1, false)

	r.TagProducerManager = tagProducerManager
}

/*
Object that holds information needed by a Remote Procedure Call Server.
*/
type RPCServer struct {
	Consumer *RMQConsume
	Callback *RMQPublish
}

/*
Create an object that holds information needed by a Remote Procedure Call Server.
*/
func newRPCServer() *RPCServer {
	return &RPCServer{}
}

/*
Populate a Remote Procedure Call Server with information needed for consuming a RPC request and publishing a RPC response.
*/
func (r *RPCServer) populate(RPCExchangeName string, RPCExchangeType string, RPCQueueName string, RPCAccessKey string, RPCQos int, RPCPurgeBeforeStarting bool, callbackExchangeName string, callbackExchangeType string, callbackQueueName string, callbackAccessKey string, RPCQueueConsumeChannel chan<- messagebroker.ConsumedMessage) {
	r.Consumer = newRMQConsume(RPCQueueConsumeChannel)
	r.Consumer.populate(RPCExchangeName, RPCExchangeType, RPCQueueName, RPCAccessKey, RPCQos, RPCPurgeBeforeStarting)

	r.Callback = newRMQPublish()
	r.Callback.populate(callbackExchangeName, callbackExchangeType, callbackQueueName, callbackAccessKey)
}
