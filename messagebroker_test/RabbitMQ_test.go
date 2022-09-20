package testing

import (
	"log"
	"strconv"
	"testing"
	"time"
	"trinovati-message-handler/services/connector/messagebroker"
	rabbitmq "trinovati-message-handler/services/connector/messagebroker/RabbitMQ"

	"github.com/streadway/amqp"
)

type testTagProducer struct {
}

func (t *testTagProducer) MakeUniqueTag() string {
	return "TAG"
}

type testSemaphore struct {
}

func (t *testSemaphore) GetPermission(permissionsTaken int64) (err error) {
	return nil
}

func (t *testSemaphore) ReleasePermission(permissionsReleased int64) {
}

func TestPopulatePublishRabbitMQ(t *testing.T) {
	exchangeName := "exchange"
	exchangeType := "type"
	queueName := "queue_name"
	queueAccessKey := "access_key"

	semaphore := &testSemaphore{}

	messageBroker := rabbitmq.NewRabbitMQ(rabbitmq.RABBITMQ_CLIENT, semaphore)

	messageBroker.PopulatePublish(exchangeName, exchangeType, queueName, queueAccessKey)

	if messageBroker.PublishData.ExchangeName != exchangeName {
		t.Error("error at ExchangeName.\nexpected: " + exchangeName + "\ngot:      " + messageBroker.PublishData.ExchangeName)
	}

	if messageBroker.PublishData.ExchangeType != exchangeType {
		t.Error("error at ExchangeType.\nexpected: " + exchangeType + "\ngot:      " + messageBroker.PublishData.ExchangeType)
	}

	if messageBroker.PublishData.QueueName != queueName {
		t.Error("error at QueueName.\nexpected: " + queueName + "\ngot:      " + messageBroker.PublishData.QueueName)
	}

	if messageBroker.PublishData.AccessKey != queueAccessKey {
		t.Error("error at AccessKey.\nexpected: " + queueAccessKey + "\ngot:      " + messageBroker.PublishData.AccessKey)
	}

	if messageBroker.ConsumeData != nil {
		t.Error("error at ConsumeData. Should be a nil pointer, since PopulatePublish should not touch ConsumeData")
	}

	if messageBroker.RemoteProcedureCallData != nil {
		t.Error("error at RemoteProcedureCallData. Should be a nil pointer, since PopulateConsume should not touch RemoteProcedureCallData")
	}

	if messageBroker.Connection != nil {
		t.Error("error at Connection. Should be a nil pointer, since PopulatePublish should not touch Connection")
	}
}

func TestPopulateConsumeRabbitMQ(t *testing.T) {
	exchangeName := "exchange"
	exchangeType := "type"
	queueName := "queue_name"
	queueAccessKey := "access_key"
	expectedNotifyQueueName := "_" + exchangeName + "__failed_messages"
	qos := 2
	purgeBeforeStarting := true
	queueConsumeChannel := make(chan messagebroker.ConsumedMessage)

	semaphore := &testSemaphore{}

	messageBroker := rabbitmq.NewRabbitMQ(rabbitmq.RABBITMQ_CLIENT, semaphore)

	messageBroker.PopulateConsume(exchangeName, exchangeType, queueName, queueAccessKey, qos, purgeBeforeStarting, queueConsumeChannel)

	if messageBroker.ConsumeData.ExchangeName != exchangeName {
		t.Error("error at ExchangeName.\nexpected: " + exchangeName + "\ngot:      " + messageBroker.PublishData.ExchangeName)
	}

	if messageBroker.ConsumeData.ExchangeType != exchangeType {
		t.Error("error at ExchangeType.\nexpected: " + exchangeType + "\ngot:      " + messageBroker.ConsumeData.ExchangeType)
	}

	if messageBroker.ConsumeData.QueueName != queueName {
		t.Error("error at QueueName.\nexpected: " + queueName + "\ngot:      " + messageBroker.ConsumeData.QueueName)
	}

	if messageBroker.ConsumeData.AccessKey != queueAccessKey {
		t.Error("error at AccessKey.\nexpected: " + queueAccessKey + "\ngot:      " + messageBroker.ConsumeData.AccessKey)
	}

	if messageBroker.ConsumeData.NotifyQueueName != expectedNotifyQueueName {
		t.Error("error at NotifyQueueName.\nexpected: " + expectedNotifyQueueName + "\ngot:      " + messageBroker.ConsumeData.NotifyQueueName)
	}

	if messageBroker.ConsumeData.Qos != qos {
		t.Error("error at Qos.\nexpected: " + strconv.Itoa(qos) + "\ngot:      " + strconv.Itoa(messageBroker.ConsumeData.Qos))
	}

	if messageBroker.ConsumeData.PurgeBeforeStarting != purgeBeforeStarting {
		t.Error("error at PurgeBeforeStarting.\nexpected: " + strconv.FormatBool(purgeBeforeStarting) + "\ngot:      " + strconv.FormatBool(messageBroker.ConsumeData.PurgeBeforeStarting))
	}

	if messageBroker.ConsumeData.QueueConsumeChannel != queueConsumeChannel {
		t.Error("error at QueueConsumeChannel. Unexpected pointer.")
	}

	if messageBroker.ConsumeData.MessagesMap == nil {
		t.Error("error at MessagesMap. Should be a valid map")
	}

	if messageBroker.PublishData != nil {
		t.Error("error at PublishData. Should be a nil pointer, since PopulateConsume should not touch PublishData")
	}

	if messageBroker.RemoteProcedureCallData != nil {
		t.Error("error at RemoteProcedureCallData. Should be a nil pointer, since PopulateConsume should not touch RemoteProcedureCallData")
	}

	if messageBroker.Connection != nil {
		t.Error("error at Connection. Should be a nil pointer, since PopulateConsume should not touch Connection")
	}
}

func TestPopulateRPCClient(t *testing.T) {
	RPCExchangeName := "RPC_exchange"
	RPCExchangeType := "RPC_type"
	RPCQueueName := "RPC_queue_name"
	RPCAccessKey := "RPC_access_key"

	callbackExchangeName := "callback_exchange"
	callbackExchangeType := "callback_type"
	callbackQueueName := "callback_queue_name"
	callbackAccessKey := "callback_access_key"

	expectedNotifyQueueName := "_" + callbackExchangeName + "__failed_messages"

	semaphore := &testSemaphore{}

	messageBroker := rabbitmq.NewRabbitMQ(rabbitmq.RABBITMQ_RPC_CLIENT, semaphore)

	messageBroker.PopulateRPCClient(RPCExchangeName, RPCExchangeType, RPCQueueName, RPCAccessKey, callbackExchangeName, callbackExchangeType, callbackQueueName, callbackAccessKey, nil)

	if messageBroker.RemoteProcedureCallData.RPCClient.Publisher.ExchangeName != RPCExchangeName {
		t.Error("error at ExchangeName.\nexpected: " + RPCExchangeName + "\ngot:      " + messageBroker.RemoteProcedureCallData.RPCClient.Publisher.ExchangeName)
	}

	if messageBroker.RemoteProcedureCallData.RPCClient.Publisher.ExchangeType != RPCExchangeType {
		t.Error("error at ExchangeType.\nexpected: " + RPCExchangeType + "\ngot:      " + messageBroker.RemoteProcedureCallData.RPCClient.Publisher.ExchangeType)
	}

	if messageBroker.RemoteProcedureCallData.RPCClient.Publisher.QueueName != RPCQueueName {
		t.Error("error at QueueName.\nexpected: " + RPCQueueName + "\ngot:      " + messageBroker.RemoteProcedureCallData.RPCClient.Publisher.QueueName)
	}

	if messageBroker.RemoteProcedureCallData.RPCClient.Publisher.AccessKey != RPCAccessKey {
		t.Error("error at AccessKey.\nexpected: " + RPCAccessKey + "\ngot:      " + messageBroker.RemoteProcedureCallData.RPCClient.Publisher.AccessKey)
	}

	if messageBroker.RemoteProcedureCallData.RPCClient.TagProducerManager != nil {
		t.Error("error at tag producer manager. Should be a nil pointer, since PopulateRPCClient should not touch TagProducerManager")
	}

	if messageBroker.RemoteProcedureCallData.RPCClient.Callback.ExchangeName != callbackExchangeName {
		t.Error("error at callback ExchangeName.\nexpected: " + callbackExchangeName + "\ngot:      " + messageBroker.RemoteProcedureCallData.RPCClient.Callback.ExchangeName)
	}

	if messageBroker.RemoteProcedureCallData.RPCClient.Callback.ExchangeType != callbackExchangeType {
		t.Error("error at callback ExchangeType.\nexpected: " + callbackExchangeType + "\ngot:      " + messageBroker.RemoteProcedureCallData.RPCClient.Callback.ExchangeType)
	}

	if messageBroker.RemoteProcedureCallData.RPCClient.Callback.QueueName != callbackQueueName {
		t.Error("error at callback QueueName.\nexpected: " + callbackQueueName + "\ngot:      " + messageBroker.RemoteProcedureCallData.RPCClient.Callback.QueueName)
	}

	if messageBroker.RemoteProcedureCallData.RPCClient.Callback.AccessKey != callbackAccessKey {
		t.Error("error at callback AccessKey.\nexpected: " + callbackAccessKey + "\ngot:      " + messageBroker.RemoteProcedureCallData.RPCClient.Callback.AccessKey)
	}

	if messageBroker.RemoteProcedureCallData.RPCClient.Callback.NotifyQueueName != expectedNotifyQueueName {
		t.Error("error at callback AccessKey.\nexpected: " + expectedNotifyQueueName + "\ngot:      " + messageBroker.RemoteProcedureCallData.RPCClient.Callback.NotifyQueueName)
	}

	if messageBroker.RemoteProcedureCallData.RPCClient.Callback.QueueConsumeChannel != nil {
		t.Error("error at consume queue channel. Should be a nil pointer")
	}

	if messageBroker.RemoteProcedureCallData.RPCClient.Callback.MessagesMap == nil {
		t.Error("error at consume messages map. Should be a valid pointer")
	}

	if messageBroker.PublishData != nil {
		t.Error("error at PublishData. Should be a nil pointer, since PopulateRPCClient should not touch PublishData")
	}

	if messageBroker.ConsumeData != nil {
		t.Error("error at ConsumeData. Should be a nil pointer, since PopulateRPCClient should not touch ConsumeData")
	}

	if messageBroker.Connection != nil {
		t.Error("error at Connection. Should be a nil pointer, since PopulateRPCClient should not touch Connection")
	}

}

func TestPopulateRPCServer(t *testing.T) {
	RPCexchangeName := "RPC_exchange"
	RPCexchangeType := "RPC_type"
	RPCQueueName := "RPC_queue_name"
	RPCQueueAccessKey := "RPC_access_key"
	RPCqos := 2
	RPCpurgeBeforeStarting := true

	callbackExchangeName := "callback_exchange"
	callbackExchangeType := "callback_type"
	callbackQueueName := "callback_queue_name"
	callbackAccessKey := "callback_access_key"

	queueConsumeChannel := make(chan messagebroker.ConsumedMessage)

	expectedNotifyQueueName := "_" + RPCexchangeName + "__failed_messages"

	semaphore := &testSemaphore{}

	messageBroker := rabbitmq.NewRabbitMQ(rabbitmq.RABBITMQ_RPC_SERVER, semaphore)

	messageBroker.PopulateRPCServer(RPCexchangeName, RPCexchangeType, RPCQueueName, RPCQueueAccessKey, RPCqos, RPCpurgeBeforeStarting, callbackExchangeName, callbackExchangeType, callbackQueueName, callbackAccessKey, queueConsumeChannel)

	if messageBroker.RemoteProcedureCallData.RPCServer.Consumer.ExchangeName != RPCexchangeName {
		t.Error("error at consumer ExchangeName.\nexpected: " + RPCexchangeName + "\ngot:      " + messageBroker.PublishData.ExchangeName)
	}

	if messageBroker.RemoteProcedureCallData.RPCServer.Consumer.ExchangeType != RPCexchangeType {
		t.Error("error at consumer ExchangeType.\nexpected: " + RPCexchangeType + "\ngot:      " + messageBroker.RemoteProcedureCallData.RPCServer.Consumer.ExchangeType)
	}

	if messageBroker.RemoteProcedureCallData.RPCServer.Consumer.QueueName != RPCQueueName {
		t.Error("error at consumer QueueName.\nexpected: " + RPCQueueName + "\ngot:      " + messageBroker.RemoteProcedureCallData.RPCServer.Consumer.QueueName)
	}

	if messageBroker.RemoteProcedureCallData.RPCServer.Consumer.AccessKey != RPCQueueAccessKey {
		t.Error("error at consumer AccessKey.\nexpected: " + RPCQueueAccessKey + "\ngot:      " + messageBroker.RemoteProcedureCallData.RPCServer.Consumer.AccessKey)
	}

	if messageBroker.RemoteProcedureCallData.RPCServer.Consumer.NotifyQueueName != expectedNotifyQueueName {
		t.Error("error at consumer NotifyQueueName.\nexpected: " + expectedNotifyQueueName + "\ngot:      " + messageBroker.RemoteProcedureCallData.RPCServer.Consumer.NotifyQueueName)
	}

	if messageBroker.RemoteProcedureCallData.RPCServer.Consumer.Qos != RPCqos {
		t.Error("error at consumer Qos.\nexpected: " + strconv.Itoa(RPCqos) + "\ngot:      " + strconv.Itoa(messageBroker.RemoteProcedureCallData.RPCServer.Consumer.Qos))
	}

	if messageBroker.RemoteProcedureCallData.RPCServer.Consumer.PurgeBeforeStarting != RPCpurgeBeforeStarting {
		t.Error("error at consumer PurgeBeforeStarting.\nexpected: " + strconv.FormatBool(RPCpurgeBeforeStarting) + "\ngot:      " + strconv.FormatBool(messageBroker.RemoteProcedureCallData.RPCServer.Consumer.PurgeBeforeStarting))
	}

	if messageBroker.RemoteProcedureCallData.RPCServer.Consumer.QueueConsumeChannel != queueConsumeChannel {
		t.Error("error at consumer QueueConsumeChannel. Unexpected pointer.")
	}

	if messageBroker.RemoteProcedureCallData.RPCServer.Consumer.MessagesMap == nil {
		t.Error("error at consumer MessagesMap. Should be a valid map")
	}

	if messageBroker.RemoteProcedureCallData.RPCServer.Callback.ExchangeName != callbackExchangeName {
		t.Error("error at callback ExchangeName.\nexpected: " + callbackExchangeName + "\ngot:      " + messageBroker.RemoteProcedureCallData.RPCServer.Callback.ExchangeName)
	}

	if messageBroker.RemoteProcedureCallData.RPCServer.Callback.ExchangeType != callbackExchangeType {
		t.Error("error at callback ExchangeType.\nexpected: " + callbackExchangeType + "\ngot:      " + messageBroker.RemoteProcedureCallData.RPCServer.Callback.ExchangeType)
	}

	if messageBroker.RemoteProcedureCallData.RPCServer.Callback.QueueName != callbackQueueName {
		t.Error("error at callback QueueName.\nexpected: " + callbackQueueName + "\ngot:      " + messageBroker.RemoteProcedureCallData.RPCServer.Callback.QueueName)
	}

	if messageBroker.RemoteProcedureCallData.RPCServer.Callback.AccessKey != callbackAccessKey {
		t.Error("error at callback AccessKey.\nexpected: " + callbackAccessKey + "\ngot:      " + messageBroker.RemoteProcedureCallData.RPCServer.Callback.AccessKey)
	}

	if messageBroker.PublishData != nil {
		t.Error("error at PublishData. Should be a nil pointer, since PopulateRPCServer should not touch PublishData")
	}

	if messageBroker.ConsumeData != nil {
		t.Error("error at ConsumeData. Should be a nil pointer, since PopulateRPCServer should not touch ConsumeData")
	}

	if messageBroker.Connection != nil {
		t.Error("error at Connection. Should be a nil pointer, since PopulateRPCServer should not touch Connection")
	}
}

func TestMakeCopyRabbitMQ(t *testing.T) {
	publishExchangeName := "publishExchange"
	publishExchangeType := "publishType"
	publishQueueName := "publishQueue"
	publishQueueAccessKey := "publishAccess"
	consumerExchangeName := "consumerExchange"
	consumerExchangeType := "consumerType"
	consumerQueueName := "consumerQueue"
	consumerQueueAccessKey := "consumerAccess"
	expectedNotifyQueueName := "_" + consumerExchangeName + "__failed_messages"
	RPCexchangeName := "RPC_exchange"
	RPCexchangeType := "RPC_type"
	RPCQueueName := "RPC_queue_name"
	RPCQueueAccessKey := "RPC_access_key"
	expectedRPCNotifyQueueName := "_" + RPCexchangeName + "__failed_messages"

	callbackExchangeName := "callback_exchange"
	callbackExchangeType := "callback_type"
	callbackQueueName := "callback_queue_name"
	callbackAccessKey := "callback_access_key"
	expectedCallbackNotifyQueueName := "_" + callbackExchangeName + "__failed_messages"
	qos := 2
	purgeBeforeStarting := true
	queueConsumeChannel := make(chan messagebroker.ConsumedMessage)

	tagProducer := &testTagProducer{}
	semaphore := &testSemaphore{}

	messageBroker := rabbitmq.NewRabbitMQ(rabbitmq.RABBITMQ_CLIENT, semaphore)

	messageBroker.PopulatePublish(publishExchangeName, publishExchangeType, publishQueueName, publishQueueAccessKey)
	messageBroker.PopulateConsume(consumerExchangeName, consumerExchangeType, consumerQueueName, consumerQueueAccessKey, qos, purgeBeforeStarting, queueConsumeChannel)
	messageBroker.PopulateRPCClient(RPCexchangeName, RPCexchangeType, RPCQueueName, RPCQueueAccessKey, callbackExchangeName, callbackExchangeType, callbackQueueName, callbackAccessKey, tagProducer)
	messageBroker.PopulateRPCServer(RPCexchangeName, RPCexchangeType, RPCQueueName, RPCQueueAccessKey, qos, purgeBeforeStarting, callbackExchangeName, callbackExchangeType, callbackQueueName, callbackAccessKey, queueConsumeChannel)

	if messageBroker.PublishData.ExchangeName != publishExchangeName {
		t.Error("error at ExchangeName.\nexpected: " + publishExchangeName + "\ngot:      " + messageBroker.PublishData.ExchangeName)
	}

	if messageBroker.PublishData.ExchangeType != publishExchangeType {
		t.Error("error at ExchangeType.\nexpected: " + publishExchangeType + "\ngot:      " + messageBroker.PublishData.ExchangeType)
	}

	if messageBroker.PublishData.QueueName != publishQueueName {
		t.Error("error at QueueName.\nexpected: " + publishQueueName + "\ngot:      " + messageBroker.PublishData.QueueName)
	}

	if messageBroker.PublishData.AccessKey != publishQueueAccessKey {
		t.Error("error at AccessKey.\nexpected: " + publishQueueAccessKey + "\ngot:      " + messageBroker.PublishData.AccessKey)
	}

	if messageBroker.ConsumeData.ExchangeName != consumerExchangeName {
		t.Error("error at ExchangeName.\nexpected: " + consumerExchangeName + "\ngot:      " + messageBroker.ConsumeData.ExchangeName)
	}

	if messageBroker.ConsumeData.ExchangeType != consumerExchangeType {
		t.Error("error at ExchangeType.\nexpected: " + consumerExchangeType + "\ngot:      " + messageBroker.ConsumeData.ExchangeType)
	}

	if messageBroker.ConsumeData.QueueName != consumerQueueName {
		t.Error("error at QueueName.\nexpected: " + consumerQueueName + "\ngot:      " + messageBroker.ConsumeData.QueueName)
	}

	if messageBroker.ConsumeData.AccessKey != consumerQueueAccessKey {
		t.Error("error at AccessKey.\nexpected: " + consumerQueueAccessKey + "\ngot:      " + messageBroker.ConsumeData.AccessKey)
	}

	if messageBroker.ConsumeData.NotifyQueueName != expectedNotifyQueueName {
		t.Error("error at NotifyQueueName.\nexpected: " + expectedNotifyQueueName + "\ngot:      " + messageBroker.ConsumeData.NotifyQueueName)
	}

	if messageBroker.ConsumeData.Qos != qos {
		t.Error("error at Qos.\nexpected: " + strconv.Itoa(qos) + "\ngot:      " + strconv.Itoa(messageBroker.ConsumeData.Qos))
	}

	if messageBroker.ConsumeData.PurgeBeforeStarting != purgeBeforeStarting {
		t.Error("error at PurgeBeforeStarting.\nexpected: " + strconv.FormatBool(purgeBeforeStarting) + "\ngot:      " + strconv.FormatBool(messageBroker.ConsumeData.PurgeBeforeStarting))
	}

	if messageBroker.ConsumeData.QueueConsumeChannel != queueConsumeChannel {
		t.Error("error at QueueConsumeChannel. Unexpected pointer.")
	}

	if messageBroker.ConsumeData.MessagesMap == nil {
		t.Error("error at MessagesMap. Should be a valid map")
	}

	if messageBroker.RemoteProcedureCallData.RPCClient.Publisher.ExchangeName != RPCexchangeName {
		t.Error("error at ExchangeName.\nexpected: " + RPCexchangeName + "\ngot:      " + messageBroker.RemoteProcedureCallData.RPCClient.Publisher.ExchangeName)
	}

	if messageBroker.RemoteProcedureCallData.RPCClient.Publisher.ExchangeType != RPCexchangeType {
		t.Error("error at ExchangeType.\nexpected: " + RPCexchangeType + "\ngot:      " + messageBroker.RemoteProcedureCallData.RPCClient.Publisher.ExchangeType)
	}

	if messageBroker.RemoteProcedureCallData.RPCClient.Publisher.QueueName != RPCQueueName {
		t.Error("error at QueueName.\nexpected: " + RPCQueueName + "\ngot:      " + messageBroker.RemoteProcedureCallData.RPCClient.Publisher.QueueName)
	}

	if messageBroker.RemoteProcedureCallData.RPCClient.Publisher.AccessKey != RPCQueueAccessKey {
		t.Error("error at AccessKey.\nexpected: " + RPCQueueAccessKey + "\ngot:      " + messageBroker.RemoteProcedureCallData.RPCClient.Publisher.AccessKey)
	}

	if messageBroker.RemoteProcedureCallData.RPCClient.Callback.ExchangeName != callbackExchangeName {
		t.Error("error at ExchangeName.\nexpected: " + callbackExchangeName + "\ngot:      " + messageBroker.RemoteProcedureCallData.RPCClient.Callback.ExchangeName)
	}

	if messageBroker.RemoteProcedureCallData.RPCClient.Callback.ExchangeType != callbackExchangeType {
		t.Error("error at ExchangeType.\nexpected: " + callbackExchangeType + "\ngot:      " + messageBroker.RemoteProcedureCallData.RPCClient.Callback.ExchangeType)
	}

	if messageBroker.RemoteProcedureCallData.RPCClient.Callback.QueueName != callbackQueueName {
		t.Error("error at QueueName.\nexpected: " + callbackQueueName + "\ngot:      " + messageBroker.RemoteProcedureCallData.RPCClient.Callback.QueueName)
	}

	if messageBroker.RemoteProcedureCallData.RPCClient.Callback.AccessKey != callbackAccessKey {
		t.Error("error at AccessKey.\nexpected: " + callbackAccessKey + "\ngot:      " + messageBroker.RemoteProcedureCallData.RPCClient.Callback.AccessKey)
	}

	if messageBroker.RemoteProcedureCallData.RPCClient.Callback.NotifyQueueName != expectedCallbackNotifyQueueName {
		t.Error("error at NotifyQueueName.\nexpected: " + expectedCallbackNotifyQueueName + "\ngot:      " + messageBroker.RemoteProcedureCallData.RPCClient.Callback.NotifyQueueName)
	}

	if messageBroker.RemoteProcedureCallData.RPCClient.Callback.Qos != 1 {
		t.Error("error at Qos.\nexpected: " + strconv.Itoa(1) + "\ngot:      " + strconv.Itoa(messageBroker.RemoteProcedureCallData.RPCClient.Callback.Qos))
	}

	if messageBroker.RemoteProcedureCallData.RPCClient.Callback.PurgeBeforeStarting != false {
		t.Error("error at PurgeBeforeStarting.\nexpected: " + strconv.FormatBool(false) + "\ngot:      " + strconv.FormatBool(messageBroker.RemoteProcedureCallData.RPCClient.Callback.PurgeBeforeStarting))
	}

	if messageBroker.RemoteProcedureCallData.RPCClient.Callback.QueueConsumeChannel != nil {
		t.Error("error at QueueConsumeChannel. Unexpected pointer, should be nil.")
	}

	if messageBroker.RemoteProcedureCallData.RPCClient.Callback.MessagesMap == nil {
		t.Error("error at MessagesMap. Should be a valid map")
	}

	if messageBroker.RemoteProcedureCallData.RPCClient.TagProducerManager == nil {
		t.Error("error at TagProducerManager. Should be a valid object")
	}

	if messageBroker.RemoteProcedureCallData.RPCServer.Callback.ExchangeName != callbackExchangeName {
		t.Error("error at ExchangeName.\nexpected: " + callbackExchangeName + "\ngot:      " + messageBroker.RemoteProcedureCallData.RPCServer.Callback.ExchangeName)
	}

	if messageBroker.RemoteProcedureCallData.RPCServer.Callback.ExchangeType != callbackExchangeType {
		t.Error("error at ExchangeType.\nexpected: " + callbackExchangeType + "\ngot:      " + messageBroker.RemoteProcedureCallData.RPCServer.Callback.ExchangeType)
	}

	if messageBroker.RemoteProcedureCallData.RPCServer.Callback.QueueName != callbackQueueName {
		t.Error("error at QueueName.\nexpected: " + callbackQueueName + "\ngot:      " + messageBroker.RemoteProcedureCallData.RPCServer.Callback.QueueName)
	}

	if messageBroker.RemoteProcedureCallData.RPCServer.Callback.AccessKey != callbackAccessKey {
		t.Error("error at AccessKey.\nexpected: " + callbackAccessKey + "\ngot:      " + messageBroker.RemoteProcedureCallData.RPCServer.Callback.AccessKey)
	}

	if messageBroker.RemoteProcedureCallData.RPCServer.Consumer.ExchangeName != RPCexchangeName {
		t.Error("error at ExchangeName.\nexpected: " + RPCexchangeName + "\ngot:      " + messageBroker.RemoteProcedureCallData.RPCServer.Consumer.ExchangeName)
	}

	if messageBroker.RemoteProcedureCallData.RPCServer.Consumer.ExchangeType != RPCexchangeType {
		t.Error("error at ExchangeType.\nexpected: " + RPCexchangeType + "\ngot:      " + messageBroker.RemoteProcedureCallData.RPCServer.Consumer.ExchangeType)
	}

	if messageBroker.RemoteProcedureCallData.RPCServer.Consumer.QueueName != RPCQueueName {
		t.Error("error at QueueName.\nexpected: " + RPCQueueName + "\ngot:      " + messageBroker.RemoteProcedureCallData.RPCServer.Consumer.QueueName)
	}

	if messageBroker.RemoteProcedureCallData.RPCServer.Consumer.AccessKey != RPCQueueAccessKey {
		t.Error("error at AccessKey.\nexpected: " + RPCQueueAccessKey + "\ngot:      " + messageBroker.RemoteProcedureCallData.RPCServer.Consumer.AccessKey)
	}

	if messageBroker.RemoteProcedureCallData.RPCServer.Consumer.NotifyQueueName != expectedRPCNotifyQueueName {
		t.Error("error at NotifyQueueName.\nexpected: " + expectedRPCNotifyQueueName + "\ngot:      " + messageBroker.RemoteProcedureCallData.RPCServer.Consumer.NotifyQueueName)
	}

	if messageBroker.RemoteProcedureCallData.RPCServer.Consumer.Qos != qos {
		t.Error("error at Qos.\nexpected: " + strconv.Itoa(qos) + "\ngot:      " + strconv.Itoa(messageBroker.RemoteProcedureCallData.RPCServer.Consumer.Qos))
	}

	if messageBroker.RemoteProcedureCallData.RPCServer.Consumer.PurgeBeforeStarting != purgeBeforeStarting {
		t.Error("error at PurgeBeforeStarting.\nexpected: " + strconv.FormatBool(purgeBeforeStarting) + "\ngot:      " + strconv.FormatBool(messageBroker.RemoteProcedureCallData.RPCServer.Consumer.PurgeBeforeStarting))
	}

	if messageBroker.RemoteProcedureCallData.RPCServer.Consumer.QueueConsumeChannel != queueConsumeChannel {
		t.Error("error at QueueConsumeChannel. Unexpected pointer.")
	}

	if messageBroker.RemoteProcedureCallData.RPCServer.Consumer.MessagesMap == nil {
		t.Error("error at MessagesMap. Should be a valid map")
	}

	if messageBroker.Connection != nil {
		t.Error("error at Connection. Should be a nil pointer, since PopulatePublish should not touch Connection")
	}
}

func TestPublishRabbitMQ(t *testing.T) {
	message := "teste001"

	publishExchangeName := "tests"
	publishExchangeType := "direct"
	publishQueueName := "test__messagehandler_Publish()"
	publishQueueAccessKey := publishQueueName
	consumerExchangeName := publishExchangeName
	consumerExchangeType := publishExchangeType
	consumerQueueName := publishQueueName
	consumerAccessKey := consumerQueueName
	consumerQos := 0

	semaphore := &testSemaphore{}

	messageBrokerPublisher := rabbitmq.NewRabbitMQ(rabbitmq.RABBITMQ_CLIENT, semaphore)
	messageBrokerConsumer := rabbitmq.NewRabbitMQ(rabbitmq.RABBITMQ_CLIENT, semaphore)

	messageBrokerPublisher.PopulatePublish(publishExchangeName, publishExchangeType, publishQueueName, publishQueueAccessKey)

	messageBrokerConsumer.Connect()
	defer messageBrokerConsumer.Connection.Close()

	channel, err := messageBrokerConsumer.Connection.Channel()
	if err != nil {
		t.Error("error creating a channel. " + err.Error())
	}
	defer channel.Close()

	err = channel.ExchangeDeclare(consumerExchangeName, consumerExchangeType, true, false, false, false, nil)
	if err != nil {
		t.Error("error creating RabbitMQ exchange: " + err.Error())
	}
	_, err = channel.QueueDeclare(consumerQueueName, true, false, false, false, nil)
	if err != nil {
		t.Error("error creating RabbitMQ queue: " + err.Error())
	}

	err = channel.QueueBind(consumerQueueName, consumerAccessKey, consumerExchangeName, false, nil)
	if err != nil {
		t.Error("error binding RabbitMQ queue: " + err.Error())
	}

	_, err = channel.QueuePurge(consumerQueueName, true)
	if err != nil {
		t.Error("error purging the queue. " + err.Error())
	}

	err = channel.Qos(consumerQos, 0, false)
	if err != nil {
		t.Error("error Qos() a channel, limiting the maximum message ConsumeRMQ queue can hold: " + err.Error())
	}

	err = messageBrokerPublisher.Publish(message)
	if err != nil {
		t.Error("error publishing to queue. " + err.Error())
	}

	deliveryChannel, err := channel.Consume(consumerQueueName, "", true, false, false, false, nil)
	if err != nil {
		t.Error("error consuming the queue. " + err.Error())
	}

	recievedMessage := <-deliveryChannel

	if string(recievedMessage.Body) != message {
		t.Error("error at with message body.\nexpected: " + message + "\ngot:      " + string(recievedMessage.Body))
	}

	err = channel.Ack(recievedMessage.DeliveryTag, false)
	if err != nil {
		t.Error("error acknowledging: " + err.Error())
	}

	log.Println(string(recievedMessage.Body))
}

func TestConsumeForeverRabbitMQ(t *testing.T) {
	var messages []string
	messages = append(messages, "teste001", "teste002", "teste003")

	publishExchangeName := "tests"
	publishExchangeType := "direct"
	publishQueueName := "test__messagehandler_ConsumeForever()"
	publishQueueAccessKey := publishQueueName

	consumerExchangeName := publishExchangeName
	consumerExchangeType := publishExchangeType
	consumerQueueName := publishQueueName
	consumerQueueAccessKey := consumerQueueName
	qos := 0
	purgeBeforeStarting := true
	queueConsumeChannel := make(chan messagebroker.ConsumedMessage)

	semaphore := &testSemaphore{}

	messageBrokerConsumer := rabbitmq.NewRabbitMQ(rabbitmq.RABBITMQ_CLIENT, semaphore)
	messageBrokerPublisher := rabbitmq.NewRabbitMQ(rabbitmq.RABBITMQ_CLIENT, semaphore)

	messageBrokerConsumer.PopulateConsume(consumerExchangeName, consumerExchangeType, consumerQueueName, consumerQueueAccessKey, qos, purgeBeforeStarting, queueConsumeChannel)

	messageBrokerPublisher.Connect()
	defer messageBrokerPublisher.Connection.Close()

	channel, err := messageBrokerPublisher.Connection.Channel()
	if err != nil {
		t.Error("error creating a channel. " + err.Error())
	}
	defer channel.Close()

	err = channel.Confirm(false)
	if err != nil {
		t.Error("error configuring channel with Confirm() protocol: " + err.Error())
	}

	notifyFlowChannel := channel.NotifyFlow(make(chan bool))
	notifyAck, notifyNack := channel.NotifyConfirm(make(chan uint64), make(chan uint64))

	go messageBrokerConsumer.ConsumeForever()

	time.Sleep(2 * time.Second)

	for i := 0; i < len(messages); i++ {
		select {
		case <-notifyFlowChannel:
			waitingTimeForFlow := 10 * time.Second
			t.Error("Queue flow is closed, waiting for " + waitingTimeForFlow.String() + " seconds to try publish again.")
			continue

		default:
			err = channel.Publish(publishExchangeName, publishQueueAccessKey, false, false, amqp.Publishing{ContentType: "application/json", Body: []byte(messages[i])})
			if err != nil {
				t.Error("error publishing message: " + err.Error())
			}

			select {
			case deniedNack := <-notifyNack:
				waitingTimeForRedelivery := 10 * time.Second
				log.Println("Publishing Nack" + strconv.Itoa(int(deniedNack)) + " denied by Queue, waiting for " + waitingTimeForRedelivery.String() + " seconds to try redeliver.")
				time.Sleep(waitingTimeForRedelivery)
				continue

			case successAck := <-notifyAck:
				log.Println("Publishing Ack" + strconv.Itoa(int(successAck)) + " recieved at " + publishQueueName + ".")
				break
			}
		}

		recievedMessage := <-queueConsumeChannel

		transmissionData := string(recievedMessage.TransmissionData.([]byte))

		if transmissionData != messages[i] {
			t.Error("error at consume.\nexpected: " + messages[i] + "\ngot:      " + transmissionData)
		}

		err = messageBrokerConsumer.Acknowledge(*messagebroker.NewConsumedMessageAcknowledger(true, "success", recievedMessage.MessageId, ""))
		if err != nil {
			t.Error("error with acknowlege: " + err.Error())
		}

		log.Println(transmissionData)
	}

	time.Sleep(3 * time.Second)
}

func TestRPCClientRequestPublish(t *testing.T) {
	message := "teste001"

	RPCExchangeName := "tests"
	RPCExchangeType := "direct"
	RPCQueueName := "test__messagehandler_RPCClientRequestPublish()"
	RPCAccessKey := RPCQueueName

	callbackExchangeName := RPCExchangeName
	callbackExchangeType := RPCExchangeType
	callbackQueueName := RPCQueueName + "__callback"
	callbackAccessKey := callbackQueueName

	consumerQos := 0

	tagProducerManager := &testTagProducer{}
	semaphore := &testSemaphore{}

	messageBrokerRequestPublisher := rabbitmq.NewRabbitMQ(rabbitmq.RABBITMQ_RPC_CLIENT, semaphore)
	messageBrokerConsumer := rabbitmq.NewRabbitMQ(rabbitmq.RABBITMQ_RPC_CLIENT, semaphore)

	messageBrokerRequestPublisher.PopulateRPCClient(RPCExchangeName, RPCExchangeType, RPCQueueName, RPCAccessKey, callbackExchangeName, callbackExchangeType, callbackQueueName, callbackAccessKey, tagProducerManager)

	messageBrokerConsumer.Connect()
	defer messageBrokerConsumer.Connection.Close()

	channel, err := messageBrokerConsumer.Connection.Channel()
	if err != nil {
		t.Error("error creating a channel. " + err.Error())
	}
	defer channel.Close()

	err = channel.ExchangeDeclare(RPCExchangeName, RPCExchangeType, true, false, false, false, nil)
	if err != nil {
		t.Error("error creating RabbitMQ exchange: " + err.Error())
	}

	_, err = channel.QueueDeclare(RPCQueueName, true, false, false, false, nil)
	if err != nil {
		t.Error("error creating RabbitMQ queue: " + err.Error())
	}

	err = channel.QueueBind(RPCQueueName, RPCAccessKey, RPCExchangeName, false, nil)
	if err != nil {
		t.Error("error binding RabbitMQ queue: " + err.Error())
	}

	_, err = channel.QueuePurge(RPCQueueName, true)
	if err != nil {
		t.Error("error purging the queue. " + err.Error())
	}

	err = channel.Qos(consumerQos, 0, false)
	if err != nil {
		t.Error("error Qos() a channel, limiting the maximum message ConsumeRMQ queue can hold: " + err.Error())
	}

	correlationId, err := messageBrokerRequestPublisher.RPCClientRequestPublish(message)
	if err != nil {
		t.Error("error publishing RPC request to rabbitmq: " + err.Error())
	}

	deliveryChannel, err := channel.Consume(RPCQueueName, "", true, false, false, false, nil)
	if err != nil {
		t.Error("error consuming the queue. " + err.Error())
	}

	recievedMessage := <-deliveryChannel

	if string(recievedMessage.Body) != message {
		t.Error("error at with message body.\nexpected: " + message + "\ngot:      " + string(recievedMessage.Body))
	}

	if correlationId != recievedMessage.CorrelationId {
		t.Error("error at with message body.\nexpected: " + correlationId + "\ngot:      " + recievedMessage.CorrelationId)
	}

	err = channel.Ack(recievedMessage.DeliveryTag, false)
	if err != nil {
		t.Error("error acknowledging: " + err.Error())
	}

	log.Println(string(recievedMessage.Body))
}

func TestRPCServerResquestConsumeForever(t *testing.T) {
	var messages []string
	messages = append(messages, "teste001", "teste002", "teste003")

	RPCExchangeName := "tests"
	RPCExchangeType := "direct"
	RPCQueueName := "test__messagehandler_RPCServerResquestConsumeForever()"
	RPCAccessKey := RPCQueueName
	RPCQos := 0
	RPCPurgeBeforeStarting := true

	RPCQueueConsumeChannel := make(chan messagebroker.ConsumedMessage)

	callbackExchangeName := RPCExchangeName
	callbackExchangeType := RPCExchangeType
	callbackQueueName := RPCQueueName + "__callback"
	callbackAccessKey := callbackQueueName

	semaphore := &testSemaphore{}

	messageBrokerRequestConsumer := rabbitmq.NewRabbitMQ(rabbitmq.RABBITMQ_RPC_SERVER, semaphore)
	messageBrokerPublisher := rabbitmq.NewRabbitMQ(rabbitmq.RABBITMQ_RPC_SERVER, semaphore)

	messageBrokerRequestConsumer.PopulateRPCServer(RPCExchangeName, RPCExchangeType, RPCQueueName, RPCAccessKey, RPCQos, RPCPurgeBeforeStarting, callbackExchangeName, callbackExchangeType, callbackQueueName, callbackAccessKey, RPCQueueConsumeChannel)

	messageBrokerPublisher.Connect()
	defer messageBrokerPublisher.Connection.Close()

	go messageBrokerRequestConsumer.RPCServerResquestConsumeForever()

	channel, err := messageBrokerPublisher.Connection.Channel()
	if err != nil {
		t.Error("error creating a channel. " + err.Error())
	}
	defer channel.Close()

	err = channel.Confirm(false)
	if err != nil {
		t.Error("error configuring channel with Confirm() protocol: " + err.Error())
	}

	notifyFlowChannel := channel.NotifyFlow(make(chan bool))
	notifyAck, notifyNack := channel.NotifyConfirm(make(chan uint64), make(chan uint64))

	time.Sleep(2 * time.Second)

	for i := 0; i < len(messages); i++ {
		select {
		case <-notifyFlowChannel:
			waitingTimeForFlow := 10 * time.Second
			t.Error("Queue flow is closed, waiting for " + waitingTimeForFlow.String() + " seconds to try publish again.")
			continue

		default:
			err = channel.Publish(RPCExchangeName, RPCAccessKey, false, false, amqp.Publishing{ContentType: "application/json", Body: []byte(messages[i]), CorrelationId: strconv.Itoa(i)})
			if err != nil {
				t.Error("error publishing message: " + err.Error())
			}

			select {
			case deniedNack := <-notifyNack:
				waitingTimeForRedelivery := 10 * time.Second
				t.Error("Publishing Nack" + strconv.Itoa(int(deniedNack)) + " denied by Queue, waiting for " + waitingTimeForRedelivery.String() + " seconds to try redeliver.")
				continue

			case successAck := <-notifyAck:
				log.Println("Publishing Ack" + strconv.Itoa(int(successAck)) + " recieved at " + RPCQueueName + ".")
				break
			}
		}

		recievedMessage := <-RPCQueueConsumeChannel

		RPCData := recievedMessage.TransmissionData.(*messagebroker.RPCDataDto)

		if RPCData.CorrelationId != strconv.Itoa(i) {
			t.Error("error at correlation ID.\nexpected: " + strconv.Itoa(i) + "\ngot:      " + RPCData.CorrelationId)
		}

		if string(RPCData.Data) != messages[i] {
			t.Error("error at consume.\nexpected: " + messages[i] + "\ngot:      " + string(RPCData.Data))
		}

		err = messageBrokerRequestConsumer.Acknowledge(*messagebroker.NewConsumedMessageAcknowledger(true, "success", recievedMessage.MessageId, ""))
		if err != nil {
			t.Error("error with acknowlege: " + err.Error())
		}

		log.Println(string(RPCData.Data))
	}

	time.Sleep(3 * time.Second)
}

func TestRPCServerCallbackPublish(t *testing.T) {
	message := "teste001"
	correlationId := "333"

	RPCExchangeName := "tests"
	RPCExchangeType := "direct"
	RPCQueueName := "test__messagehandler_RPCServerCallbackPublish()"
	RPCAccessKey := RPCQueueName

	callbackExchangeName := RPCExchangeName
	callbackExchangeType := RPCExchangeType
	callbackQueueName := RPCQueueName + "__callback"
	callbackAccessKey := callbackQueueName

	RPCQos := 0
	RPCPurgeBeforeStarting := true

	RPCQueueConsumeChannel := make(chan messagebroker.ConsumedMessage)

	semaphore := &testSemaphore{}

	messageBrokerResponsePublisher := rabbitmq.NewRabbitMQ(rabbitmq.RABBITMQ_RPC_SERVER, semaphore)
	messageBrokerConsumer := rabbitmq.NewRabbitMQ(rabbitmq.RABBITMQ_RPC_SERVER, semaphore)

	messageBrokerResponsePublisher.PopulateRPCServer(RPCExchangeName, RPCExchangeType, RPCQueueName, RPCAccessKey, RPCQos, RPCPurgeBeforeStarting, callbackExchangeName, callbackExchangeType, callbackQueueName, callbackAccessKey, RPCQueueConsumeChannel)

	messageBrokerConsumer.Connect()
	defer messageBrokerConsumer.Connection.Close()

	channel, err := messageBrokerConsumer.Connection.Channel()
	if err != nil {
		t.Error("error creating a channel. " + err.Error())
	}
	defer channel.Close()

	err = channel.ExchangeDeclare(callbackExchangeName, callbackExchangeType, true, false, false, false, nil)
	if err != nil {
		t.Error("error creating RabbitMQ exchange: " + err.Error())
	}
	_, err = channel.QueueDeclare(callbackQueueName, true, false, false, false, nil)
	if err != nil {
		t.Error("error creating RabbitMQ queue: " + err.Error())
	}

	err = channel.QueueBind(callbackQueueName, callbackAccessKey, callbackExchangeName, false, nil)
	if err != nil {
		t.Error("error binding RabbitMQ queue: " + err.Error())
	}

	_, err = channel.QueuePurge(callbackQueueName, true)
	if err != nil {
		t.Error("error purging the queue. " + err.Error())
	}

	err = channel.Qos(RPCQos, 0, false)
	if err != nil {
		t.Error("error Qos() a channel, limiting the maximum message ConsumeRMQ queue can hold: " + err.Error())
	}

	err = messageBrokerResponsePublisher.RPCServerCallbackPublish(message, correlationId, callbackQueueName)
	if err != nil {
		t.Error("error publishing RPC request to rabbitmq: " + err.Error())
	}

	deliveryChannel, err := channel.Consume(callbackQueueName, "", true, false, false, false, nil)
	if err != nil {
		t.Error("error consuming the queue. " + err.Error())
	}

	recievedMessage := <-deliveryChannel

	if string(recievedMessage.Body) != message {
		t.Error("error at with message body.\nexpected: " + message + "\ngot:      " + string(recievedMessage.Body))
	}

	if correlationId != recievedMessage.CorrelationId {
		t.Error("error at with message body.\nexpected: " + correlationId + "\ngot:      " + recievedMessage.CorrelationId)
	}

	err = channel.Ack(recievedMessage.DeliveryTag, false)
	if err != nil {
		t.Error("error acknowledging: " + err.Error())
	}

	log.Println(string(recievedMessage.Body))
}

func TestRPCClientCallbackConsume(t *testing.T) {
	message := "teste001"
	correlationId := "333"

	RPCExchangeName := "tests"
	RPCExchangeType := "direct"
	RPCQueueName := "test__messagehandler_RPCClientCallbackConsume()"
	RPCAccessKey := RPCQueueName

	callbackExchangeName := RPCExchangeName
	callbackExchangeType := RPCExchangeType
	callbackQueueName := RPCQueueName + "__callback"
	callbackAccessKey := callbackQueueName

	semaphore := &testSemaphore{}

	messageBrokerResponseConsumer := rabbitmq.NewRabbitMQ(rabbitmq.RABBITMQ_RPC_CLIENT, semaphore)
	messageBrokerPublisher := rabbitmq.NewRabbitMQ(rabbitmq.RABBITMQ_RPC_CLIENT, semaphore)

	messageBrokerResponseConsumer.PopulateRPCClient(RPCExchangeName, RPCExchangeType, RPCQueueName, RPCAccessKey, callbackExchangeName, callbackExchangeType, callbackQueueName, callbackAccessKey, nil)

	messageBrokerPublisher.Connect()
	defer messageBrokerPublisher.Connection.Close()

	channel, err := messageBrokerPublisher.Connection.Channel()
	if err != nil {
		t.Error("error creating a channel. " + err.Error())
	}
	defer channel.Close()

	err = channel.ExchangeDeclare(callbackExchangeName, callbackExchangeType, true, false, false, false, nil)
	if err != nil {
		t.Error("error creating RabbitMQ exchange: " + err.Error())
	}
	_, err = channel.QueueDeclare(callbackQueueName, true, false, false, false, nil)
	if err != nil {
		t.Error("error creating RabbitMQ queue: " + err.Error())
	}

	err = channel.QueueBind(callbackQueueName, callbackAccessKey, callbackExchangeName, false, nil)
	if err != nil {
		t.Error("error binding RabbitMQ queue: " + err.Error())
	}

	_, err = channel.QueuePurge(callbackQueueName, true)
	if err != nil {
		t.Error("error purging the queue. " + err.Error())
	}

	err = channel.Confirm(false)
	if err != nil {
		t.Error("error configuring channel with Confirm() protocol: " + err.Error())
	}

	notifyFlowChannel := channel.NotifyFlow(make(chan bool))
	notifyAck, notifyNack := channel.NotifyConfirm(make(chan uint64), make(chan uint64))

	time.Sleep(2 * time.Second)

	select {
	case <-notifyFlowChannel:
		waitingTimeForFlow := 10 * time.Second
		t.Error("Queue flow is closed, waiting for " + waitingTimeForFlow.String() + " seconds to try publish again.")

	default:
		err = channel.Publish(callbackExchangeName, callbackAccessKey, false, false, amqp.Publishing{ContentType: "application/json", Body: []byte(message), CorrelationId: correlationId})
		if err != nil {
			t.Error("error publishing message: " + err.Error())
		}

		select {
		case deniedNack := <-notifyNack:
			waitingTimeForRedelivery := 10 * time.Second
			t.Error("Publishing Nack" + strconv.Itoa(int(deniedNack)) + " denied by Queue, waiting for " + waitingTimeForRedelivery.String() + " seconds to try redeliver.")

		case successAck := <-notifyAck:
			log.Println("Publishing Ack" + strconv.Itoa(int(successAck)) + " recieved at " + callbackQueueName + ".")
			break
		}
	}

	recievedMessage, err := messageBrokerResponseConsumer.RPCClientCallbackConsume(correlationId)
	if err != nil {
		t.Error("error recieving message: " + err.Error())
	}

	if string(recievedMessage) != message {
		t.Error("error at consume.\nexpected: " + message + "\ngot:      " + string(recievedMessage))
	}

	log.Println(string(recievedMessage))

	time.Sleep(3 * time.Second)
}
