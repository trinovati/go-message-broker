package testing

import (
	"context"
	"strconv"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	messagebroker "gitlab.com/aplicacao/trinovati-connector-message-brokers"
	rabbitmq "gitlab.com/aplicacao/trinovati-connector-message-brokers/RabbitMQ"
)

func TestPopulatePublishRabbitMQ(t *testing.T) {
	exchangeName := "exchange"
	exchangeType := "type"
	queueName := "queue_name"
	queueAccessKey := "access_key"

	messageBroker := rabbitmq.NewRabbitMQ().PopulatePublish(exchangeName, exchangeType, queueName, queueAccessKey)

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

	if messageBroker.Connection == nil {
		t.Error("error at Connection. Should not be a valid pointer")
	}
}

func TestPopulateConsumeRabbitMQ(t *testing.T) {
	exchangeName := "exchange"
	exchangeType := "type"
	queueName := "queue_name"
	queueAccessKey := "access_key"
	errorNotificationQueueName := "_" + exchangeName + "__failed_messages"
	qos := 2
	purgeBeforeStarting := true
	outgoingDeliveryChannel := make(chan interface{})

	messageBroker := rabbitmq.NewRabbitMQ().PopulateConsume(exchangeName, exchangeType, queueName, queueAccessKey, qos, purgeBeforeStarting, outgoingDeliveryChannel)

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

	if messageBroker.ConsumeData.ErrorNotificationQueueName != errorNotificationQueueName {
		t.Error("error at NotifyQueueName.\nexpected: " + errorNotificationQueueName + "\ngot:      " + messageBroker.ConsumeData.ErrorNotificationQueueName)
	}

	if messageBroker.ConsumeData.Qos != qos {
		t.Error("error at Qos.\nexpected: " + strconv.Itoa(qos) + "\ngot:      " + strconv.Itoa(messageBroker.ConsumeData.Qos))
	}

	if messageBroker.ConsumeData.PurgeBeforeStarting != purgeBeforeStarting {
		t.Error("error at PurgeBeforeStarting.\nexpected: " + strconv.FormatBool(purgeBeforeStarting) + "\ngot:      " + strconv.FormatBool(messageBroker.ConsumeData.PurgeBeforeStarting))
	}

	if messageBroker.ConsumeData.OutgoingDeliveryChannel != outgoingDeliveryChannel {
		t.Error("error at OutgoingDeliveryChannel. Unexpected pointer.")
	}

	if messageBroker.ConsumeData.UnacknowledgedDeliveryMap == nil {
		t.Error("error at UnacknowledgedDeliveryMap. Should be a valid map")
	}

	if messageBroker.PublishData != nil {
		t.Error("error at PublishData. Should be a nil pointer, since PopulateConsume should not touch PublishData")
	}

	if messageBroker.Connection == nil {
		t.Error("error at Connection. Should not be a valid pointer")
	}
}

func TestGetPopulatedDataFrom(t *testing.T) {
	publishExchangeName := "publishExchange"
	publishExchangeType := "publishType"
	publishQueueName := "publishQueue"
	publishQueueAccessKey := "publishAccess"

	consumerExchangeName := "consumerExchange"
	consumerExchangeType := "consumerType"
	consumerQueueName := "consumerQueue"
	consumerQueueAccessKey := "consumerAccess"
	expectedErrorNotificationQueueName := "_" + consumerExchangeName + "__failed_messages"
	qos := 2
	purgeBeforeStarting := true
	outgoingDeliveryChannel := make(chan interface{})

	baseMessageBroker := rabbitmq.NewRabbitMQ().PopulatePublish(publishExchangeName, publishExchangeType, publishQueueName, publishQueueAccessKey)
	messageBroker := rabbitmq.NewRabbitMQ().GetPopulatedDataFrom(baseMessageBroker)

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

	if messageBroker.Connection == nil {
		t.Error("error at Connection. Should be a valid pointer")
	}

	baseMessageBroker = rabbitmq.NewRabbitMQ().PopulateConsume(consumerExchangeName, consumerExchangeType, consumerQueueName, consumerQueueAccessKey, qos, purgeBeforeStarting, outgoingDeliveryChannel)
	messageBroker = rabbitmq.NewRabbitMQ().GetPopulatedDataFrom(baseMessageBroker)

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

	if messageBroker.ConsumeData.ErrorNotificationQueueName != expectedErrorNotificationQueueName {
		t.Error("error at ErrorNotificationQueueName.\nexpected: " + expectedErrorNotificationQueueName + "\ngot:      " + messageBroker.ConsumeData.ErrorNotificationQueueName)
	}

	if messageBroker.ConsumeData.Qos != qos {
		t.Error("error at Qos.\nexpected: " + strconv.Itoa(qos) + "\ngot:      " + strconv.Itoa(messageBroker.ConsumeData.Qos))
	}

	if messageBroker.ConsumeData.PurgeBeforeStarting != purgeBeforeStarting {
		t.Error("error at PurgeBeforeStarting.\nexpected: " + strconv.FormatBool(purgeBeforeStarting) + "\ngot:      " + strconv.FormatBool(messageBroker.ConsumeData.PurgeBeforeStarting))
	}

	if messageBroker.ConsumeData.OutgoingDeliveryChannel != outgoingDeliveryChannel {
		t.Error("error at OutgoingDeliveryChannel. Unexpected pointer.")
	}

	if messageBroker.ConsumeData.UnacknowledgedDeliveryMap != baseMessageBroker.ConsumeData.UnacknowledgedDeliveryMap {
		t.Error("error at UnacknowledgedDeliveryMap. Should be the same map")
	}

	if messageBroker.Connection == nil {
		t.Error("error at Connection. Should be a valid pointer")
	}
}

func TestSharesConnectionWith(t *testing.T) {
	baseMessageBroker := rabbitmq.NewRabbitMQ().Connect()
	messageBroker := rabbitmq.NewRabbitMQ().SharesConnectionWith(baseMessageBroker)

	if messageBroker.Connection != baseMessageBroker.Connection {
		t.Error("error at Connection, both pointers should be the same")
	}
}

func TestSharesChannelWith(t *testing.T) {
	var err error

	baseMessageBroker := rabbitmq.NewRabbitMQ().Connect()
	baseMessageBroker.Channel.Channel, err = baseMessageBroker.Connection.Connection.Channel()
	if err != nil {
		t.Error("error creating channel " + err.Error())
	}

	messageBroker := rabbitmq.NewRabbitMQ().SharesChannelWith(baseMessageBroker)

	if messageBroker.Connection != baseMessageBroker.Connection {
		t.Error("error at Connection, both pointers should be the same")
	}

	if messageBroker.Channel != baseMessageBroker.Channel {
		t.Error("error at Channel, both pointers should be the same")
	}
}

func TestPublishRabbitMQ(t *testing.T) {
	var err error

	expectedMessage := "teste001"

	exchangeName := "tests"
	exchangeType := "direct"
	queueName := exchangeName + "__Publish()"
	accessKey := queueName

	messageBrokerPublisher := rabbitmq.NewRabbitMQ().Connect().PopulatePublish(exchangeName, exchangeType, queueName, accessKey)

	err = messageBrokerPublisher.Publish("creting queue", "")
	if err != nil {
		t.Error("error publishing to queue: " + err.Error())
	}

	_, err = messageBrokerPublisher.Channel.Channel.QueuePurge(queueName, true)
	if err != nil {
		t.Error("error purging the queue: " + err.Error())
	}

	err = messageBrokerPublisher.Publish(expectedMessage, "")
	if err != nil {
		t.Error("error publishing to queue: " + err.Error())
	}

	recievedMessage, _, err := messageBrokerPublisher.Channel.Channel.Get(queueName, true)
	if err != nil {
		t.Error("error consuming message: " + err.Error())
	}

	if string(recievedMessage.Body) != expectedMessage {
		t.Error("error at with message body.\nexpected: " + expectedMessage + "\ngot:      " + string(recievedMessage.Body))
	}

	err = rabbitmq.DeleteQueueAndExchange(messageBrokerPublisher.Channel.Channel, queueName, exchangeName, "doit")
	if err != nil {
		t.Error("error deleting queue: " + err.Error())
	}

	messageBrokerPublisher.Channel.Channel.Close()
}

func TestConsumeForeverRabbitMQ(t *testing.T) {
	var messages []string
	messages = append(messages, "teste001", "teste002", "teste003")

	exchangeName := "tests"
	exchangeType := "direct"
	queueName := exchangeName + "__ConsumeForever()"
	accessKey := queueName
	qos := 0
	purgeBeforeStarting := true
	queueConsumeChannel := make(chan interface{})

	messageBrokerConsumer := rabbitmq.NewRabbitMQ().Connect().PopulateConsume(exchangeName, exchangeType, queueName, accessKey, qos, purgeBeforeStarting, queueConsumeChannel)

	go messageBrokerConsumer.ConsumeForever()

	time.Sleep(time.Second)

	for _, expectedMessage := range messages {
		confirmation, err := messageBrokerConsumer.Channel.Channel.PublishWithDeferredConfirmWithContext(context.Background(), exchangeName, accessKey, true, false, amqp.Publishing{Body: []byte(expectedMessage)})
		if err != nil {
			t.Error("error publishing message to RabbitMQ: " + err.Error())
		}

		success := confirmation.Wait()
		if !success {
			t.Error("Publishing confirmation failed on queue '" + queueName + "' with delivery TAG '" + strconv.FormatUint(confirmation.DeliveryTag, 10) + "'.")
		}

		recievedMessage := <-queueConsumeChannel

		transmissionData := string(recievedMessage.(*messagebroker.MessageBrokerConsumedMessage).TransmissionData.([]byte))

		if transmissionData != expectedMessage {
			t.Error("error at consume.\nexpected: " + expectedMessage + "\ngot:      " + transmissionData)
		}

		err = messageBrokerConsumer.Acknowledge(true, "success", recievedMessage.(*messagebroker.MessageBrokerConsumedMessage).MessageId, "")
		if err != nil {
			t.Error("error with acknowlege: " + err.Error())
		}
	}

	err := rabbitmq.DeleteQueueAndExchange(messageBrokerConsumer.Channel.Channel, queueName, exchangeName, "doit")
	if err != nil {
		t.Error("error deleting queue " + queueName + ": " + err.Error())
	}

	messageBrokerConsumer.Channel.Channel.Close()
}

func TestPersistDataRabbitMQ(t *testing.T) {
	expectedMessage := "teste"

	exchangeName := "tests"
	exchangeType := "direct"
	queueName := "tests"
	accessKey := queueName

	expectedQueueName := exchangeName + "__PersistData()"

	messageBrokerPublisher := rabbitmq.NewRabbitMQ().Connect().PopulatePublish(exchangeName, exchangeType, queueName, accessKey)

	err := messageBrokerPublisher.PersistData(expectedMessage, expectedQueueName, "")
	if err != nil {
		t.Error("error persisting data: " + err.Error())
	}

	delivery, _, err := messageBrokerPublisher.Channel.Channel.Get(expectedQueueName, true)
	if err != nil {
		t.Error("error consuming message: " + err.Error())
	}

	if string(delivery.Body) != expectedMessage {
		t.Error("error persisting data.\nexpected: " + expectedMessage + "\ngot:      " + string(delivery.Body))
	}

	err = rabbitmq.DeleteQueueAndExchange(messageBrokerPublisher.Channel.Channel, expectedQueueName, exchangeName, "doit")
	if err != nil {
		t.Error("error deleting queue and exchange: " + err.Error())
	}

	messageBrokerPublisher.Channel.Channel.Close()
}
