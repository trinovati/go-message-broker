package testing

import (
	"context"
	"log"
	"strconv"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	messagebroker "gitlab.com/aplicacao/trinovati-connector-message-brokers"
	rabbitmq "gitlab.com/aplicacao/trinovati-connector-message-brokers/RabbitMQ"
	"gitlab.com/aplicacao/trinovati-connector-message-brokers/RabbitMQ/config"
	"gitlab.com/aplicacao/trinovati-connector-message-brokers/RabbitMQ/dto"
)

func TestPersistDataRabbitMQ(t *testing.T) {
	log.Println("starting TestPersistDataRabbitMQ")

	expectedMessage := "teste"

	exchangeName := "tests"
	exchangeType := "direct"
	queueName := exchangeName + "__PersistData()"
	accessKey := queueName

	messageBroker := rabbitmq.NewRabbitMQ().
		AddBehaviour(rabbitmq.NewPublisher())

	messageBroker.Behaviour[0].CreateChannel()

	publishBehaviourDto := dto.NewBehaviourDto().FillPublisherData(exchangeName, exchangeType, queueName, accessKey)
	messageBroker.Behaviour[0].Populate(publishBehaviourDto)

	messageBroker.Behaviour[0].(*rabbitmq.Publisher).PreparePublishQueue()

	err := messageBroker.PersistData(expectedMessage, "", "")
	if err != nil {
		t.Error("error persisting data: " + err.Error())
	}

	delivery, _, err := messageBroker.Behaviour[0].(*rabbitmq.Publisher).Channel.Channel.Get(queueName, true)
	if err != nil {
		t.Error("error consuming message: " + err.Error())
	}

	{
		if string(delivery.Body) != expectedMessage {
			t.Error("error persisting data.\nexpected: " + expectedMessage + "\ngot:      " + string(delivery.Body))
		}
	}

	err = rabbitmq.DeleteQueueAndExchange(messageBroker.Behaviour[0].(*rabbitmq.Publisher).Channel.Channel, queueName, exchangeName, "doit")
	if err != nil {
		t.Error("error deleting queue and exchange: " + err.Error())
	}

	messageBroker.Behaviour[0].CloseConnection()

	log.Printf("finishing TestPersistDataRabbitMQ\n\n\n")
}

func TestConnectionDataRabbitMQ(t *testing.T) {
	log.Printf("starting TestConnectionDataRabbitMQ\n")

	messageBroker := rabbitmq.NewRabbitMQ().
		AddBehaviour(rabbitmq.NewPublisher()).
		AddBehaviour(rabbitmq.NewConsumer())

	messageBroker.Behaviour[0].(*rabbitmq.Publisher).Channel.Connection.Connect()
	messageBroker.Behaviour[1].(*rabbitmq.Consumer).Channel.Connection.Connect()

	{
		if messageBroker.Behaviour[0].(*rabbitmq.Publisher).Channel.Connection.ConnectionId != 1 {
			t.Error("error at publisher Connection id.\nexpected: " + strconv.Itoa(1) + "\ngot:      " + strconv.FormatUint(messageBroker.Behaviour[0].(*rabbitmq.Publisher).Channel.Connection.ConnectionId, 10))
		}

		if messageBroker.Behaviour[1].(*rabbitmq.Consumer).Channel.Connection.ConnectionId != 1 {
			t.Error("error at consumer Connection id.\nexpected: " + strconv.Itoa(1) + "\ngot:      " + strconv.FormatUint(messageBroker.Behaviour[1].(*rabbitmq.Consumer).Channel.Connection.ConnectionId, 10))
		}

		if messageBroker.Behaviour[0].(*rabbitmq.Publisher).Channel.Connection.IsConnectionDown() {
			t.Error("error at publisher Connection.IsConnectionDown, connection should be up")
		}

		if messageBroker.Behaviour[1].(*rabbitmq.Consumer).Channel.Connection.IsConnectionDown() {
			t.Error("error at consumer Connection.IsConnectionDown, connection should be up")
		}

		messageBroker.Behaviour[0].(*rabbitmq.Publisher).Channel.Connection.Connection.Close()
		time.Sleep(time.Millisecond * 50)
		if !messageBroker.Behaviour[0].(*rabbitmq.Publisher).Channel.Connection.IsConnectionDown() {
			t.Error("error at publisher IsConnectionDown, channel should be down")
		}

		messageBroker.Behaviour[0].(*rabbitmq.Publisher).Channel.Connection.WaitForConnection()
		if messageBroker.Behaviour[0].(*rabbitmq.Publisher).Channel.Connection.ConnectionId != 2 {
			t.Error("error at publisher connection id.\nexpected: " + strconv.Itoa(2) + "\ngot:      " + strconv.FormatUint(messageBroker.Behaviour[0].(*rabbitmq.Publisher).Channel.Connection.ConnectionId, 10))
		}

		messageBroker.Behaviour[1].(*rabbitmq.Consumer).Channel.Connection.Connection.Close()
		time.Sleep(time.Millisecond * 50)
		if !messageBroker.Behaviour[1].(*rabbitmq.Consumer).Channel.Connection.IsConnectionDown() {
			t.Error("error at consumer IsConnectionDown, channel should be down")
		}

		messageBroker.Behaviour[1].(*rabbitmq.Consumer).Channel.Connection.WaitForConnection()
		if messageBroker.Behaviour[1].(*rabbitmq.Consumer).Channel.Connection.ConnectionId != 2 {
			t.Error("error at consumer connection id.\nexpected: " + strconv.Itoa(2) + "\ngot:      " + strconv.FormatUint(messageBroker.Behaviour[1].(*rabbitmq.Consumer).Channel.Connection.ConnectionId, 10))
		}
	}

	messageBroker.Behaviour[0].(*rabbitmq.Publisher).Channel.Connection.CloseConnection()
	messageBroker.Behaviour[1].(*rabbitmq.Consumer).Channel.Connection.CloseConnection()

	{
		if !messageBroker.Behaviour[0].(*rabbitmq.Publisher).Channel.Connection.IsConnectionDown() {
			t.Error("error at IsConnectionDown, channel should be down")
		}

		if !messageBroker.Behaviour[1].(*rabbitmq.Consumer).Channel.Connection.IsConnectionDown() {
			t.Error("error at IsConnectionDown, channel should be down")
		}
	}

	messageBroker.Behaviour[0].(*rabbitmq.Publisher).CloseConnection()
	messageBroker.Behaviour[1].(*rabbitmq.Consumer).CloseConnection()

	log.Printf("finishing TestConnectionDataRabbitMQ\n\n\n")
}

func TestChannelDataRabbitMQ(t *testing.T) {
	log.Printf("starting TestChannelDataRabbitMQ\n")

	messageBroker := rabbitmq.NewRabbitMQ().
		AddBehaviour(rabbitmq.NewPublisher()).
		AddBehaviour(rabbitmq.NewConsumer())

	messageBroker.Behaviour[0].CreateChannel()
	messageBroker.Behaviour[1].CreateChannel()

	{
		if messageBroker.Behaviour[0].(*rabbitmq.Publisher).Channel.ChannelId != 1 {
			t.Error("error at Channel id.\nexpected: " + strconv.Itoa(1) + "\ngot:      " + strconv.FormatUint(messageBroker.Behaviour[0].(*rabbitmq.Publisher).Channel.ChannelId, 10))
		}

		if messageBroker.Behaviour[1].(*rabbitmq.Consumer).Channel.ChannelId != 1 {
			t.Error("error at BehaviourType.\nexpected: " + strconv.Itoa(1) + "\ngot:      " + strconv.FormatUint(messageBroker.Behaviour[1].(*rabbitmq.Consumer).Channel.ChannelId, 10))
		}

		if messageBroker.Behaviour[0].(*rabbitmq.Publisher).Channel.IsChannelDown() {
			t.Error("error at IsChannelDown, channel should be up")
		}

		if messageBroker.Behaviour[1].(*rabbitmq.Consumer).Channel.IsChannelDown() {
			t.Error("error at IsChannelDown, channel should be up")
		}

		messageBroker.Behaviour[0].(*rabbitmq.Publisher).Channel.Channel.Close()
		time.Sleep(time.Millisecond * 50)
		if !messageBroker.Behaviour[0].(*rabbitmq.Publisher).Channel.IsChannelDown() {
			t.Error("error at IsChannelDown, channel should be down")
		}

		messageBroker.Behaviour[0].(*rabbitmq.Publisher).Channel.WaitForChannel()
		if messageBroker.Behaviour[0].(*rabbitmq.Publisher).Channel.ChannelId != 2 {
			t.Error("error at Channel id.\nexpected: " + strconv.Itoa(2) + "\ngot:      " + strconv.FormatUint(messageBroker.Behaviour[0].(*rabbitmq.Publisher).Channel.ChannelId, 10))
		}

		messageBroker.Behaviour[1].(*rabbitmq.Consumer).Channel.Channel.Close()
		time.Sleep(time.Millisecond * 50)
		if !messageBroker.Behaviour[1].(*rabbitmq.Consumer).Channel.IsChannelDown() {
			t.Error("error at IsChannelDown, channel should be down")
		}

		messageBroker.Behaviour[1].(*rabbitmq.Consumer).Channel.WaitForChannel()
		if messageBroker.Behaviour[1].(*rabbitmq.Consumer).Channel.ChannelId != 2 {
			t.Error("error at IsChannelDown.\nexpected: " + strconv.Itoa(2) + "\ngot:      " + strconv.FormatUint(messageBroker.Behaviour[1].(*rabbitmq.Consumer).Channel.ChannelId, 10))
		}
	}

	messageBroker.Behaviour[0].(*rabbitmq.Publisher).Channel.CloseChannel()
	messageBroker.Behaviour[1].(*rabbitmq.Consumer).Channel.CloseChannel()

	{
		if !messageBroker.Behaviour[0].(*rabbitmq.Publisher).Channel.IsChannelDown() {
			t.Error("error at IsChannelDown, channel should be down")
		}

		if !messageBroker.Behaviour[1].(*rabbitmq.Consumer).Channel.IsChannelDown() {
			t.Error("error at IsChannelDown, channel should be down")
		}
	}

	messageBroker.Behaviour[0].(*rabbitmq.Publisher).CloseConnection()
	messageBroker.Behaviour[1].(*rabbitmq.Consumer).CloseConnection()
	messageBroker.Behaviour[1].(*rabbitmq.Consumer).FailedMessagePublisher.CloseConnection()

	log.Printf("starting TestChannelDataRabbitMQ\n\n\n")
}

func TestPopulateRabbitMQ(t *testing.T) {
	log.Printf("starting TestPopulateRabbitMQ\n")
	publisherExchangeName := "publisher_exchange"
	publisherExchangeType := "publisher_type"
	publisherQueueName := "publisher_queue_name"
	publisherAccessKey := "publisher_access_key"

	consumerExchangeName := "consumer_exchange"
	consumerExchangeType := "consumer_type"
	consumerQueueName := "consumer_queue_name"
	consumerAccessKey := "consumer_access_key"
	qos := 2
	purgeBeforeStarting := true
	outgoingDeliveryChannel := make(chan interface{})
	unacknowledgedDeliveryMap := &sync.Map{}

	expectedFailedMessagesPublisherExchangeName := "failed"
	expectedFailedMessagesPublisherExchangetype := "direct"
	expectedFailedMessagesPublisherQueueName := "_" + consumerExchangeName + "__failed_messages"
	expectedFailedMessagesPublisherAccessKey := expectedFailedMessagesPublisherQueueName

	publisherBehaviourDto := dto.NewBehaviourDto().FillPublisherData(publisherExchangeName, publisherExchangeType, publisherQueueName, publisherAccessKey)
	consumerBehaviourDto := dto.NewBehaviourDto().FillConsumerData(consumerExchangeName, consumerExchangeType, consumerQueueName, consumerAccessKey, qos, purgeBeforeStarting, outgoingDeliveryChannel, unacknowledgedDeliveryMap)

	messageBroker := rabbitmq.NewRabbitMQ().
		AddBehaviour(rabbitmq.NewPublisher()).
		AddBehaviour(rabbitmq.NewConsumer())

	messageBroker.Behaviour[0].Populate(publisherBehaviourDto)
	messageBroker.Behaviour[1].Populate(consumerBehaviourDto)

	{
		if messageBroker.Behaviour[0].(*rabbitmq.Publisher).BehaviourType != config.RABBITMQ_PUBLISHER_BEHAVIOUR {
			t.Error("error at BehaviourType.\nexpected: " + config.RABBITMQ_PUBLISHER_BEHAVIOUR + "\ngot:      " + messageBroker.Behaviour[0].(*rabbitmq.Publisher).BehaviourType)
		}

		if messageBroker.Behaviour[0].(*rabbitmq.Publisher).ExchangeName != publisherExchangeName {
			t.Error("error at ExchangeName.\nexpected: " + publisherExchangeName + "\ngot:      " + messageBroker.Behaviour[0].(*rabbitmq.Publisher).ExchangeName)
		}

		if messageBroker.Behaviour[0].(*rabbitmq.Publisher).ExchangeType != publisherExchangeType {
			t.Error("error at ExchangeType.\nexpected: " + publisherExchangeType + "\ngot:      " + messageBroker.Behaviour[0].(*rabbitmq.Publisher).ExchangeType)
		}

		if messageBroker.Behaviour[0].(*rabbitmq.Publisher).QueueName != publisherQueueName {
			t.Error("error at QueueName.\nexpected: " + publisherQueueName + "\ngot:      " + messageBroker.Behaviour[0].(*rabbitmq.Publisher).QueueName)
		}

		if messageBroker.Behaviour[0].(*rabbitmq.Publisher).AccessKey != publisherAccessKey {
			t.Error("error at AccessKey.\nexpected: " + publisherAccessKey + "\ngot:      " + messageBroker.Behaviour[0].(*rabbitmq.Publisher).AccessKey)
		}

		if messageBroker.Behaviour[1].(*rabbitmq.Consumer).BehaviourType != config.RABBITMQ_CONSUMER_BEHAVIOUR {
			t.Error("error at BehaviourType.\nexpected: " + config.RABBITMQ_CONSUMER_BEHAVIOUR + "\ngot:      " + messageBroker.Behaviour[1].(*rabbitmq.Consumer).BehaviourType)
		}

		if messageBroker.Behaviour[1].(*rabbitmq.Consumer).ExchangeName != consumerExchangeName {
			t.Error("error at ExchangeName.\nexpected: " + consumerExchangeName + "\ngot:      " + messageBroker.Behaviour[1].(*rabbitmq.Publisher).ExchangeName)
		}

		if messageBroker.Behaviour[1].(*rabbitmq.Consumer).ExchangeType != consumerExchangeType {
			t.Error("error at ExchangeType.\nexpected: " + consumerExchangeType + "\ngot:      " + messageBroker.Behaviour[1].(*rabbitmq.Consumer).ExchangeType)
		}

		if messageBroker.Behaviour[1].(*rabbitmq.Consumer).QueueName != consumerQueueName {
			t.Error("error at QueueName.\nexpected: " + consumerQueueName + "\ngot:      " + messageBroker.Behaviour[1].(*rabbitmq.Consumer).QueueName)
		}

		if messageBroker.Behaviour[1].(*rabbitmq.Consumer).AccessKey != consumerAccessKey {
			t.Error("error at AccessKey.\nexpected: " + consumerAccessKey + "\ngot:      " + messageBroker.Behaviour[1].(*rabbitmq.Consumer).AccessKey)
		}

		if messageBroker.Behaviour[1].(*rabbitmq.Consumer).Qos != qos {
			t.Error("error at Qos.\nexpected: " + strconv.Itoa(qos) + "\ngot:      " + strconv.Itoa(messageBroker.Behaviour[1].(*rabbitmq.Consumer).Qos))
		}

		if messageBroker.Behaviour[1].(*rabbitmq.Consumer).PurgeBeforeStarting != purgeBeforeStarting {
			t.Error("error at PurgeBeforeStarting.\nexpected: " + strconv.FormatBool(purgeBeforeStarting) + "\ngot:      " + strconv.FormatBool(messageBroker.Behaviour[1].(*rabbitmq.Consumer).PurgeBeforeStarting))
		}

		if messageBroker.Behaviour[1].(*rabbitmq.Consumer).OutgoingDeliveryChannel != outgoingDeliveryChannel {
			t.Error("error at OutgoingDeliveryChannel. Unexpected pointer.")
		}

		if messageBroker.Behaviour[1].(*rabbitmq.Consumer).UnacknowledgedDeliveryMap != unacknowledgedDeliveryMap {
			t.Error("error at UnacknowledgedDeliveryMap. unexpected pointer")
		}

		if messageBroker.Behaviour[1].(*rabbitmq.Consumer).FailedMessagePublisher.ExchangeName != expectedFailedMessagesPublisherExchangeName {
			t.Error("error at AccessKey.\nexpected: " + expectedFailedMessagesPublisherExchangeName + "\ngot:      " + messageBroker.Behaviour[1].(*rabbitmq.Consumer).FailedMessagePublisher.ExchangeName)
		}

		if messageBroker.Behaviour[1].(*rabbitmq.Consumer).FailedMessagePublisher.ExchangeType != expectedFailedMessagesPublisherExchangetype {
			t.Error("error at AccessKey.\nexpected: " + expectedFailedMessagesPublisherExchangetype + "\ngot:      " + messageBroker.Behaviour[1].(*rabbitmq.Consumer).FailedMessagePublisher.ExchangeType)
		}

		if messageBroker.Behaviour[1].(*rabbitmq.Consumer).FailedMessagePublisher.QueueName != expectedFailedMessagesPublisherQueueName {
			t.Error("error at AccessKey.\nexpected: " + expectedFailedMessagesPublisherQueueName + "\ngot:      " + messageBroker.Behaviour[1].(*rabbitmq.Consumer).FailedMessagePublisher.QueueName)
		}

		if messageBroker.Behaviour[1].(*rabbitmq.Consumer).FailedMessagePublisher.AccessKey != expectedFailedMessagesPublisherAccessKey {
			t.Error("error at AccessKey.\nexpected: " + expectedFailedMessagesPublisherAccessKey + "\ngot:      " + messageBroker.Behaviour[1].(*rabbitmq.Consumer).FailedMessagePublisher.AccessKey)
		}
	}

	log.Printf("finished TestPopulateRabbitMQ\n\n\n")
}

func TestGetPopulatedDataFromRabbitMQ(t *testing.T) {
	log.Printf("starting TestGetPopulatedDataFromRabbitMQ\n")

	publisherExchangeName := "publisher_exchange"
	publisherExchangeType := "publisher_type"
	publisherQueueName := "publisher_queue"
	publisherAccessKey := "publisher_access"

	consumerExchangeName := "consumer_exchange"
	consumerExchangeType := "consumer_type"
	consumerQueueName := "consumer_queue"
	consumerAccessKey := "consumer_access"
	qos := 2
	purgeBeforeStarting := true
	outgoingDeliveryChannel := make(chan interface{})
	unacknowledgedDeliveryMap := &sync.Map{}

	expectedFailedMessagesPublisherExchangeName := "failed"
	expectedFailedMessagesPublisherExchangetype := "direct"
	expectedFailedMessagesPublisherQueueName := "_" + consumerExchangeName + "__failed_messages"
	expectedFailedMessagesPublisherAccessKey := expectedFailedMessagesPublisherQueueName

	publisherBehaviourDto := dto.NewBehaviourDto().FillPublisherData(publisherExchangeName, publisherExchangeType, publisherQueueName, publisherAccessKey)
	consumerBehaviourDto := dto.NewBehaviourDto().FillConsumerData(consumerExchangeName, consumerExchangeType, consumerQueueName, consumerAccessKey, qos, purgeBeforeStarting, outgoingDeliveryChannel, unacknowledgedDeliveryMap)

	messageBroker := rabbitmq.NewRabbitMQ().
		AddBehaviour(rabbitmq.NewPublisher()).
		AddBehaviour(rabbitmq.NewConsumer()).
		AddBehaviour(rabbitmq.NewPublisher()).
		AddBehaviour(rabbitmq.NewConsumer())

	messageBroker.Behaviour[0].Populate(publisherBehaviourDto)
	messageBroker.Behaviour[1].Populate(consumerBehaviourDto)

	messageBroker.Behaviour[2].GetPopulatedDataFrom(messageBroker.Behaviour[0])
	messageBroker.Behaviour[3].GetPopulatedDataFrom(messageBroker.Behaviour[1])

	{
		if messageBroker.Behaviour[2].(*rabbitmq.Publisher).BehaviourType != config.RABBITMQ_PUBLISHER_BEHAVIOUR {
			t.Error("error at BehaviourType.\nexpected: " + config.RABBITMQ_PUBLISHER_BEHAVIOUR + "\ngot:      " + messageBroker.Behaviour[2].(*rabbitmq.Publisher).BehaviourType)
		}

		if messageBroker.Behaviour[2].(*rabbitmq.Publisher).ExchangeName != publisherExchangeName {
			t.Error("error at ExchangeName.\nexpected: " + publisherExchangeName + "\ngot:      " + messageBroker.Behaviour[2].(*rabbitmq.Publisher).ExchangeName)
		}

		if messageBroker.Behaviour[2].(*rabbitmq.Publisher).ExchangeType != publisherExchangeType {
			t.Error("error at ExchangeType.\nexpected: " + publisherExchangeType + "\ngot:      " + messageBroker.Behaviour[2].(*rabbitmq.Publisher).ExchangeType)
		}

		if messageBroker.Behaviour[2].(*rabbitmq.Publisher).QueueName != publisherQueueName {
			t.Error("error at QueueName.\nexpected: " + publisherQueueName + "\ngot:      " + messageBroker.Behaviour[2].(*rabbitmq.Publisher).QueueName)
		}

		if messageBroker.Behaviour[2].(*rabbitmq.Publisher).AccessKey != publisherAccessKey {
			t.Error("error at AccessKey.\nexpected: " + publisherAccessKey + "\ngot:      " + messageBroker.Behaviour[2].(*rabbitmq.Publisher).AccessKey)
		}

		if messageBroker.Behaviour[3].(*rabbitmq.Consumer).BehaviourType != config.RABBITMQ_CONSUMER_BEHAVIOUR {
			t.Error("error at BehaviourType.\nexpected: " + config.RABBITMQ_CONSUMER_BEHAVIOUR + "\ngot:      " + messageBroker.Behaviour[3].(*rabbitmq.Consumer).BehaviourType)
		}

		if messageBroker.Behaviour[3].(*rabbitmq.Consumer).ExchangeName != consumerExchangeName {
			t.Error("error at ExchangeName.\nexpected: " + consumerExchangeName + "\ngot:      " + messageBroker.Behaviour[3].(*rabbitmq.Consumer).ExchangeName)
		}

		if messageBroker.Behaviour[3].(*rabbitmq.Consumer).ExchangeType != consumerExchangeType {
			t.Error("error at ExchangeType.\nexpected: " + consumerExchangeType + "\ngot:      " + messageBroker.Behaviour[3].(*rabbitmq.Consumer).ExchangeType)
		}

		if messageBroker.Behaviour[3].(*rabbitmq.Consumer).QueueName != consumerQueueName {
			t.Error("error at QueueName.\nexpected: " + consumerQueueName + "\ngot:      " + messageBroker.Behaviour[3].(*rabbitmq.Consumer).QueueName)
		}

		if messageBroker.Behaviour[3].(*rabbitmq.Consumer).AccessKey != consumerAccessKey {
			t.Error("error at AccessKey.\nexpected: " + consumerAccessKey + "\ngot:      " + messageBroker.Behaviour[3].(*rabbitmq.Consumer).AccessKey)
		}

		if messageBroker.Behaviour[3].(*rabbitmq.Consumer).Qos != qos {
			t.Error("error at Qos.\nexpected: " + strconv.Itoa(qos) + "\ngot:      " + strconv.Itoa(messageBroker.Behaviour[3].(*rabbitmq.Consumer).Qos))
		}

		if messageBroker.Behaviour[3].(*rabbitmq.Consumer).PurgeBeforeStarting != purgeBeforeStarting {
			t.Error("error at PurgeBeforeStarting.\nexpected: " + strconv.FormatBool(purgeBeforeStarting) + "\ngot:      " + strconv.FormatBool(messageBroker.Behaviour[3].(*rabbitmq.Consumer).PurgeBeforeStarting))
		}

		if messageBroker.Behaviour[3].(*rabbitmq.Consumer).OutgoingDeliveryChannel != outgoingDeliveryChannel {
			t.Error("error at OutgoingDeliveryChannel. Unexpected pointer.")
		}

		if messageBroker.Behaviour[3].(*rabbitmq.Consumer).UnacknowledgedDeliveryMap != unacknowledgedDeliveryMap {
			t.Error("error at UnacknowledgedDeliveryMap. Unexpected pointer")
		}

		if messageBroker.Behaviour[3].(*rabbitmq.Consumer).FailedMessagePublisher.ExchangeName != expectedFailedMessagesPublisherExchangeName {
			t.Error("error at ExchangeName.\nexpected: " + expectedFailedMessagesPublisherExchangeName + "\ngot:      " + messageBroker.Behaviour[3].(*rabbitmq.Consumer).FailedMessagePublisher.ExchangeName)
		}

		if messageBroker.Behaviour[3].(*rabbitmq.Consumer).FailedMessagePublisher.ExchangeType != expectedFailedMessagesPublisherExchangetype {
			t.Error("error at ExchangeType.\nexpected: " + expectedFailedMessagesPublisherExchangetype + "\ngot:      " + messageBroker.Behaviour[3].(*rabbitmq.Consumer).FailedMessagePublisher.ExchangeType)
		}

		if messageBroker.Behaviour[3].(*rabbitmq.Consumer).FailedMessagePublisher.QueueName != expectedFailedMessagesPublisherQueueName {
			t.Error("error at QueueName.\nexpected: " + expectedFailedMessagesPublisherQueueName + "\ngot:      " + messageBroker.Behaviour[3].(*rabbitmq.Consumer).FailedMessagePublisher.QueueName)
		}

		if messageBroker.Behaviour[3].(*rabbitmq.Consumer).FailedMessagePublisher.AccessKey != expectedFailedMessagesPublisherAccessKey {
			t.Error("error at AccessKey.\nexpected: " + expectedFailedMessagesPublisherAccessKey + "\ngot:      " + messageBroker.Behaviour[3].(*rabbitmq.Consumer).FailedMessagePublisher.AccessKey)
		}
	}

	log.Printf("finished TestGetPopulatedDataFromRabbitMQ\n\n\n")
}

func TestSharesConnectionWithRabbitMQ(t *testing.T) {
	log.Printf("starting TestSharesConnectionWithRabbitMQ\n")

	messageBroker := rabbitmq.NewRabbitMQ().
		AddBehaviour(rabbitmq.NewPublisher()).
		AddBehaviour(rabbitmq.NewConsumer()).
		AddBehaviour(rabbitmq.NewPublisher()).
		AddBehaviour(rabbitmq.NewConsumer())

	messageBroker.Behaviour[2].SharesConnectionWith(messageBroker.Behaviour[0])
	messageBroker.Behaviour[3].SharesConnectionWith(messageBroker.Behaviour[1])

	{
		if messageBroker.Behaviour[2].(*rabbitmq.Publisher).Channel == messageBroker.Behaviour[0].(*rabbitmq.Publisher).Channel {
			t.Error("error at publisher Channel, should be different pointers")
		}

		if messageBroker.Behaviour[3].(*rabbitmq.Consumer).Channel == messageBroker.Behaviour[1].(*rabbitmq.Consumer).Channel {
			t.Error("error at consumer Channel, should be different pointers")
		}

		if messageBroker.Behaviour[2].(*rabbitmq.Publisher).Channel.Connection != messageBroker.Behaviour[0].(*rabbitmq.Publisher).Channel.Connection {
			t.Error("error at publisher Connection, should be same pointers")
		}

		if messageBroker.Behaviour[3].(*rabbitmq.Consumer).Channel.Connection != messageBroker.Behaviour[1].(*rabbitmq.Consumer).Channel.Connection {
			t.Error("error at consumer ChanConnectionnel, should be same pointers")
		}
	}

	log.Printf("finished TestSharesConnectionWithRabbitMQ\n\n\n")
}

func TestSharesChannelRabbitMQ(t *testing.T) {
	log.Printf("starting TestSharesChannelRabbitMQ\n")

	messageBroker := rabbitmq.NewRabbitMQ().
		AddBehaviour(rabbitmq.NewPublisher()).
		AddBehaviour(rabbitmq.NewConsumer()).
		AddBehaviour(rabbitmq.NewPublisher()).
		AddBehaviour(rabbitmq.NewConsumer())

	messageBroker.Behaviour[2].SharesChannelWith(messageBroker.Behaviour[0])
	messageBroker.Behaviour[3].SharesChannelWith(messageBroker.Behaviour[1])

	{
		if messageBroker.Behaviour[2].(*rabbitmq.Publisher).Channel != messageBroker.Behaviour[0].(*rabbitmq.Publisher).Channel {
			t.Error("error at publisher Channel, should be same pointers")
		}

		if messageBroker.Behaviour[3].(*rabbitmq.Consumer).Channel != messageBroker.Behaviour[1].(*rabbitmq.Consumer).Channel {
			t.Error("error at consumer Channel, should be same pointers")
		}

		if messageBroker.Behaviour[2].(*rabbitmq.Publisher).Channel.Connection != messageBroker.Behaviour[0].(*rabbitmq.Publisher).Channel.Connection {
			t.Error("error at publisher Connection, should be same pointers")
		}

		if messageBroker.Behaviour[3].(*rabbitmq.Consumer).Channel.Connection != messageBroker.Behaviour[1].(*rabbitmq.Consumer).Channel.Connection {
			t.Error("error at consumer ChanConnectionnel, should be same pointers")
		}
	}

	messageBroker.Behaviour[1].(*rabbitmq.Consumer).FailedMessagePublisher.CloseConnection()
	messageBroker.Behaviour[1].(*rabbitmq.Consumer).FailedMessagePublisher.CloseChannel()

	log.Printf("finishing TestSharesChannelRabbitMQ\n\n\n")
}

func TestPublishRabbitMQ(t *testing.T) {
	log.Println("starting TestPublishRabbitMQ")

	var err error

	expectedMessage := "teste001"

	exchangeName := "tests"
	exchangeType := "direct"
	queueName := exchangeName + "__Publish()"
	accessKey := queueName

	messageBroker := rabbitmq.NewRabbitMQ().
		AddBehaviour(rabbitmq.NewPublisher())

	messageBroker.Behaviour[0].CreateChannel()

	publishBehaviourDto := dto.NewBehaviourDto().FillPublisherData(exchangeName, exchangeType, queueName, accessKey)
	messageBroker.Behaviour[0].Populate(publishBehaviourDto)

	messageBroker.Behaviour[0].(*rabbitmq.Publisher).PreparePublishQueue()

	_, err = messageBroker.Behaviour[0].(*rabbitmq.Publisher).Channel.Channel.QueuePurge(queueName, true)
	if err != nil {
		t.Error("error purging the queue: " + err.Error())
	}

	err = messageBroker.Behaviour[0].(*rabbitmq.Publisher).Publish(expectedMessage, "", "")
	if err != nil {
		t.Error("error publishing to queue: " + err.Error())
	}

	recievedMessage, _, err := messageBroker.Behaviour[0].(*rabbitmq.Publisher).Channel.Channel.Get(queueName, true)
	if err != nil {
		t.Error("error consuming message: " + err.Error())
	}

	{
		if string(recievedMessage.Body) != expectedMessage {
			t.Error("error at with message body.\nexpected: " + expectedMessage + "\ngot:      " + string(recievedMessage.Body))
		}
	}

	err = rabbitmq.DeleteQueueAndExchange(messageBroker.Behaviour[0].(*rabbitmq.Publisher).Channel.Channel, queueName, exchangeName, "doit")
	if err != nil {
		t.Error("error deleting queue: " + err.Error())
	}

	messageBroker.Behaviour[0].CloseConnection()

	log.Printf("finishing TestPublishRabbitMQ\n\n\n")
}

func TestConsumeForeverRabbitMQ(t *testing.T) {
	log.Println("starting TestConsumeForeverRabbitMQ")

	var messages []string
	messages = append(messages, "teste001", "teste002", "teste003")

	exchangeName := "tests"
	exchangeType := "direct"
	queueName := exchangeName + "__ConsumeForever()"
	accessKey := queueName
	qos := 0
	purgeBeforeStarting := true
	queueConsumeChannel := make(chan interface{})
	unacknowledgedDeliveryMap := &sync.Map{}

	messageBroker := rabbitmq.NewRabbitMQ().
		AddBehaviour(rabbitmq.NewConsumer())

	messageBroker.Behaviour[0].CreateChannel()

	behaviourDto := dto.NewBehaviourDto().FillConsumerData(exchangeName, exchangeType, queueName, accessKey, qos, purgeBeforeStarting, queueConsumeChannel, unacknowledgedDeliveryMap)
	messageBroker.Behaviour[0].Populate(behaviourDto)

	go messageBroker.ConsumeForever()
	time.Sleep(time.Second)

	for _, expectedMessage := range messages {
		confirmation, err := messageBroker.Behaviour[0].(*rabbitmq.Consumer).Channel.Channel.PublishWithDeferredConfirmWithContext(context.Background(), exchangeName, accessKey, true, false, amqp.Publishing{Body: []byte(expectedMessage)})
		if err != nil {
			t.Error("error publishing message to RabbitMQ: " + err.Error())
		}

		success := confirmation.Wait()
		if !success {
			t.Error("Publishing confirmation failed on queue '" + queueName + "' with delivery TAG '" + strconv.FormatUint(confirmation.DeliveryTag, 10) + "'.")
		}

		recievedMessage := <-queueConsumeChannel
		transmissionData := string(recievedMessage.(*messagebroker.MessageBrokerConsumedMessage).MessageData.([]byte))

		{
			if transmissionData != expectedMessage {
				t.Error("error at consume.\nexpected: " + expectedMessage + "\ngot:      " + transmissionData)
			}
		}

		err = messageBroker.Acknowledge(true, "success", recievedMessage.(*messagebroker.MessageBrokerConsumedMessage).MessageId, "")
		if err != nil {
			t.Error("error with acknowlege: " + err.Error())
		}
	}

	err := rabbitmq.DeleteQueueAndExchange(messageBroker.Behaviour[0].(*rabbitmq.Consumer).Channel.Channel, queueName, exchangeName, "doit")
	if err != nil {
		t.Error("error deleting queue " + queueName + ": " + err.Error())
	}

	err = rabbitmq.DeleteQueueAndExchange(messageBroker.Behaviour[0].(*rabbitmq.Consumer).Channel.Channel, queueName, exchangeName, "doit")
	if err != nil {
		t.Error("error deleting queue " + queueName + ": " + err.Error())
	}

	messageBroker.Behaviour[0].CloseConnection()

	log.Printf("finishing TestConsumeForeverRabbitMQ\n\n\n")
}
