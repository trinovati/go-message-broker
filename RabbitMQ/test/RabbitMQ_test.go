package testing

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"reflect"
	"strconv"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	rabbitmq "github.com/trinovati/go-message-broker/v3/RabbitMQ"
	"github.com/trinovati/go-message-broker/v3/RabbitMQ/config"
	rabbitmqdto "github.com/trinovati/go-message-broker/v3/RabbitMQ/dto"
	"github.com/trinovati/go-message-broker/v3/constants"
	dto_pkg "github.com/trinovati/go-message-broker/v3/dto"
)

/*
UNSAFE!!!
FOR TEST PURPOSES ONLY!!!

Delete a queue and a exchange.
safePassword asserts that you're sure of it.
*/
func deleteQueueAndExchange(channel *amqp.Channel, queueName string, exchangeName string, safePassword string) (err error) {
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

func TestConnectionRabbitMQ(t *testing.T) {
	log.Printf("testing Connection for RabbitMQ\n\n")

	var env config.RABBITMQ_CONFIG = config.RABBITMQ_CONFIG{
		RABBITMQ_PROTOCOL: "amqp",
		RABBITMQ_HOST:     "localhost",
		RABBITMQ_PORT:     "5672",
		RABBITMQ_USERNAME: "guest",
		RABBITMQ_PASSWORD: "guest",
	}

	var queue rabbitmqdto.RabbitMQQueue = rabbitmqdto.RabbitMQQueue{
		Exchange:     "",
		ExchangeType: "",
		Name:         "",
		AccessKey:    "",
		Qos:          1,
		Purge:        true,
	}

	var consumer *rabbitmq.RabbitMQConsumer = rabbitmq.NewRabbitMQConsumer(
		env,
		"test",
		nil,
		queue,
	)

	var publisher *rabbitmq.RabbitMQPublisher = rabbitmq.NewRabbitMQPublisher(
		env,
		"test",
		queue,
	)

	consumer.Connect()
	publisher.Connect()

	if consumer.Channel().Connection().ConnectionCount != 1 {
		t.Fatalf("error at consumer connection count.\nexpected: %d\ngot:      %d", 1, consumer.Channel().Connection().ConnectionId)
	}
	if publisher.Channel().Connection().ConnectionCount != 1 {
		t.Fatalf("error at publisher connection count.\nexpected: %d\ngot:      %d", 1, publisher.Channel().Connection().ConnectionId)
	}

	if consumer.Channel().Connection().IsConnectionDown() {
		t.Fatalf("connection should be up")
	}
	if publisher.Channel().Connection().IsConnectionDown() {
		t.Fatalf("connection should be up")
	}

	consumer.Channel().Connection().Connection.Close()
	publisher.Channel().Connection().Connection.Close()

	time.Sleep(500 * time.Millisecond)

	consumer.Channel().Connection().WaitForConnection()
	publisher.Channel().Connection().WaitForConnection()

	if consumer.Channel().Connection().ConnectionCount != 2 {
		t.Fatalf("error at consumer connection count.\nexpected: %d\ngot:      %d", 2, consumer.Channel().Connection().ConnectionCount)
	}
	if publisher.Channel().Connection().ConnectionCount != 2 {
		t.Fatalf("error at publisher connection count.\nexpected: %d\ngot:      %d", 2, publisher.Channel().Connection().ConnectionCount)
	}

	time.Sleep(time.Second)
	consumer.CloseConnection()
	publisher.CloseConnection()
	time.Sleep(time.Second)

	if !consumer.Channel().Connection().IsConnectionDown() {
		t.Error("consumer connection should be down")
	}
	if !publisher.Channel().Connection().IsConnectionDown() {
		t.Error("publisher connection should be down")
	}

	if !consumer.Channel().IsChannelDown() {
		t.Error("consumer channel should be down")
	}
	if !publisher.Channel().IsChannelDown() {
		t.Error("publisher channel should be down")
	}

	log.Printf("finishing testing Connection for RabbitMQ\n\n")
}

func TestChannelRabbitMQ(t *testing.T) {
	log.Printf("testing Channel for RabbitMQ\n\n")

	var env config.RABBITMQ_CONFIG = config.RABBITMQ_CONFIG{
		RABBITMQ_PROTOCOL: "amqp",
		RABBITMQ_HOST:     "localhost",
		RABBITMQ_PORT:     "5672",
		RABBITMQ_USERNAME: "guest",
		RABBITMQ_PASSWORD: "guest",
	}

	var queue rabbitmqdto.RabbitMQQueue = rabbitmqdto.RabbitMQQueue{
		Exchange:     "",
		ExchangeType: "",
		Name:         "",
		AccessKey:    "",
		Qos:          1,
		Purge:        true,
	}

	var consumer *rabbitmq.RabbitMQConsumer = rabbitmq.NewRabbitMQConsumer(
		env,
		"test",
		nil,
		queue,
	)

	var publisher *rabbitmq.RabbitMQPublisher = rabbitmq.NewRabbitMQPublisher(
		env,
		"test",
		queue,
	)

	consumer.Connect()
	publisher.Connect()

	if consumer.Channel().ChannelCount != 1 {
		t.Fatalf("error at consumer channel.\nexpected: %d\ngot:      %d", 1, consumer.Channel().ChannelCount)
	}
	if publisher.Channel().ChannelCount != 1 {
		t.Fatalf("error at publisher channel.\nexpected: %d\ngot:      %d", 1, publisher.Channel().ChannelCount)
	}

	if consumer.Channel().IsChannelDown() {
		t.Fatalf("consumer channel should be up")
	}
	if publisher.Channel().IsChannelDown() {
		t.Fatalf("publisher channel should be up")
	}

	publisher.Channel().Channel.Close()
	consumer.Channel().Channel.Close()

	consumer.Channel().WaitForChannel()
	publisher.Channel().WaitForChannel()

	if consumer.Channel().ChannelCount != 2 {
		t.Fatalf("error at consumer channel count.\nexpected: %d\ngot:      %d", 2, consumer.Channel().ChannelCount)
	}
	if publisher.Channel().ChannelCount != 2 {
		t.Fatalf("error at publisher channel count.\nexpected: %d\ngot:      %d", 2, publisher.Channel().ChannelCount)
	}

	consumer.CloseChannel()
	publisher.CloseChannel()
	time.Sleep(time.Second)

	if !consumer.Channel().IsChannelDown() {
		t.Error("consumer channel should be down")
	}
	if !publisher.Channel().IsChannelDown() {
		t.Error("publisher channel should be down")
	}

	consumer.CloseConnection()
	publisher.CloseConnection()
	time.Sleep(time.Second)

	log.Printf("finishing testing Channel for RabbitMQ\n\n")
}

func TestShareConnectionRabbitMQ(t *testing.T) {
	log.Printf("testing ShareConnection for RabbitMQ\n\n")

	var env config.RABBITMQ_CONFIG = config.RABBITMQ_CONFIG{
		RABBITMQ_PROTOCOL: "amqp",
		RABBITMQ_HOST:     "localhost",
		RABBITMQ_PORT:     "5672",
		RABBITMQ_USERNAME: "guest",
		RABBITMQ_PASSWORD: "guest",
	}

	var queue rabbitmqdto.RabbitMQQueue = rabbitmqdto.RabbitMQQueue{
		Exchange:     "",
		ExchangeType: "",
		Name:         "",
		AccessKey:    "",
		Qos:          1,
		Purge:        true,
	}

	var consumer *rabbitmq.RabbitMQConsumer = rabbitmq.NewRabbitMQConsumer(
		env,
		"test",
		nil,
		queue,
	)

	var publisher *rabbitmq.RabbitMQPublisher = rabbitmq.NewRabbitMQPublisher(
		env,
		"test",
		queue,
	)

	publisher.ShareConnection(consumer)

	consumer.Connect()
	publisher.Connect()

	if consumer.Channel().Connection().IsConnectionDown() {
		t.Fatalf("connection should be up")
	}
	if publisher.Channel().Connection().IsConnectionDown() {
		t.Fatalf("connection should be up")
	}

	if reflect.DeepEqual(publisher.Channel().Connection(), consumer.Channel().Connection()) == false {
		t.Fatalf("error with shared connections\nexpected: %+v\ngot:      %+v", publisher.Channel().Connection(), consumer.Channel().Connection())
	}

	if reflect.DeepEqual(publisher.Channel(), consumer.Channel()) == true {
		t.Fatalf("should not share channels\n")
	}

	consumer.CloseConnection()
	time.Sleep(time.Second)

	if !consumer.Channel().Connection().IsConnectionDown() {
		t.Error("consumer connection should be down")
	}
	if !publisher.Channel().Connection().IsConnectionDown() {
		t.Error("publisher connection should be down")
	}

	if !consumer.Channel().IsChannelDown() {
		t.Error("consumer channel should be down")
	}
	if !publisher.Channel().IsChannelDown() {
		t.Error("publisher channel should be down")
	}

	log.Printf("finishing testing ShareConnection for RabbitMQ\n\n")
}

func TestShareChannelRabbitMQ(t *testing.T) {
	log.Printf("testing ShareChannel for RabbitMQ\n\n")

	var env config.RABBITMQ_CONFIG = config.RABBITMQ_CONFIG{
		RABBITMQ_PROTOCOL: "amqp",
		RABBITMQ_HOST:     "localhost",
		RABBITMQ_PORT:     "5672",
		RABBITMQ_USERNAME: "guest",
		RABBITMQ_PASSWORD: "guest",
	}

	var queue rabbitmqdto.RabbitMQQueue = rabbitmqdto.RabbitMQQueue{
		Exchange:     "",
		ExchangeType: "",
		Name:         "",
		AccessKey:    "",
		Qos:          1,
		Purge:        true,
	}

	var consumer *rabbitmq.RabbitMQConsumer = rabbitmq.NewRabbitMQConsumer(
		env,
		"test",
		nil,
		queue,
	)

	var publisher *rabbitmq.RabbitMQPublisher = rabbitmq.NewRabbitMQPublisher(
		env,
		"test",
		queue,
	)

	publisher.ShareChannel(consumer)
	consumer.Connect()

	if consumer.Channel().Connection().IsConnectionDown() {
		t.Fatalf("connection should be up")
	}
	if publisher.Channel().Connection().IsConnectionDown() {
		t.Fatalf("connection should be up")
	}

	if consumer.Channel().IsChannelDown() {
		t.Fatalf("channel should be up")
	}
	if publisher.Channel().IsChannelDown() {
		t.Fatalf("channel should be up")
	}

	if reflect.DeepEqual(publisher.Channel().Connection(), consumer.Channel().Connection()) == false {
		t.Fatalf("error with shared connections\nexpected: %+v\ngot:      %+v", publisher.Channel().Connection(), consumer.Channel().Connection())
	}

	if reflect.DeepEqual(publisher.Channel(), consumer.Channel()) == false {
		t.Fatalf("error with shared channels\nexpected: %+v\ngot:      %+v", publisher.Channel(), consumer.Channel())
	}

	consumer.CloseConnection()
	time.Sleep(time.Second)

	if !consumer.Channel().Connection().IsConnectionDown() {
		t.Fatalf("connection should be down")
	}
	if !publisher.Channel().Connection().IsConnectionDown() {
		t.Fatalf("connection should be down")
	}

	if !consumer.Channel().IsChannelDown() {
		t.Fatalf("channel should be down")
	}
	if !publisher.Channel().IsChannelDown() {
		t.Fatalf("channel should be down")
	}

	log.Printf("finishing testing ShareChannel for RabbitMQ\n\n")
}

func TestPublishRabbitMQ(t *testing.T) {
	log.Print("testing Publish for RabbitMQ\n\n")

	var env config.RABBITMQ_CONFIG = config.RABBITMQ_CONFIG{
		RABBITMQ_PROTOCOL: "amqp",
		RABBITMQ_HOST:     "localhost",
		RABBITMQ_PORT:     "5672",
		RABBITMQ_USERNAME: "guest",
		RABBITMQ_PASSWORD: "guest",
	}

	var queue rabbitmqdto.RabbitMQQueue = rabbitmqdto.RabbitMQQueue{
		Exchange:     "test",
		ExchangeType: "direct",
		Name:         "adapter_test",
		AccessKey:    "adapter_test",
	}

	var publisher *rabbitmq.RabbitMQPublisher = rabbitmq.NewRabbitMQPublisher(
		env,
		"test",
		queue,
	)

	publisher.Connect()

	publisher.PrepareQueue()

	_, err := publisher.Channel().Channel.QueuePurge(queue.Name, true)
	if err != nil {
		t.Fatalf("error purging the queue: %s", err.Error())
	}

	var expectedPublishing dto_pkg.BrokerPublishing = dto_pkg.BrokerPublishing{
		Header: map[string]any{
			"wololo": "walala",
			"type":   "test",
		},
		Body: []byte("payload"),
	}
	expectedHeader, err := json.Marshal(expectedPublishing.Header)
	if err != nil {
		t.Fatalf("failed to marshall expected header json: %s", err.Error())
	}

	err = publisher.Publish(expectedPublishing)
	if err != nil {
		t.Fatalf("error publishing to queue: %s", err.Error())
	}

	delivery, _, err := publisher.Channel().Channel.Get(queue.Name, true)
	if err != nil {
		t.Fatalf("error consuming message: %s", err.Error())
	}

	if string(expectedPublishing.Body) != string(delivery.Body) {
		t.Fatalf("error at body.\nexpected: %s\ngot:      %s", string(expectedPublishing.Body), string(delivery.Body))
	}

	deliveryHeader, err := json.Marshal(delivery.Headers)
	if err != nil {
		t.Fatalf("failed to marshall expected header json: %s", err.Error())
	}

	if reflect.DeepEqual(expectedHeader, deliveryHeader) == false {
		t.Fatalf("error at header.\nexpected: %v\ngot:      %v", expectedPublishing.Header, delivery.Headers)
	}

	err = deleteQueueAndExchange(publisher.Channel().Channel, queue.Name, queue.Exchange, "doit")
	if err != nil {
		t.Error("error deleting queue: " + err.Error())
	}

	publisher.CloseConnection()
	time.Sleep(time.Second)

	log.Printf("finishing testing Publish for RabbitMQ\n\n")
}

func TestConsumeForeverAndAcknowledgeRabbitMQ(t *testing.T) {
	ctx := context.Background()
	log.Printf("testing ConsumeForever and Acknowledge for RabbitMQ\n\n")

	var messages []string = []string{"test001", "test002", "test003"}
	var expectedHeader map[string]any = map[string]any{
		"wololo": "walala",
		"type":   "test",
	}

	var env config.RABBITMQ_CONFIG = config.RABBITMQ_CONFIG{
		RABBITMQ_PROTOCOL: "amqp",
		RABBITMQ_HOST:     "localhost",
		RABBITMQ_PORT:     "5672",
		RABBITMQ_USERNAME: "guest",
		RABBITMQ_PASSWORD: "guest",
	}

	var queue rabbitmqdto.RabbitMQQueue = rabbitmqdto.RabbitMQQueue{
		Exchange:     "test",
		ExchangeType: "direct",
		Name:         "adapter_test",
		AccessKey:    "adapter_test",
		Qos:          1,
		Purge:        true,
	}

	var consumer *rabbitmq.RabbitMQConsumer = rabbitmq.NewRabbitMQConsumer(
		env,
		"test",
		nil,
		queue,
	)

	consumer.Connect()

	consumer.PrepareQueue()

	deliveryChannel := consumer.Deliveries()

	go consumer.ConsumeForever(ctx)
	time.Sleep(time.Second)

	for i, expectedMessage := range messages {
		confirmation, err := consumer.Channel().Channel.PublishWithDeferredConfirmWithContext(
			context.Background(),
			queue.Exchange,
			queue.AccessKey,
			true,
			false,
			amqp.Publishing{
				Body:    []byte(expectedMessage),
				Headers: expectedHeader,
			},
		)
		if err != nil {
			t.Fatalf("error publishing message to RabbitMQ: %s", err.Error())
		}

		success := confirmation.Wait()
		if !success {
			t.Fatalf("publishing confirmation failed on queue %s with delivery TAG %d", queue.Name, confirmation.DeliveryTag)
		}

		delivery := <-deliveryChannel

		if expectedMessage != string(delivery.Body) {
			t.Fatalf("error at delivery.\nexpected: %s\ngot:      %s", expectedMessage, string(delivery.Body))
		}

		if delivery.Id != strconv.Itoa(i+1) {
			t.Fatalf("error at delivery id.\nexpected: %d\ngot:      %s", i+1, delivery.Id)
		}

		if reflect.DeepEqual(expectedHeader, delivery.Header) == false {
			t.Fatalf("error at header.\nexpected: %v\ngot:      %v", expectedHeader, delivery.Header)
		}

		err = consumer.Acknowledge(
			dto_pkg.BrokerAcknowledge{
				MessageId: delivery.Id,
				Action:    constants.ACKNOWLEDGE_SUCCESS,
			},
		)
		if err != nil {
			t.Fatalf("error with acknowledge: %s", err.Error())
		}
	}

	consumer.BreakConsume()

	err := deleteQueueAndExchange(consumer.Channel().Channel, queue.Name, queue.Exchange, "doit")
	if err != nil {
		t.Fatalf("error deleting queue: %s", err.Error())
	}

	consumer.CloseConnection()

	log.Printf("finishing testing ConsumeForever and Acknowledge for RabbitMQ\n\n")
}

func TestConsumeForeverAndAcknowledgeViaChannelRabbitMQ(t *testing.T) {
	ctx := context.Background()
	log.Printf("testing ConsumeForever and Acknowledge via channel for RabbitMQ\n\n")

	var messages []string = []string{"test001", "test002"}
	var expectedHeader map[string]any = map[string]any{
		"wololo": "walala",
		"type":   "test",
	}

	var env config.RABBITMQ_CONFIG = config.RABBITMQ_CONFIG{
		RABBITMQ_PROTOCOL: "amqp",
		RABBITMQ_HOST:     "localhost",
		RABBITMQ_PORT:     "5672",
		RABBITMQ_USERNAME: "guest",
		RABBITMQ_PASSWORD: "guest",
	}

	var queue rabbitmqdto.RabbitMQQueue = rabbitmqdto.RabbitMQQueue{
		Exchange:     "test",
		ExchangeType: "direct",
		Name:         "adapter_test",
		AccessKey:    "adapter_test",
		Qos:          1,
		Purge:        true,
	}

	var deadletter rabbitmqdto.RabbitMQQueue = rabbitmqdto.RabbitMQQueue{
		Exchange:     "test",
		ExchangeType: "direct",
		Name:         "deadletter_test",
		AccessKey:    "deadletter_test",
	}

	var publisher *rabbitmq.RabbitMQPublisher = rabbitmq.NewRabbitMQPublisher(
		env,
		"deadletter",
		deadletter,
	)
	var consumer *rabbitmq.RabbitMQConsumer = rabbitmq.NewRabbitMQConsumer(
		env,
		"test",
		publisher,
		queue,
	)

	consumer.Connect()

	consumer.PrepareQueue()

	deliveryChannel := consumer.Deliveries()

	go consumer.ConsumeForever(ctx)
	time.Sleep(time.Second)

	for _, expectedMessage := range messages {
		confirmation, err := consumer.Channel().Channel.PublishWithDeferredConfirmWithContext(
			context.Background(),
			queue.Exchange,
			queue.AccessKey,
			true,
			false,
			amqp.Publishing{
				Body:    []byte(expectedMessage),
				Headers: expectedHeader,
			},
		)
		if err != nil {
			t.Fatalf("error publishing message to RabbitMQ: %s", err.Error())
		}

		success := confirmation.Wait()
		if !success {
			t.Fatalf("publishing confirmation failed on queue %s with delivery TAG %d", queue.Name, confirmation.DeliveryTag)
		}
	}

	//IN CASE OF FAILURE, THE TEST WOULD BLOCK

	//testing success
	delivery := <-deliveryChannel

	if messages[0] != string(delivery.Body) {
		t.Fatalf("error at delivery.\nexpected: %s\ngot:      %s", messages[0], string(delivery.Body))
	}

	if delivery.Id != "1" {
		t.Fatalf("error at delivery id.\nexpected: %d\ngot:      %s", 1, delivery.Id)
	}

	if reflect.DeepEqual(expectedHeader, delivery.Header) == false {
		t.Fatalf("error at header.\nexpected: %v\ngot:      %v", expectedHeader, delivery.Header)
	}

	delivery.Acknowledger <- dto_pkg.BrokerAcknowledge{
		MessageId: delivery.Id,
		Action:    constants.ACKNOWLEDGE_SUCCESS,
	}
	//testing success

	//testing requeue
	delivery = <-deliveryChannel

	if messages[1] != string(delivery.Body) {
		t.Fatalf("error at delivery.\nexpected: %s\ngot:      %s", messages[1], string(delivery.Body))
	}

	if delivery.Id != "2" {
		t.Fatalf("error at delivery id.\nexpected: %d\ngot:      %s", 2, delivery.Id)
	}

	if reflect.DeepEqual(expectedHeader, delivery.Header) == false {
		t.Fatalf("error at header.\nexpected: %v\ngot:      %v", expectedHeader, delivery.Header)
	}

	delivery.Acknowledger <- dto_pkg.BrokerAcknowledge{
		MessageId: delivery.Id,
		Action:    constants.ACKNOWLEDGE_REQUEUE,
	}
	//testing requeue

	//testing deadletter
	delivery = <-deliveryChannel

	if messages[1] != string(delivery.Body) {
		t.Fatalf("error at delivery.\nexpected: %s\ngot:      %s", messages[1], string(delivery.Body))
	}

	if delivery.Id != "3" {
		t.Fatalf("error at delivery id.\nexpected: %d\ngot:      %s", 3, delivery.Id)
	}

	if reflect.DeepEqual(expectedHeader, delivery.Header) == false {
		t.Fatalf("error at header.\nexpected: %v\ngot:      %v", expectedHeader, delivery.Header)
	}

	delivery.Acknowledger <- dto_pkg.BrokerAcknowledge{
		MessageId: delivery.Id,
		Action:    constants.ACKNOWLEDGE_DEADLETTER,
		Report: dto_pkg.BrokerPublishing{
			Header: delivery.Header,
			Body:   delivery.Body,
		},
	}

	time.Sleep(time.Second)

	amqpDelivery, _, err := publisher.Channel().Channel.Get(deadletter.Name, true)
	if err != nil {
		t.Fatalf("error consuming message: %s", err.Error())
	}

	expectedDeadletterHeader, err := json.Marshal(expectedHeader)
	if err != nil {
		t.Fatalf("failed to marshall expected header json: %s", err.Error())
	}
	deliveryHeader, err := json.Marshal(amqpDelivery.Headers)
	if err != nil {
		t.Fatalf("failed to marshall expected header json: %s", err.Error())
	}

	if messages[1] != string(amqpDelivery.Body) {
		t.Fatalf("error at delivery.\nexpected: %s\ngot:      %s", messages[1], string(amqpDelivery.Body))
	}

	if reflect.DeepEqual(expectedDeadletterHeader, deliveryHeader) == false {
		t.Fatalf("error at header.\nexpected: %v\ngot:      %v", expectedDeadletterHeader, deliveryHeader)
	}
	//testing deadletter

	time.Sleep(time.Second)
	err = deleteQueueAndExchange(consumer.Channel().Channel, queue.Name, queue.Exchange, "doit")
	if err != nil {
		t.Fatalf("error deleting queue: %s", err.Error())
	}
	err = deleteQueueAndExchange(consumer.Channel().Channel, deadletter.Name, deadletter.Exchange, "doit")
	if err != nil {
		t.Fatalf("error deleting queue: %s", err.Error())
	}

	consumer.BreakConsume()
	consumer.CloseConnection()

	log.Printf("finishing testing ConsumeForever and Acknowledge via channel for RabbitMQ\n\n")
}

func TestAcknowledgeDeadletterMissingPublisherRabbitMQ(t *testing.T) {
	ctx := context.Background()
	log.Printf("testing Acknowledge deadletter with missing publisher RabbitMQ\n\n")

	var message string = "test001"
	var expectedHeader map[string]any = map[string]any{
		"wololo": "walala",
		"type":   "test",
	}

	var env config.RABBITMQ_CONFIG = config.RABBITMQ_CONFIG{
		RABBITMQ_PROTOCOL: "amqp",
		RABBITMQ_HOST:     "localhost",
		RABBITMQ_PORT:     "5672",
		RABBITMQ_USERNAME: "guest",
		RABBITMQ_PASSWORD: "guest",
	}

	var queue rabbitmqdto.RabbitMQQueue = rabbitmqdto.RabbitMQQueue{
		Exchange:     "test",
		ExchangeType: "direct",
		Name:         "adapter_test",
		AccessKey:    "adapter_test",
		Qos:          1,
		Purge:        true,
	}

	var consumer *rabbitmq.RabbitMQConsumer = rabbitmq.NewRabbitMQConsumer(
		env,
		"test",
		nil,
		queue,
	)

	consumer.Connect()

	consumer.PrepareQueue()

	deliveryChannel := consumer.Deliveries()

	go consumer.ConsumeForever(ctx)
	time.Sleep(time.Second)

	confirmation, err := consumer.Channel().Channel.PublishWithDeferredConfirmWithContext(
		context.Background(),
		queue.Exchange,
		queue.AccessKey,
		true,
		false,
		amqp.Publishing{
			Body:    []byte(message),
			Headers: expectedHeader,
		},
	)
	if err != nil {
		t.Fatalf("error publishing message to RabbitMQ: %s", err.Error())
	}

	success := confirmation.Wait()
	if !success {
		t.Fatalf("publishing confirmation failed on queue %s with delivery TAG %d", queue.Name, confirmation.DeliveryTag)
	}

	delivery := <-deliveryChannel

	if message != string(delivery.Body) {
		t.Fatalf("error at delivery.\nexpected: %s\ngot:      %s", message, string(delivery.Body))
	}

	if delivery.Id != "1" {
		t.Fatalf("error at delivery id.\nexpected: %d\ngot:      %s", 1, delivery.Id)
	}

	if reflect.DeepEqual(expectedHeader, delivery.Header) == false {
		t.Fatalf("error at header.\nexpected: %v\ngot:      %v", expectedHeader, delivery.Header)
	}

	consumer.Acknowledge(dto_pkg.BrokerAcknowledge{
		MessageId: delivery.Id,
		Action:    constants.ACKNOWLEDGE_DEADLETTER,
		Report: dto_pkg.BrokerPublishing{
			Header: delivery.Header,
			Body:   delivery.Body,
		},
	})

	err = deleteQueueAndExchange(consumer.Channel().Channel, queue.Name, queue.Exchange, "doit")
	if err != nil {
		t.Fatalf("error deleting queue: %s", err.Error())
	}

	consumer.BreakConsume()
	consumer.CloseConnection()

	log.Printf("finishing testing Acknowledge deadletter with missing publisher RabbitMQ\n\n")
}
