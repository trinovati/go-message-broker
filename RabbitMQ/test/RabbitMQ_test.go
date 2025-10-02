package testing

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"testing"
	"time"

	rabbitmq "github.com/trinovati/go-message-broker/v3/RabbitMQ"
	"github.com/trinovati/go-message-broker/v3/RabbitMQ/config"
	dto_rabbitmq "github.com/trinovati/go-message-broker/v3/RabbitMQ/dto"
	constant_broker "github.com/trinovati/go-message-broker/v3/pkg/constant"
	dto_broker "github.com/trinovati/go-message-broker/v3/pkg/dto"

	amqp "github.com/rabbitmq/amqp091-go"
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
			return fmt.Errorf("can't delete queue %s: %w", queueName, err)
		}

		err = channel.ExchangeDelete(exchangeName, false, false)
		if err != nil {
			return fmt.Errorf("can't delete exchange %s: %w", exchangeName, err)
		}

	} else {
		return fmt.Errorf("can't delete: you seem not sure of it")
	}

	return nil
}

func TestConnectionRabbitMQ(t *testing.T) {
	t.Logf("testing Connection for RabbitMQ\n\n")
	ctx := context.Background()

	var env config.RABBITMQ_CONFIG = config.RABBITMQ_CONFIG{
		PROTOCOL: "amqp",
		HOST:     "localhost",
		PORT:     "5672",
		USERNAME: "guest",
		PASSWORD: "guest",
	}

	var queue dto_rabbitmq.RabbitMQQueue = dto_rabbitmq.RabbitMQQueue{
		Exchange:     "",
		ExchangeType: "",
		Name:         "",
		AccessKey:    "",
		Qos:          1,
		Purge:        true,
	}

	var consumer *rabbitmq.RabbitMQConsumer = rabbitmq.NewRabbitMQConsumer(
		ctx,
		env,
		"test",
		nil,
		queue,
		0,
		0,
		nil,
	)

	var publisher *rabbitmq.RabbitMQPublisher = rabbitmq.NewRabbitMQPublisher(
		ctx,
		env,
		"test",
		queue,
		nil,
	)

	consumer.Connect(ctx)
	publisher.Connect(ctx)

	if consumer.Channel().Connection().TimesConnected != 1 {
		t.Fatalf("error at consumer connection count.\nexpected: %d\ngot:      %d", 1, consumer.Channel().Connection().Id)
	}
	if publisher.Channel().Connection().TimesConnected != 1 {
		t.Fatalf("error at publisher connection count.\nexpected: %d\ngot:      %d", 1, publisher.Channel().Connection().Id)
	}

	if !consumer.Channel().Connection().IsConnected(true) {
		t.Fatalf("connection should be up")
	}
	if !publisher.Channel().Connection().IsConnected(true) {
		t.Fatalf("connection should be up")
	}

	consumer.Channel().Connection().Connection.Close()
	publisher.Channel().Connection().Connection.Close()

	err := consumer.Channel().Connection().WaitForConnection(ctx, true)
	if err != nil {
		t.Fatalf("%s", err)
	}
	err = publisher.Channel().Connection().WaitForConnection(ctx, true)
	if err != nil {
		t.Fatalf("%s", err)
	}

	if consumer.Channel().Connection().TimesConnected != 2 {
		t.Fatalf("error at consumer connection count.\nexpected: %d\ngot:      %d", 2, consumer.Channel().Connection().TimesConnected)
	}
	if publisher.Channel().Connection().TimesConnected != 2 {
		t.Fatalf("error at publisher connection count.\nexpected: %d\ngot:      %d", 2, publisher.Channel().Connection().TimesConnected)
	}

	consumer.Channel().Connection().Close(ctx)
	publisher.Channel().Connection().Close(ctx)

	if consumer.Channel().Connection().TimesConnected != 2 {
		t.Fatalf("error at consumer connection count.\nexpected: %d\ngot:      %d", 2, consumer.Channel().Connection().TimesConnected)
	}
	if publisher.Channel().Connection().TimesConnected != 2 {
		t.Fatalf("error at publisher connection count.\nexpected: %d\ngot:      %d", 2, publisher.Channel().Connection().TimesConnected)
	}

	consumer.CloseConnection(ctx)
	publisher.CloseConnection(ctx)

	time.Sleep(600 * time.Millisecond)

	if consumer.Channel().Connection().IsConnected(true) {
		t.Fatalf("consumer connection should be down")
	}
	if publisher.Channel().Connection().IsConnected(true) {
		t.Fatalf("publisher connection should be down")
	}

	if consumer.Channel().IsChannelUp(true) {
		t.Fatalf("consumer channel should be down")
	}
	if publisher.Channel().IsChannelUp(true) {
		t.Fatalf("publisher channel should be down")
	}

	t.Logf("finishing testing Connection for RabbitMQ\n\n")
}

func TestChannelRabbitMQ(t *testing.T) {
	t.Logf("testing Channel for RabbitMQ\n\n")
	ctx := context.Background()

	var env config.RABBITMQ_CONFIG = config.RABBITMQ_CONFIG{
		PROTOCOL: "amqp",
		HOST:     "localhost",
		PORT:     "5672",
		USERNAME: "guest",
		PASSWORD: "guest",
	}

	var queue dto_rabbitmq.RabbitMQQueue = dto_rabbitmq.RabbitMQQueue{
		Exchange:     "",
		ExchangeType: "",
		Name:         "",
		AccessKey:    "",
		Qos:          1,
		Purge:        true,
	}

	var consumer *rabbitmq.RabbitMQConsumer = rabbitmq.NewRabbitMQConsumer(
		ctx,
		env,
		"test",
		nil,
		queue,
		0,
		0,
		nil,
	)

	var publisher *rabbitmq.RabbitMQPublisher = rabbitmq.NewRabbitMQPublisher(
		ctx,
		env,
		"test",
		queue,
		nil,
	)

	consumer.Connect(ctx)
	publisher.Connect(ctx)

	if consumer.Channel().TimesCreated != 1 {
		t.Fatalf("error at consumer channel.\nexpected: %d\ngot:      %d", 1, consumer.Channel().TimesCreated)
	}
	if publisher.Channel().TimesCreated != 1 {
		t.Fatalf("error at publisher channel.\nexpected: %d\ngot:      %d", 1, publisher.Channel().TimesCreated)
	}
	if !consumer.Channel().IsChannelUp(true) {
		t.Fatalf("consumer channel should be up")
	}
	if !publisher.Channel().IsChannelUp(true) {
		t.Fatalf("publisher channel should be up")
	}

	consumer.Channel().Channel.Close()
	publisher.Channel().Channel.Close()

	err := consumer.Channel().WaitForChannel(ctx, true)
	if err != nil {
		t.Fatalf("%s", err)
	}
	err = publisher.Channel().WaitForChannel(ctx, true)
	if err != nil {
		t.Fatalf("%s", err)
	}

	if !consumer.Channel().IsChannelUp(true) {
		t.Fatalf("consumer channel should be up")
	}
	if !publisher.Channel().IsChannelUp(true) {
		t.Fatalf("publisher channel should be up")
	}

	if consumer.Channel().TimesCreated != 2 {
		t.Fatalf("error at consumer channel count.\nexpected: %d\ngot:      %d", 2, consumer.Channel().TimesCreated)
	}
	if publisher.Channel().TimesCreated != 2 {
		t.Fatalf("error at publisher channel count.\nexpected: %d\ngot:      %d", 2, publisher.Channel().TimesCreated)
	}

	consumer.CloseChannel(ctx)
	publisher.CloseChannel(ctx)

	if consumer.Channel().IsChannelUp(true) {
		t.Fatalf("consumer channel should be down")
	}
	if publisher.Channel().IsChannelUp(true) {
		t.Fatalf("publisher channel should be down")
	}

	if consumer.Channel().TimesCreated != 2 {
		t.Fatalf("error at consumer channel count.\nexpected: %d\ngot:      %d", 2, consumer.Channel().TimesCreated)
	}
	if publisher.Channel().TimesCreated != 2 {
		t.Fatalf("error at publisher channel count.\nexpected: %d\ngot:      %d", 2, publisher.Channel().TimesCreated)
	}

	consumer.CloseConnection(ctx)
	publisher.CloseConnection(ctx)
	time.Sleep(time.Second)

	t.Logf("finishing testing Channel for RabbitMQ\n\n")
}

func TestShareConnectionRabbitMQ(t *testing.T) {
	t.Logf("testing ShareConnection for RabbitMQ\n\n")
	ctx := context.Background()

	var env config.RABBITMQ_CONFIG = config.RABBITMQ_CONFIG{
		PROTOCOL: "amqp",
		HOST:     "localhost",
		PORT:     "5672",
		USERNAME: "guest",
		PASSWORD: "guest",
	}

	var queue dto_rabbitmq.RabbitMQQueue = dto_rabbitmq.RabbitMQQueue{
		Exchange:     "",
		ExchangeType: "",
		Name:         "",
		AccessKey:    "",
		Qos:          1,
		Purge:        true,
	}

	var consumer *rabbitmq.RabbitMQConsumer = rabbitmq.NewRabbitMQConsumer(
		ctx,
		env,
		"test",
		nil,
		queue,
		0,
		0,
		nil,
	)

	var publisher *rabbitmq.RabbitMQPublisher = rabbitmq.NewRabbitMQPublisher(
		ctx,
		env,
		"test",
		queue,
		nil,
	)

	publisher.ShareConnection(consumer)

	consumer.Connect(ctx)
	publisher.Connect(ctx)

	if !consumer.Channel().Connection().IsConnected(true) {
		t.Fatalf("connection should be up")
	}
	if !publisher.Channel().Connection().IsConnected(true) {
		t.Fatalf("connection should be up")
	}

	if reflect.DeepEqual(publisher.Channel().Connection(), consumer.Channel().Connection()) == false {
		t.Fatalf("error with shared connections\nexpected: %+v\ngot:      %+v", publisher.Channel().Connection(), consumer.Channel().Connection())
	}

	if reflect.DeepEqual(publisher.Channel(), consumer.Channel()) == true {
		t.Fatalf("should not share channels\n")
	}

	consumer.CloseConnection(ctx)

	time.Sleep(600 * time.Millisecond)

	if consumer.Channel().Connection().IsConnected(true) {
		t.Fatalf("consumer connection should be down")
	}
	if publisher.Channel().Connection().IsConnected(true) {
		t.Fatalf("publisher connection should be down")
	}

	if consumer.Channel().IsChannelUp(true) {
		t.Fatalf("consumer channel should be down")
	}
	if publisher.Channel().IsChannelUp(true) {
		t.Fatalf("publisher channel should be down")
	}

	t.Logf("finishing testing ShareConnection for RabbitMQ\n\n")
}

func TestShareChannelRabbitMQ(t *testing.T) {
	t.Logf("testing ShareChannel for RabbitMQ\n\n")
	ctx := context.Background()

	var env config.RABBITMQ_CONFIG = config.RABBITMQ_CONFIG{
		PROTOCOL: "amqp",
		HOST:     "localhost",
		PORT:     "5672",
		USERNAME: "guest",
		PASSWORD: "guest",
	}

	var queue dto_rabbitmq.RabbitMQQueue = dto_rabbitmq.RabbitMQQueue{
		Exchange:     "",
		ExchangeType: "",
		Name:         "",
		AccessKey:    "",
		Qos:          1,
		Purge:        true,
	}

	var consumer *rabbitmq.RabbitMQConsumer = rabbitmq.NewRabbitMQConsumer(
		ctx,
		env,
		"test",
		nil,
		queue,
		0,
		0,
		nil,
	)

	var publisher *rabbitmq.RabbitMQPublisher = rabbitmq.NewRabbitMQPublisher(
		ctx,
		env,
		"test",
		queue,
		nil,
	)

	publisher.ShareChannel(consumer)
	consumer.Connect(ctx)

	if !consumer.Channel().Connection().IsConnected(true) {
		t.Fatalf("connection should be up")
	}
	if !publisher.Channel().Connection().IsConnected(true) {
		t.Fatalf("connection should be up")
	}

	if !consumer.Channel().IsChannelUp(true) {
		t.Fatalf("channel should be up")
	}
	if !publisher.Channel().IsChannelUp(true) {
		t.Fatalf("channel should be up")
	}

	if reflect.DeepEqual(publisher.Channel().Connection(), consumer.Channel().Connection()) == false {
		t.Fatalf("error with shared connections\nexpected: %+v\ngot:      %+v", publisher.Channel().Connection(), consumer.Channel().Connection())
	}

	if reflect.DeepEqual(publisher.Channel(), consumer.Channel()) == false {
		t.Fatalf("error with shared channels\nexpected: %+v\ngot:      %+v", publisher.Channel(), consumer.Channel())
	}

	consumer.CloseConnection(ctx)

	time.Sleep(600 * time.Millisecond)

	if consumer.Channel().Connection().IsConnected(true) {
		t.Fatalf("connection should be down")
	}
	if publisher.Channel().Connection().IsConnected(true) {
		t.Fatalf("connection should be down")
	}

	if consumer.Channel().IsChannelUp(true) {
		t.Fatalf("channel should be down")
	}
	if publisher.Channel().IsChannelUp(true) {
		t.Fatalf("channel should be down")
	}

	t.Logf("finishing testing ShareChannel for RabbitMQ\n\n")
}

func TestChannelAndConnectionContextCancellationRabbitMQ(t *testing.T) {
	t.Logf("testing Channel And Connection Context Cancellation for RabbitMQ\n\n")
	ctx := context.Background()

	var env config.RABBITMQ_CONFIG = config.RABBITMQ_CONFIG{
		PROTOCOL: "amqp",
		HOST:     "localhost",
		PORT:     "5672",
		USERNAME: "guest",
		PASSWORD: "guest",
	}

	var queue dto_rabbitmq.RabbitMQQueue = dto_rabbitmq.RabbitMQQueue{
		Exchange:     "",
		ExchangeType: "",
		Name:         "",
		AccessKey:    "",
		Qos:          1,
		Purge:        true,
	}

	var publisher *rabbitmq.RabbitMQPublisher = rabbitmq.NewRabbitMQPublisher(
		ctx,
		env,
		"test",
		queue,
		nil,
	)

	var consumer *rabbitmq.RabbitMQConsumer = rabbitmq.NewRabbitMQConsumer(
		ctx,
		env,
		"test",
		publisher,
		queue,
		0,
		0,
		nil,
	)

	publisher.ShareConnection(consumer)

	consumer.Connect(ctx)

	time.Sleep(300 * time.Millisecond)

	consumer.CloseConnection(ctx)

	time.Sleep(600 * time.Millisecond)

	if consumer.IsRunning() {
		t.Fatalf("consumer should not be running")
	}

	if consumer.IsConsuming() {
		t.Fatalf("consumer should not be consuming")
	}

	if consumer.Channel().IsActive(true) {
		t.Fatalf("channel should be closed")
	}

	if consumer.Channel().IsChannelUp(true) {
		t.Fatalf("channel should not be up")
	}

	if publisher.Channel().IsActive(true) {
		t.Fatalf("channel should be closed")
	}

	if publisher.Channel().IsChannelUp(true) {
		t.Fatalf("channel should be up")
	}

	if consumer.Channel().Connection().IsActive(true) {
		t.Fatalf("connection should be closed")
	}

	if consumer.Channel().Connection().IsConnected(true) {
		t.Fatalf("connection should be down")
	}

	if publisher.Channel().Connection().IsActive(true) {
		t.Fatalf("connection should be closed")
	}

	if publisher.Channel().Connection().IsConnected(true) {
		t.Fatalf("connection should be down")
	}

	t.Logf("finishing testing Channel And Connection Context Cancellation for RabbitMQ\n\n")
}

func TestPublishRabbitMQ(t *testing.T) {
	t.Logf("testing Publish for RabbitMQ\n\n")
	ctx := context.Background()

	var env config.RABBITMQ_CONFIG = config.RABBITMQ_CONFIG{
		PROTOCOL: "amqp",
		HOST:     "localhost",
		PORT:     "5672",
		USERNAME: "guest",
		PASSWORD: "guest",
	}

	var queue dto_rabbitmq.RabbitMQQueue = dto_rabbitmq.RabbitMQQueue{
		Exchange:     "test",
		ExchangeType: "direct",
		Name:         "adapter_test",
		AccessKey:    "adapter_test",
	}

	var publisher *rabbitmq.RabbitMQPublisher = rabbitmq.NewRabbitMQPublisher(
		ctx,
		env,
		"test",
		queue,
		nil,
	)

	publisher.Connect(ctx)

	publisher.PrepareQueue(ctx, true)

	_, err := publisher.Channel().Channel.QueuePurge(queue.Name, true)
	if err != nil {
		t.Fatalf("error purging the queue: %s", err.Error())
	}

	var expectedPublishing dto_broker.BrokerPublishing = dto_broker.BrokerPublishing{
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

	err = publisher.Publish(ctx, expectedPublishing)
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
		t.Fatalf("error deleting queue: %s", err)
	}

	publisher.CloseConnection(ctx)

	t.Logf("finishing testing Publish for RabbitMQ\n\n")
}

func TestConsumeForeverAndAcknowledgeRabbitMQ(t *testing.T) {
	t.Logf("testing ConsumeForever and Acknowledge for RabbitMQ\n\n")
	ctx := context.Background()

	var messages []string = []string{"test001", "test002", "test003"}
	var expectedHeader map[string]any = map[string]any{
		"wololo": "walala",
		"type":   "test",
	}

	var env config.RABBITMQ_CONFIG = config.RABBITMQ_CONFIG{
		PROTOCOL: "amqp",
		HOST:     "localhost",
		PORT:     "5672",
		USERNAME: "guest",
		PASSWORD: "guest",
	}

	var queue dto_rabbitmq.RabbitMQQueue = dto_rabbitmq.RabbitMQQueue{
		Exchange:     "test",
		ExchangeType: "direct",
		Name:         "adapter_test",
		AccessKey:    "adapter_test",
		Qos:          1,
		Purge:        true,
	}

	var consumer *rabbitmq.RabbitMQConsumer = rabbitmq.NewRabbitMQConsumer(
		ctx,
		env,
		"test",
		nil,
		queue,
		0,
		0,
		nil,
	)

	consumer.Connect(ctx)

	deliveryChannel := consumer.Deliveries()

	go consumer.ConsumeForever(ctx)
	time.Sleep(600 * time.Millisecond)

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
			dto_broker.BrokerAcknowledge{
				MessageId: delivery.Id,
				Action:    constant_broker.ACKNOWLEDGE_SUCCESS,
			},
		)
		if err != nil {
			t.Fatalf("error with acknowledge: %s", err.Error())
		}
	}

	amqpQueue, err := consumer.Channel().Channel.QueueDeclarePassive(queue.Name, true, false, false, false, nil)
	if err != nil {
		t.Fatalf("%s", err)
	}

	if amqpQueue.Messages != 0 {
		t.Fatalf("expected to have no messages on queue, but still got %d", amqpQueue.Messages)
	}

	err = deleteQueueAndExchange(consumer.Channel().Channel, queue.Name, queue.Exchange, "doit")
	if err != nil {
		t.Fatalf("error deleting queue: %s", err)
	}

	consumer.BreakConsume(ctx)
	consumer.CloseConnection(ctx)

	t.Logf("finishing testing ConsumeForever and Acknowledge for RabbitMQ\n\n")
}

func TestConsumeForeverAndAcknowledgeViaChannelRabbitMQ(t *testing.T) {
	t.Logf("testing ConsumeForever and Acknowledge via channel for RabbitMQ\n\n")
	ctx := context.Background()

	var messages []string = []string{"test001", "test002"}
	var expectedHeader map[string]any = map[string]any{
		"wololo": "walala",
		"type":   "test",
	}

	var env config.RABBITMQ_CONFIG = config.RABBITMQ_CONFIG{
		PROTOCOL: "amqp",
		HOST:     "localhost",
		PORT:     "5672",
		USERNAME: "guest",
		PASSWORD: "guest",
	}

	var queue dto_rabbitmq.RabbitMQQueue = dto_rabbitmq.RabbitMQQueue{
		Exchange:     "test",
		ExchangeType: "direct",
		Name:         "adapter_test",
		AccessKey:    "adapter_test",
		Qos:          1,
		Purge:        true,
	}

	var deadletter dto_rabbitmq.RabbitMQQueue = dto_rabbitmq.RabbitMQQueue{
		Exchange:     "test",
		ExchangeType: "direct",
		Name:         "deadletter_test",
		AccessKey:    "deadletter_test",
	}

	var publisher *rabbitmq.RabbitMQPublisher = rabbitmq.NewRabbitMQPublisher(
		ctx,
		env,
		"deadletter",
		deadletter,
		nil,
	)
	var consumer *rabbitmq.RabbitMQConsumer = rabbitmq.NewRabbitMQConsumer(
		ctx,
		env,
		"test",
		publisher,
		queue,
		0,
		0,
		nil,
	)

	publisher.ShareChannel(consumer)
	consumer.Connect(ctx)

	deliveryChannel := consumer.Deliveries()

	go consumer.ConsumeForever(ctx)
	time.Sleep(600 * time.Millisecond)

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

	// IN CASE OF FAILURE, THE TEST WOULD BLOCK

	// testing success
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

	delivery.Acknowledger <- dto_broker.BrokerAcknowledge{
		MessageId: delivery.Id,
		Action:    constant_broker.ACKNOWLEDGE_SUCCESS,
	}
	// testing success

	// testing requeue
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

	delivery.Acknowledger <- dto_broker.BrokerAcknowledge{
		MessageId: delivery.Id,
		Action:    constant_broker.ACKNOWLEDGE_REQUEUE,
	}
	// testing requeue

	// testing deadletter
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

	delivery.Acknowledger <- dto_broker.BrokerAcknowledge{
		MessageId: delivery.Id,
		Action:    constant_broker.ACKNOWLEDGE_DEADLETTER,
		Report: dto_broker.BrokerPublishing{
			Header: delivery.Header,
			Body:   delivery.Body,
		},
		LoggingCtx: ctx,
	}

	time.Sleep(600 * time.Millisecond)

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
	// testing deadletter

	amqpQueue, err := consumer.Channel().Channel.QueueDeclarePassive(queue.Name, true, false, false, false, nil)
	if err != nil {
		t.Fatalf("%s", err)
	}

	if amqpQueue.Messages != 0 {
		t.Fatalf("expected to have no messages on queue, but still got %d", amqpQueue.Messages)
	}

	err = deleteQueueAndExchange(consumer.Channel().Channel, queue.Name, queue.Exchange, "doit")
	if err != nil {
		t.Fatalf("error deleting queue: %s", err.Error())
	}
	err = deleteQueueAndExchange(consumer.Channel().Channel, deadletter.Name, deadletter.Exchange, "doit")
	if err != nil {
		t.Fatalf("error deleting queue: %s", err.Error())
	}

	consumer.BreakConsume(ctx)
	consumer.CloseConnection(ctx)

	t.Logf("finishing testing ConsumeForever and Acknowledge via channel for RabbitMQ\n\n")
}

func TestAcknowledgeDeadletterMissingPublisherRabbitMQ(t *testing.T) {
	t.Logf("testing Acknowledge deadletter with missing publisher RabbitMQ\n\n")
	ctx := context.Background()

	var message string = "test001"
	var expectedHeader map[string]any = map[string]any{
		"wololo": "walala",
		"type":   "test",
	}

	var env config.RABBITMQ_CONFIG = config.RABBITMQ_CONFIG{
		PROTOCOL: "amqp",
		HOST:     "localhost",
		PORT:     "5672",
		USERNAME: "guest",
		PASSWORD: "guest",
	}

	var queue dto_rabbitmq.RabbitMQQueue = dto_rabbitmq.RabbitMQQueue{
		Exchange:     "test",
		ExchangeType: "direct",
		Name:         "adapter_test",
		AccessKey:    "adapter_test",
		Qos:          1,
		Purge:        true,
	}

	var consumer *rabbitmq.RabbitMQConsumer = rabbitmq.NewRabbitMQConsumer(
		ctx,
		env,
		"test",
		nil,
		queue,
		0,
		0,
		nil,
	)

	consumer.Connect(ctx)

	consumer.PrepareQueue(ctx, true)

	deliveryChannel := consumer.Deliveries()

	go consumer.ConsumeForever(ctx)
	time.Sleep(600 * time.Millisecond)

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

	consumer.Acknowledge(dto_broker.BrokerAcknowledge{
		MessageId: delivery.Id,
		Action:    constant_broker.ACKNOWLEDGE_DEADLETTER,
		Report: dto_broker.BrokerPublishing{
			Header: delivery.Header,
			Body:   delivery.Body,
		},
	})

	err = deleteQueueAndExchange(consumer.Channel().Channel, queue.Name, queue.Exchange, "doit")
	if err != nil {
		t.Fatalf("error deleting queue: %s", err.Error())
	}

	consumer.BreakConsume(ctx)
	consumer.CloseConnection(ctx)

	t.Logf("finishing testing Acknowledge deadletter with missing publisher RabbitMQ\n\n")
}
