package test

import (
	"context"
	"fmt"
	"reflect"
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

	if !reflect.DeepEqual(publisher.Channel().Connection(), consumer.Channel().Connection()) {
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

	if !reflect.DeepEqual(publisher.Channel().Connection(), consumer.Channel().Connection()) {
		t.Fatalf("error with shared connections\nexpected: %+v\ngot:      %+v", publisher.Channel().Connection(), consumer.Channel().Connection())
	}

	if !reflect.DeepEqual(publisher.Channel(), consumer.Channel()) {
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

	err := publisher.PrepareQueue(ctx, true)
	if err != nil {
		t.Fatalf("error preparing queue: %s", err)
	}

	_, err = publisher.Channel().Channel.QueuePurge(queue.Name, true)
	if err != nil {
		t.Fatalf("error purging the queue: %s", err.Error())
	}

	var expectedHeader map[string]any = map[string]any{
		"reason": "test",
		"type":   "test",
	}
	var expectedBody []byte = []byte("payload")

	err = publisher.Publish(ctx, expectedHeader, expectedBody)
	if err != nil {
		t.Fatalf("error publishing to queue: %s", err.Error())
	}

	delivery, _, err := publisher.Channel().Channel.Get(queue.Name, true)
	if err != nil {
		t.Fatalf("error consuming message: %s", err.Error())
	}

	if string(expectedBody) != string(delivery.Body) {
		t.Fatalf("error at body.\nexpected: %s\ngot:      %s", string(expectedBody), string(delivery.Body))
	}

	if !reflect.DeepEqual(expectedHeader, map[string]any(delivery.Headers)) {
		t.Fatalf("error at header.\nexpected: %v\ngot:      %v", expectedHeader, map[string]any(delivery.Headers))
	}

	err = deleteQueueAndExchange(publisher.Channel().Channel, queue.Name, queue.Exchange, "doit")
	if err != nil {
		t.Fatalf("error deleting queue: %s", err)
	}

	publisher.CloseConnection(ctx)

	t.Logf("finishing testing Publish for RabbitMQ\n\n")
}

func TestConsumeForeverAndAcknowledgeRabbitMQ(t *testing.T) {
	t.Logf("testing ConsumeForever and Acknowledge via channel for RabbitMQ\n\n")
	ctx := context.Background()

	var messages []string = []string{
		"test success",
		"test requeue then deadletter",
	}
	var expectedHeader map[string]any = map[string]any{
		"reason": "test",
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
		10,
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

	// testing success
	if len(deliveryChannel) != 1 {
		t.Fatalf("expected to have 1 message at delivery channel, but have %d", len(deliveryChannel))
	}

	delivery := <-deliveryChannel

	var expectedDelivery dto_broker.BrokerDelivery = dto_broker.BrokerDelivery{
		Id:     "1",
		Header: expectedHeader,
		Body:   []byte(messages[0]),
		ConsumerDetail: map[string]any{
			"rabbitmq_queue": dto_rabbitmq.RabbitMQQueue{
				Exchange:     queue.Exchange,
				ExchangeType: queue.ExchangeType,
				Name:         queue.Name,
				AccessKey:    queue.AccessKey,
			},
		},
	}

	if !reflect.DeepEqual(expectedDelivery, delivery) {
		t.Fatalf("error at delivery.\nexpected: %v\ngot:      %v", expectedDelivery, delivery)
	}

	err := consumer.Acknowledge(
		ctx,
		delivery.Id,
		constant_broker.ACKNOWLEDGE_SUCCESS,
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("error acknowledging: %s", err)
	}
	// testing success

	// testing requeue
	time.Sleep(600 * time.Millisecond)
	if len(deliveryChannel) != 1 {
		t.Fatalf("expected to have 1 message at delivery channel, but have %d", len(deliveryChannel))
	}

	delivery = <-deliveryChannel

	expectedDelivery = dto_broker.BrokerDelivery{
		Id:     "2",
		Header: expectedHeader,
		Body:   []byte(messages[1]),
		ConsumerDetail: map[string]any{
			"rabbitmq_queue": dto_rabbitmq.RabbitMQQueue{
				Exchange:     queue.Exchange,
				ExchangeType: queue.ExchangeType,
				Name:         queue.Name,
				AccessKey:    queue.AccessKey,
			},
		},
	}

	if !reflect.DeepEqual(expectedDelivery, delivery) {
		t.Fatalf("error at delivery.\nexpected: %v\ngot:      %v", expectedDelivery, delivery)
	}

	err = consumer.Acknowledge(
		ctx,
		delivery.Id,
		constant_broker.ACKNOWLEDGE_REQUEUE,
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("error acknowledging: %s", err)
	}
	// testing requeue

	// testing deadletter
	time.Sleep(600 * time.Millisecond)
	if len(deliveryChannel) != 1 {
		t.Fatalf("expected to have 1 message at delivery channel, but have %d", len(deliveryChannel))
	}

	delivery = <-deliveryChannel

	expectedDelivery = dto_broker.BrokerDelivery{
		Id:     "3",
		Header: expectedHeader,
		Body:   []byte(messages[1]),
		ConsumerDetail: map[string]any{
			"rabbitmq_queue": dto_rabbitmq.RabbitMQQueue{
				Exchange:     queue.Exchange,
				ExchangeType: queue.ExchangeType,
				Name:         queue.Name,
				AccessKey:    queue.AccessKey,
			},
		},
	}

	if !reflect.DeepEqual(expectedDelivery, delivery) {
		t.Fatalf("error at delivery.\nexpected: %v\ngot:      %v", expectedDelivery, delivery)
	}

	err = consumer.Acknowledge(
		ctx,
		delivery.Id,
		constant_broker.ACKNOWLEDGE_DEADLETTER,
		delivery.Header,
		delivery.Body,
	)
	if err != nil {
		t.Fatalf("error acknowledging: %s", err)
	}

	time.Sleep(600 * time.Millisecond)

	amqpDelivery, _, err := publisher.Channel().Channel.Get(deadletter.Name, true)
	if err != nil {
		t.Fatalf("error consuming message: %s", err.Error())
	}

	if messages[1] != string(amqpDelivery.Body) {
		t.Fatalf("error at delivery.\nexpected: %s\ngot:      %s", messages[1], string(amqpDelivery.Body))
	}

	if !reflect.DeepEqual(expectedHeader, map[string]any(amqpDelivery.Headers)) {
		t.Fatalf("error at header.\nexpected: %v\ngot:      %v", expectedHeader, map[string]any(amqpDelivery.Headers))
	}
	// testing deadletter

	if len(deliveryChannel) != 0 {
		t.Fatalf("expected to have no messages at queue")
	}

	amqpQueue, err := consumer.Channel().Channel.QueueDeclarePassive(queue.Name, true, false, false, false, nil)
	if err != nil {
		t.Fatalf("failed to get queue: %s", err)
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

func TestAcknowledgeRabbitMQCaseDeadletterMissingPublisher(t *testing.T) {
	t.Logf("testing Acknowledge for RabbitMQ CASE DEADLETTER MISSING PUBLISHER\n\n")
	ctx := context.Background()

	var message string = "test001"
	var expectedHeader map[string]any = map[string]any{
		"reason": "test",
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
		10,
		nil,
	)

	consumer.Connect(ctx)

	err := consumer.PrepareQueue(ctx, true)
	if err != nil {
		t.Fatalf("error preparing queue: %s", err)
	}

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

	time.Sleep(600 * time.Millisecond)

	if len(deliveryChannel) != 1 {
		t.Fatalf("expected to have 1 message at delivery channel, but have %d", len(deliveryChannel))
	}

	delivery := <-deliveryChannel

	var expectedDelivery dto_broker.BrokerDelivery = dto_broker.BrokerDelivery{
		Id:     "1",
		Header: expectedHeader,
		Body:   []byte(message),
		ConsumerDetail: map[string]any{
			"rabbitmq_queue": dto_rabbitmq.RabbitMQQueue{
				Exchange:     queue.Exchange,
				ExchangeType: queue.ExchangeType,
				Name:         queue.Name,
				AccessKey:    queue.AccessKey,
			},
		},
	}

	if reflect.DeepEqual(expectedDelivery, delivery) == false {
		t.Fatalf("error at delivery.\nexpected: %v\ngot:      %v", expectedDelivery, delivery)
	}

	err = consumer.Acknowledge(
		ctx,
		delivery.Id,
		constant_broker.ACKNOWLEDGE_DEADLETTER,
		delivery.Header,
		delivery.Body,
	)
	if err != nil {
		t.Fatalf("error acknowledging: %s", err)
	}

	err = deleteQueueAndExchange(consumer.Channel().Channel, queue.Name, queue.Exchange, "doit")
	if err != nil {
		t.Fatalf("error deleting queue: %s", err.Error())
	}

	consumer.BreakConsume(ctx)
	consumer.CloseConnection(ctx)

	t.Logf("finishing testing Acknowledge deadletter with missing publisher RabbitMQ\n\n")
}
