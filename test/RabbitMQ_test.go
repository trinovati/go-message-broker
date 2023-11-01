package testing

import (
	"bytes"
	"context"
	"encoding/gob"
	"log"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	rabbitmq "gitlab.com/aplicacao/trinovati-connector-message-brokers/RabbitMQ"
	"gitlab.com/aplicacao/trinovati-connector-message-brokers/RabbitMQ/channel"
	"gitlab.com/aplicacao/trinovati-connector-message-brokers/RabbitMQ/connection"
	"gitlab.com/aplicacao/trinovati-connector-message-brokers/dto"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestConnectionRabbitMQ(t *testing.T) {
	log.Printf("\ntesting Connection for RabbitMQ\n\n")

	messageBroker := rabbitmq.NewRabbitMQ().Behave(
		rabbitmq.NewPublisher(
			"",
			"",
			"",
			"",
		),
	).Behave(
		rabbitmq.NewConsumer(
			nil,
			nil,
			nil,
			"",
			"",
			"",
			"",
			0,
			false,
		),
	).Connect()

	if messageBroker.Publisher.Channel().Connection().(*connection.Connection).ConnectionId != 1 {
		t.Error("error at publisher Connection id.\nexpected: " + strconv.Itoa(1) + "\ngot:      " + strconv.FormatUint(messageBroker.Publisher.Channel().Connection().(*connection.Connection).ConnectionId, 10))
	}
	if messageBroker.Consumer.Channel().Connection().(*connection.Connection).ConnectionId != 1 {
		t.Error("error at consumer Connection id.\nexpected: " + strconv.Itoa(1) + "\ngot:      " + strconv.FormatUint(messageBroker.Consumer.Channel().Connection().(*connection.Connection).ConnectionId, 10))
	}

	if messageBroker.Publisher.Channel().Connection().IsConnectionDown() {
		t.Error("error at publisher Connection.IsConnectionDown, connection should be up")
	}
	if messageBroker.Consumer.Channel().Connection().IsConnectionDown() {
		t.Error("error at consumer Connection.IsConnectionDown, connection should be up")
	}

	messageBroker.Publisher.Channel().Connection().(*connection.Connection).Connection.Close()
	messageBroker.Consumer.Channel().Connection().(*connection.Connection).Connection.Close()

	time.Sleep(500 * time.Millisecond)

	messageBroker.Publisher.Channel().Connection().WaitForConnection()
	messageBroker.Consumer.Channel().Connection().WaitForConnection()

	if messageBroker.Publisher.Channel().Connection().(*connection.Connection).ConnectionId != 2 {
		t.Error("error at publisher connection id.\nexpected: " + strconv.Itoa(2) + "\ngot:      " + strconv.FormatUint(messageBroker.Publisher.Channel().Connection().(*connection.Connection).ConnectionId, 10))
	}
	if messageBroker.Consumer.Channel().Connection().(*connection.Connection).ConnectionId != 2 {
		t.Error("error at consumer connection id.\nexpected: " + strconv.Itoa(2) + "\ngot:      " + strconv.FormatUint(messageBroker.Consumer.Channel().Connection().(*connection.Connection).ConnectionId, 10))
	}

	time.Sleep(time.Second)
	messageBroker.CloseConnection()
	time.Sleep(time.Second)

	if !messageBroker.Publisher.Channel().Connection().IsConnectionDown() {
		t.Error("error at IsConnectionDown, connection should be down")
	}

	if !messageBroker.Consumer.Channel().Connection().IsConnectionDown() {
		t.Error("error at IsConnectionDown, connection should be down")
	}

	if !messageBroker.Publisher.Channel().IsChannelDown() {
		t.Error("error at IsChannelDown, channel should be down")
	}

	if !messageBroker.Consumer.Channel().IsChannelDown() {
		t.Error("error at IsChannelDown, channel should be down")
	}

	log.Printf("\nfinishing testing Connection for RabbitMQ\n\n\n")
}

func TestChannelRabbitMQ(t *testing.T) {
	log.Printf("\ntesting Channel for RabbitMQ\n\n")

	messageBroker := rabbitmq.NewRabbitMQ().Behave(
		rabbitmq.NewPublisher(
			"",
			"",
			"",
			"",
		),
	).Behave(
		rabbitmq.NewConsumer(
			nil,
			nil,
			nil,
			"",
			"",
			"",
			"",
			0,
			false,
		),
	).Connect()

	if messageBroker.Publisher.Channel().(*channel.Channel).ChannelId != 1 {
		t.Error("error at Channel id.\nexpected: " + strconv.Itoa(1) + "\ngot:      " + strconv.FormatUint(messageBroker.Publisher.Channel().(*channel.Channel).ChannelId, 10))
	}

	if messageBroker.Consumer.Channel().(*channel.Channel).ChannelId != 1 {
		t.Error("error at BehaviourType.\nexpected: " + strconv.Itoa(1) + "\ngot:      " + strconv.FormatUint(messageBroker.Consumer.Channel().(*channel.Channel).ChannelId, 10))
	}

	if messageBroker.Publisher.Channel().IsChannelDown() {
		t.Error("error at IsChannelDown, channel should be up")
	}

	if messageBroker.Consumer.Channel().IsChannelDown() {
		t.Error("error at IsChannelDown, channel should be up")
	}

	messageBroker.Publisher.Channel().(*channel.Channel).Channel.Close()
	messageBroker.Consumer.Channel().(*channel.Channel).Channel.Close()

	messageBroker.Publisher.Channel().WaitForChannel()
	if messageBroker.Publisher.Channel().(*channel.Channel).ChannelId != 2 {
		t.Error("error at Channel id.\nexpected: " + strconv.Itoa(2) + "\ngot:      " + strconv.FormatUint(messageBroker.Publisher.Channel().(*channel.Channel).ChannelId, 10))
	}

	messageBroker.Consumer.Channel().WaitForChannel()
	if messageBroker.Consumer.Channel().(*channel.Channel).ChannelId != 2 {
		t.Error("error at IsChannelDown.\nexpected: " + strconv.Itoa(2) + "\ngot:      " + strconv.FormatUint(messageBroker.Consumer.Channel().(*channel.Channel).ChannelId, 10))
	}

	messageBroker.CloseChannel()
	time.Sleep(time.Second)

	if !messageBroker.Publisher.Channel().IsChannelDown() {
		t.Error("error at IsChannelDown, channel should be down")
	}

	if !messageBroker.Consumer.Channel().IsChannelDown() {
		t.Error("error at IsChannelDown, channel should be down")
	}

	messageBroker.CloseConnection()
	time.Sleep(time.Second)

	log.Printf("\nfinishing testing Channel for RabbitMQ\n\n\n")
}

func TestShareConnectionRabbitMQ(t *testing.T) {
	log.Printf("\ntesting ShareConnection for RabbitMQ\n\n")

	messageBroker := rabbitmq.NewRabbitMQ().Behave(
		rabbitmq.NewPublisher(
			"",
			"",
			"",
			"",
		),
	).Behave(
		rabbitmq.NewConsumer(
			nil,
			nil,
			nil,
			"",
			"",
			"",
			"",
			0,
			false,
		),
	).Connect()

	newMessageBroker := rabbitmq.NewRabbitMQ().Behave(
		rabbitmq.NewPublisher(
			"",
			"",
			"",
			"",
		),
	).Behave(
		rabbitmq.NewConsumer(
			nil,
			nil,
			nil,
			"",
			"",
			"",
			"",
			0,
			false,
		),
	)

	newMessageBroker.Publisher.ShareConnection(messageBroker.Publisher)
	newMessageBroker.Consumer.ShareConnection(messageBroker.Consumer)

	if reflect.DeepEqual(messageBroker.Publisher.Channel().Connection(), newMessageBroker.Publisher.Channel().Connection()) == false {
		t.Errorf("error with Movimentation\nexpected: %+v\ngot:      %+v", messageBroker.Publisher.Channel().Connection(), newMessageBroker.Publisher.Channel().Connection())
	}

	if reflect.DeepEqual(messageBroker.Consumer.Channel().Connection(), newMessageBroker.Consumer.Channel().Connection()) == false {
		t.Errorf("error with Movimentation\nexpected: %+v\ngot:      %+v", messageBroker.Consumer.Channel().Connection(), newMessageBroker.Consumer.Channel().Connection())
	}

	messageBroker.CloseConnection()
	time.Sleep(time.Second)

	log.Printf("\nfinishing testing ShareConnection for RabbitMQ\n\n\n")
}

func TestShareChannelRabbitMQ(t *testing.T) {
	log.Printf("\ntesting ShareChannel for RabbitMQ\n\n")

	messageBroker := rabbitmq.NewRabbitMQ().Behave(
		rabbitmq.NewPublisher(
			"",
			"",
			"",
			"",
		),
	).Behave(
		rabbitmq.NewConsumer(
			nil,
			nil,
			nil,
			"",
			"",
			"",
			"",
			0,
			false,
		),
	).Connect()

	newMessageBroker := rabbitmq.NewRabbitMQ().Behave(
		rabbitmq.NewPublisher(
			"",
			"",
			"",
			"",
		),
	).Behave(
		rabbitmq.NewConsumer(
			nil,
			nil,
			nil,
			"",
			"",
			"",
			"",
			0,
			false,
		),
	)

	newMessageBroker.Publisher.ShareChannel(messageBroker.Publisher)
	newMessageBroker.Consumer.ShareChannel(messageBroker.Consumer)

	if reflect.DeepEqual(messageBroker.Publisher.Channel(), newMessageBroker.Publisher.Channel()) == false {
		t.Errorf("error with Movimentation\nexpected: %+v\ngot:      %+v", messageBroker.Publisher.Channel().Connection(), newMessageBroker.Publisher.Channel())
	}

	if reflect.DeepEqual(messageBroker.Consumer.Channel(), newMessageBroker.Consumer.Channel()) == false {
		t.Errorf("error with Movimentation\nexpected: %+v\ngot:      %+v", messageBroker.Consumer.Channel(), newMessageBroker.Consumer.Channel())
	}

	messageBroker.CloseConnection()
	time.Sleep(time.Second)

	log.Printf("\nfinishing testing ShareChannel for RabbitMQ\n\n\n")
}

func TestPublishRabbitMQ(t *testing.T) {
	log.Print("\nfinishing testing Publish for RabbitMQ\n\n")

	var err error

	var expectedMessage string = "teste001"

	var gobTarget bytes.Buffer
	var target dto.Target

	var exchangeName string = "tests"
	var exchangeType string = "direct"
	var queueName string = exchangeName + "__Publish()"
	var accessKey string = queueName

	target = dto.Target{
		Exchange:     exchangeName,
		ExchangeType: accessKey,
		Queue:        queueName,
		AccessKey:    accessKey,
	}

	err = gob.NewEncoder(&gobTarget).Encode(target)
	if err != nil {
		t.Fatal("error encoding gob: " + err.Error())
	}

	messageBroker := rabbitmq.NewRabbitMQ().Behave(
		rabbitmq.NewPublisher(
			exchangeName,
			exchangeType,
			queueName,
			accessKey,
		),
	).Connect()

	messageBroker.Publisher.PrepareQueue(nil)

	_, err = messageBroker.Publisher.Channel().(*channel.Channel).Channel.QueuePurge(queueName, true)
	if err != nil {
		t.Error("error purging the queue: " + err.Error())
	}

	err = messageBroker.Publisher.(*rabbitmq.Publisher).Publish([]byte(expectedMessage), nil)
	if err != nil {
		t.Error("error publishing to queue: " + err.Error())
	}

	recievedMessage, _, err := messageBroker.Publisher.Channel().Access().Get(queueName, true)
	if err != nil {
		t.Error("error consuming message: " + err.Error())
	}

	if string(recievedMessage.Body) != expectedMessage {
		t.Error("error at with message body.\nexpected: " + expectedMessage + "\ngot:      " + string(recievedMessage.Body))
	}

	err = messageBroker.Publisher.(*rabbitmq.Publisher).Publish([]byte(expectedMessage), gobTarget.Bytes())
	if err != nil {
		t.Error("error publishing to queue: " + err.Error())
	}

	recievedMessage, _, err = messageBroker.Publisher.Channel().Access().Get(queueName, true)
	if err != nil {
		t.Error("error consuming message: " + err.Error())
	}

	if string(recievedMessage.Body) != expectedMessage {
		t.Error("error at with message body.\nexpected: " + expectedMessage + "\ngot:      " + string(recievedMessage.Body))
	}

	err = rabbitmq.DeleteQueueAndExchange(messageBroker.Publisher.Channel().Access(), queueName, exchangeName, "doit")
	if err != nil {
		t.Error("error deleting queue: " + err.Error())
	}

	messageBroker.CloseConnection()
	time.Sleep(time.Second)

	log.Printf("\nfinishing testing Publish for RabbitMQ\n\n\n")
}

func TestPersistRabbitMQ(t *testing.T) {
	log.Printf("\ntesting Persist for RabbitMQ\n\n")

	var err error

	var expectedMessage string = "teste"

	var gobTarget bytes.Buffer
	var target dto.Target

	var exchangeName string = "tests"
	var exchangeType string = "direct"
	var queueName string = exchangeName + "__Persist()"
	var accessKey string = queueName

	target = dto.Target{
		Exchange:     exchangeName,
		ExchangeType: accessKey,
		Queue:        queueName,
		AccessKey:    accessKey,
	}

	err = gob.NewEncoder(&gobTarget).Encode(target)
	if err != nil {
		t.Fatal("error encoding gob: " + err.Error())
	}

	messageBroker := rabbitmq.NewRabbitMQ().Behave(
		rabbitmq.NewPublisher(
			exchangeName,
			exchangeType,
			queueName,
			accessKey,
		),
	).Connect()

	err = messageBroker.Publisher.PrepareQueue(nil)
	if err != nil {
		t.Fatal("error preparing queue: " + err.Error())
	}

	err = messageBroker.Persist([]byte(expectedMessage), nil)
	if err != nil {
		t.Fatal("error persisting data: " + err.Error())
	}

	delivery, _, err := messageBroker.Publisher.(*rabbitmq.Publisher).Channel().Access().Get(queueName, true)
	if err != nil {
		t.Error("error consuming message: " + err.Error())
	}

	if string(delivery.Body) != expectedMessage {
		t.Error("error persisting data.\nexpected: " + expectedMessage + "\ngot:      " + string(delivery.Body))
	}

	err = messageBroker.Persist([]byte(expectedMessage), gobTarget.Bytes())
	if err != nil {
		t.Fatal("error persisting data: " + err.Error())
	}

	delivery, _, err = messageBroker.Publisher.(*rabbitmq.Publisher).Channel().Access().Get(queueName, true)
	if err != nil {
		t.Error("error consuming message: " + err.Error())
	}

	if string(delivery.Body) != expectedMessage {
		t.Error("error persisting data.\nexpected: " + expectedMessage + "\ngot:      " + string(delivery.Body))
	}

	err = rabbitmq.DeleteQueueAndExchange(messageBroker.Publisher.(*rabbitmq.Publisher).Channel().Access(), queueName, exchangeName, "doit")
	if err != nil {
		t.Error("error deleting queue and exchange: " + err.Error())
	}

	messageBroker.CloseConnection()
	time.Sleep(time.Second)

	log.Printf("\nfinishing testing Persist for RabbitMQ\n\n\n")
}

func TestConsumeForeverRabbitMQ(t *testing.T) {
	log.Printf("\ntesting ConsumeForever for RabbitMQ\n\n")

	var messages []string
	messages = append(messages, "teste001", "teste002", "teste003")

	var buffer bytes.Buffer
	var message dto.Message
	var acknowledge dto.Acknowledge
	var delivery []byte

	exchangeName := "tests"
	exchangeType := "direct"
	queueName := exchangeName + "__ConsumeForever()"
	accessKey := queueName
	qos := 0
	purgeBeforeStarting := true
	deliveryChannel := make(chan []byte)
	unacknowledgedDeliveryMap := &sync.Map{}

	messageBroker := rabbitmq.NewRabbitMQ().Behave(
		rabbitmq.NewConsumer(
			nil,
			deliveryChannel,
			unacknowledgedDeliveryMap,
			exchangeName,
			exchangeType,
			queueName,
			accessKey,
			qos,
			purgeBeforeStarting,
		),
	).Connect()

	go messageBroker.ConsumeForever()
	time.Sleep(time.Second)

	for i, expectedMessage := range messages {
		confirmation, err := messageBroker.Consumer.Channel().Access().PublishWithDeferredConfirmWithContext(context.Background(), exchangeName, accessKey, true, false, amqp.Publishing{Body: []byte(expectedMessage)})
		if err != nil {
			t.Error("error publishing message to RabbitMQ: " + err.Error())
		}

		success := confirmation.Wait()
		if !success {
			t.Error("Publishing confirmation failed on queue '" + queueName + "' with delivery TAG '" + strconv.FormatUint(confirmation.DeliveryTag, 10) + "'.")
		}

		delivery = <-deliveryChannel

		buffer.Reset()
		_, err = buffer.Write(delivery)
		if err != nil {
			t.Fatal("error writing to buffer: " + err.Error())
		}

		err = gob.NewDecoder(&buffer).Decode(&message)
		if err != nil {
			t.Fatal("error decoding gob: " + err.Error())
		}

		if string(message.Data) != expectedMessage {
			t.Fatal("error at consume.\nexpected: " + expectedMessage + "\ngot:      " + string(message.Data))
		}

		if message.Id != strconv.Itoa(i+1) {
			t.Fatal("error at consume id.\nexpected: " + strconv.Itoa(i+1) + "\ngot:      " + message.Id)
		}

		log.Println(string(message.Data))

		acknowledge = dto.Acknowledge{
			Id:      message.Id,
			Success: true,
			Requeue: false,
			Enqueue: false,
		}

		buffer.Reset()
		err = gob.NewEncoder(&buffer).Encode(acknowledge)
		if err != nil {
			t.Fatal("error encoding gob: " + err.Error())
		}

		err = messageBroker.Acknowledge(buffer.Bytes())
		if err != nil {
			t.Error("error with acknowlege: " + err.Error())
		}
	}

	err := rabbitmq.DeleteQueueAndExchange(messageBroker.Consumer.Channel().Access(), queueName, exchangeName, "doit")
	if err != nil {
		t.Error("error deleting queue " + queueName + ": " + err.Error())
	}

	messageBroker.CloseConnection()

	log.Printf("\nfinishing testing ConsumeForever for RabbitMQ\n\n\n")
}
