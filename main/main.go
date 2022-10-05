package main

import (
	"log"
	"runtime"
	"strconv"
	"time"

	messagebroker "gitlab.com/aplicacao/trinovati-connector-message-brokers"
	rabbitmq "gitlab.com/aplicacao/trinovati-connector-message-brokers/NewLibraryRabbitMQ"
)

func main() {
	messageConsumeManagerChannel := make(chan interface{})

	exchangeName := "devices"
	exchangeType := "direct"
	queueName := "devices__tests"
	accessKey := queueName
	qos := 0
	purgeBeforeStarting := false

	consumer := rabbitmq.NewRabbitMQ().Connect().PopulateConsume(exchangeName, exchangeType, queueName, accessKey, qos, purgeBeforeStarting, messageConsumeManagerChannel)

	publisher := rabbitmq.NewRabbitMQ().SharesChannelWith(consumer).PopulatePublish(exchangeName, exchangeType, queueName, accessKey)
	publisher2 := rabbitmq.NewRabbitMQ().SharesChannelWith(publisher).GetPopulatedDataFrom(publisher)

	go consumer.ConsumeForever(1)

	log.Println("starting to print")
	time.Sleep(time.Second)

	go printConsume(messageConsumeManagerChannel, consumer)

	go keepPublishing(publisher, 1)
	go keepPublishing(publisher2, 2)
	go keepPublishing(publisher2, 3)
	go keepPublishing(publisher2, 4)

	runtime.Goexit()
}

func printConsume(messageConsumeManagerChannel <-chan interface{}, messageBroker *rabbitmq.RabbitMQ) {
	for message := range messageConsumeManagerChannel {
		log.Println("RECIEVED :" + string(message.(*messagebroker.MessageBrokerConsumedMessage).TransmissionData.([]byte)))

		log.Println("MAIN MESSAGE ID: " + message.(*messagebroker.MessageBrokerConsumedMessage).MessageId)
		err := messageBroker.Acknowledge(true, message.(*messagebroker.MessageBrokerConsumedMessage).MessageId, "", "sucess")
		if err != nil {
			log.Println("ERROR ack: " + err.Error())
		}
	}
}

func keepPublishing(publisher *rabbitmq.RabbitMQ, j int) {
	var message string

	for i := 1; true; i++ {
		message = "MESSAGE NUMBER " + strconv.Itoa(i) + "FROM " + strconv.Itoa(j)

		err := publisher.Publish(message, "")
		if err != nil {
			log.Println("ERROR ack: " + err.Error())
			i--
		}

		time.Sleep(2 * time.Second)
	}
}
