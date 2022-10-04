package main

import (
	"log"
	"runtime"
	"time"

	messagebrokerZ "gitlab.com/aplicacao/trinovati-connector-message-brokers"
	messagebroker "gitlab.com/aplicacao/trinovati-connector-message-brokers/NewLibraryRabbitMQ"
)

func main() {
	messageConsumeManagerChannel := make(chan interface{})
	messageConsumeManagerChannel2 := make(chan interface{})

	exchangeName := "amq.topic"
	exchangeType := "topic"
	queueName := "devices__untreated_messages"
	accessKey := "application.*.device.*.event.*"
	qos := 0
	purgeBeforeStarting := false

	messageBroker := messagebroker.NewRabbitMQ().Connect().PopulateConsume(exchangeName, exchangeType, queueName, accessKey, qos, purgeBeforeStarting, messageConsumeManagerChannel)
	messageBroker2 := messagebroker.NewRabbitMQ().SharesChannelWith(messageBroker).PopulateConsume(exchangeName, exchangeType, queueName, accessKey, qos, purgeBeforeStarting, messageConsumeManagerChannel2)

	go messageBroker.ConsumeForever(1)
	go messageBroker2.ConsumeForever(2)

	log.Println("starting to print")
	log.Println(messageBroker.Channel)
	log.Println(messageBroker2.Channel)
	time.Sleep(time.Second)

	go printConsume(messageConsumeManagerChannel, messageBroker)

	go printConsume(messageConsumeManagerChannel2, messageBroker2)

	runtime.Goexit()
}

func printConsume(messageConsumeManagerChannel <-chan interface{}, messageBroker *messagebroker.RabbitMQ) {
	for message := range messageConsumeManagerChannel {
		log.Println("RECIEVED :" + string(message.(*messagebrokerZ.MessageBrokerConsumedMessage).TransmissionData.([]byte)))

		log.Println("MAIN MESSAGE ID: " + message.(*messagebrokerZ.MessageBrokerConsumedMessage).MessageId)
		err := messageBroker.Acknowledge(true, message.(*messagebrokerZ.MessageBrokerConsumedMessage).MessageId, "", "sucess")
		if err != nil {
			log.Println("ERROR ack: " + err.Error())
		}
	}
}
