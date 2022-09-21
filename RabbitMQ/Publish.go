package rabbitmq

import (
	"errors"
	"log"
	"strconv"
	"time"

	"github.com/streadway/amqp"
)

/*
Publish data to the queue linked to RabbitMQ.PublishData object.

body is the string to be queued.

newQueue is optional for a publishing in a diferent queue in the same exchange. Case it is empty string, will be used queue stored at object.
*/
func (r *RabbitMQ) Publish(body string, newQueue string) (err error) {
	errorFileIdentification := "RabbitMQ.go at Publish()"

	publisher := r.MakeCopy()
	if newQueue != "" {
		publisher.PublishData.QueueName = newQueue
		publisher.PublishData.AccessKey = publisher.PublishData.QueueName
	}

	publisher.semaphore.GetPermission(1)
	defer publisher.semaphore.ReleasePermission(1)
	publisher.Connect()
	defer publisher.Connection.Close()

	err = publisher.PublishData.prepareChannel(publisher)
	if err != nil {
		return errors.New("error preparing channel in " + errorFileIdentification + ": " + err.Error())
	}
	defer publisher.PublishData.Channel.Close()

	err = publisher.PublishData.prepareQueue(publisher)
	if err != nil {
		return errors.New("error preparing queue in " + errorFileIdentification + ": " + err.Error())
	}

	err = publisher.PublishData.Channel.Confirm(false)
	if err != nil {
		return errors.New("error configuring channel with Confirm() protocol in " + errorFileIdentification + ": " + err.Error())
	}

	notifyFlowChannel := publisher.PublishData.Channel.NotifyFlow(make(chan bool))
	notifyAck, notifyNack := publisher.PublishData.Channel.NotifyConfirm(make(chan uint64), make(chan uint64))

	message := amqp.Publishing{ContentType: "application/json", Body: []byte(body), DeliveryMode: amqp.Persistent}

	for {
		select {
		case <-notifyFlowChannel:
			waitingTimeForFlow := 10 * time.Second
			log.Println("Queue '" + publisher.PublishData.QueueName + "' flow is closed, waiting " + waitingTimeForFlow.String() + " seconds to try publish again.")
			time.Sleep(waitingTimeForFlow)
			continue

		default:
			err = publisher.PublishData.Channel.Publish(r.PublishData.ExchangeName, publisher.PublishData.AccessKey, true, false, message)
			if err != nil {
				return errors.New("error publishing message in " + errorFileIdentification + ": " + err.Error())
			}

			select {
			case deniedNack := <-notifyNack:
				waitingTimeForRedelivery := 10 * time.Second
				log.Println("Publishing Nack " + strconv.Itoa(int(deniedNack)) + " denied by '" + publisher.PublishData.QueueName + "' queue, waiting " + waitingTimeForRedelivery.String() + " seconds to try redeliver.")
				time.Sleep(waitingTimeForRedelivery)
				continue

			case successAck := <-notifyAck:
				log.Println("Publishing Ack " + strconv.Itoa(int(successAck)) + " recieved at '" + publisher.PublishData.QueueName + "'.")
				return nil
			}
		}
	}
}
