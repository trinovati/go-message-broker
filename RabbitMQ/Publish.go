package rabbitmq

import (
	"log"
	"strconv"
	"time"

	"github.com/streadway/amqp"
)

/*
Publish data to the queue linked to RabbitMQ.PublishData object.

body is the string to be queued.

newQueue is optional for a publishing in a diferent queue in the same exchange. Case it is empty string, will be used the queue stored at object.
*/
func (r *RabbitMQ) Publish(body string, newQueue string) (err error) {
	errorFileIdentification := "RabbitMQ.go at Publish()"

	publisher := r.MakeCopy()
	if newQueue != "" {
		publisher.PublishData.QueueName = newQueue
		publisher.PublishData.AccessKey = publisher.PublishData.QueueName
	}

	message := amqp.Publishing{ContentType: "application/json", Body: []byte(body), DeliveryMode: amqp.Persistent}

	for {
		notifyFlowChannel, notifyAck, notifyNack, closeNotifyChannel, err := publisher.PublishData.preparePublisher(r)
		if err != nil {
			compelteError := "***ERROR*** Publishing stopped on queue '" + r.PublishData.QueueName + "' due to error preparing publisher in " + errorFileIdentification + ": " + err.Error()
			log.Println(compelteError)
			time.Sleep(2 * time.Second)
			continue
		}
		defer publisher.PublishData.Channel.Close()

		select {
		case <-notifyFlowChannel:
			waitingTimeForFlow := 10 * time.Second
			log.Println("Queue '" + publisher.PublishData.QueueName + "' flow is closed, waiting " + waitingTimeForFlow.String() + " seconds to try publish again.")
			time.Sleep(waitingTimeForFlow)
			continue

		case closeNotification := <-closeNotifyChannel:
			compelteError := "***ERROR*** in " + errorFileIdentification + ": publishing stopped on queue '" + r.PublishData.QueueName + "' due to closure of channel with reason '" + closeNotification.Reason + "'"
			log.Println(compelteError)
			time.Sleep(2 * time.Second)
			continue

		default:
			err = publisher.PublishData.Channel.Publish(r.PublishData.ExchangeName, publisher.PublishData.AccessKey, true, false, message)
			if err != nil {
				compelteError := "***ERROR*** error publishing message in " + errorFileIdentification + ": " + err.Error()
				log.Println(compelteError)
				time.Sleep(2 * time.Second)
				continue
			}

			select {
			case closeNotification := <-closeNotifyChannel:
				compelteError := "***ERROR*** in " + errorFileIdentification + ": publishing stopped on queue '" + r.PublishData.QueueName + "' due to closure of channel with reason '" + closeNotification.Reason + "'"
				log.Println(compelteError)
				time.Sleep(2 * time.Second)
				continue

			case deniedNack := <-notifyNack:
				waitingTimeForRedelivery := 10 * time.Second
				log.Println("Publishing denied by RabbitMQ server at queue '" + publisher.PublishData.QueueName + "' with Nack '" + strconv.Itoa(int(deniedNack)) + "', waiting " + waitingTimeForRedelivery.String() + " seconds to try redeliver.")
				time.Sleep(waitingTimeForRedelivery)
				continue

			case successAck := <-notifyAck:
				log.Println("Publishing success on queue '" + publisher.PublishData.QueueName + "' with Ack '" + strconv.Itoa(int(successAck)) + "'.")
				return nil
			}
		}
	}
}
