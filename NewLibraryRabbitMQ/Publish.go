package rabbitmq

import (
	"context"
	"log"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

/*
Publish data to the queue linked to RabbitMQ.PublishData object.

body is the string to be queued.

newQueue is optional for a publishing in a diferent queue in the same exchange. Case it is empty string, will be used the queue stored at object.
*/
func (r *RabbitMQ) Publish(body string, newQueue string) (err error) {
	errorFileIdentification := "RabbitMQ.go at Publish()"

	var publisher *RabbitMQ

	if newQueue == "" {
		publisher = r

	} else {
		publisher = NewRabbitMQ().SharesChannelWith(r).GetPopulatedDataFrom(r)
		publisher.PublishData.QueueName = newQueue
		publisher.PublishData.AccessKey = newQueue
	}

	message := amqp.Publishing{ContentType: "application/json", Body: []byte(body), DeliveryMode: amqp.Persistent}

	for {
		err := publisher.PublishData.preparePublisher(r)
		if err != nil {
			compelteError := "***ERROR*** Publishing stopped on queue '" + r.PublishData.QueueName + "' due to error preparing publisher in " + errorFileIdentification + ": " + err.Error()
			log.Println(compelteError)
			time.Sleep(2 * time.Second)
			continue
		}

		select {
		case <-*r.PublishData.notifyFlowChannel:
			waitingTimeForFlow := 10 * time.Second
			log.Println("Queue '" + publisher.PublishData.QueueName + "' flow is closed, waiting " + waitingTimeForFlow.String() + " seconds to try publish again.")
			time.Sleep(waitingTimeForFlow)
			continue

		default:
			confirmation, err := publisher.PublishData.Channel.PublishWithDeferredConfirmWithContext(context.Background(), r.PublishData.ExchangeName, publisher.PublishData.AccessKey, true, false, message)
			if err != nil {
				compelteError := "***ERROR*** error publishing message in " + errorFileIdentification + ": " + err.Error()
				log.Println(compelteError)
				time.Sleep(2 * time.Second)
				continue
			}

			seccess := confirmation.Wait()
			if seccess {
				log.Println("Publishing success on queue '" + publisher.PublishData.QueueName + "' with delivery TAG '" + strconv.FormatUint(confirmation.DeliveryTag, 10) + "'.")
				return nil
			}
		}
	}
}
