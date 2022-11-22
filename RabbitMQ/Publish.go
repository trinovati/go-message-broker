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

	publisher := NewRabbitMQ().SharesChannelWith(r).GetPopulatedDataFrom(r)
	if newQueue != "" {
		publisher.PublishData.QueueName = newQueue
		publisher.PublishData.AccessKey = newQueue
	}

	message := amqp.Publishing{ContentType: "application/json", Body: []byte(body), DeliveryMode: amqp.Persistent}

	for {
		publisher.Channel.semaphore.Lock()
		isConnectionDown := r.isConnectionDown()
		if isConnectionDown {
			publisher.Connection.semaphore.Lock()
		}

		notifyFlowChannel := publisher.preparePublisher()

		select {
		case <-notifyFlowChannel:
			waitingTimeForFlow := 10 * time.Second
			log.Println("Queue '" + publisher.PublishData.QueueName + "' flow is closed, waiting " + waitingTimeForFlow.String() + " seconds to try publish again.")
			r.amqpChannelUnlock(isConnectionDown)
			time.Sleep(waitingTimeForFlow)
			continue

		default:
			confirmation, err := publisher.Channel.Channel.PublishWithDeferredConfirmWithContext(context.Background(), publisher.PublishData.ExchangeName, publisher.PublishData.AccessKey, true, false, message)
			if err != nil {
				compelteError := "***ERROR*** error publishing message in " + errorFileIdentification + ": " + err.Error()
				log.Println(compelteError)
				r.amqpChannelUnlock(isConnectionDown)
				time.Sleep(time.Second)
				continue
			}

			success := confirmation.Wait()
			if success {
				log.Println("Publishing success on queue '" + publisher.PublishData.QueueName + "' with delivery TAG '" + strconv.FormatUint(confirmation.DeliveryTag, 10) + "'.")
				confirmation.Confirm(true)
				r.amqpChannelUnlock(isConnectionDown)
				return nil

			} else {
				log.Println("Publishing confirmation failed on queue '" + publisher.PublishData.QueueName + "' with delivery TAG '" + strconv.FormatUint(confirmation.DeliveryTag, 10) + "'.")
				r.amqpChannelUnlock(isConnectionDown)
				time.Sleep(time.Second)
				continue
			}
		}
	}
}
