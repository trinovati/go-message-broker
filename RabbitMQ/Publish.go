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

newQueue and newExchange are optional for a publishing in a diferent location than objects holds. Case it is empty string, will be used the location stored at object.
*/
func (r *RabbitMQ) Publish(body string, exchange string, queue string) (err error) {
	errorFileIdentification := "RabbitMQ.go at Publish()"

	exchangeName := r.PublishData.ExchangeName
	queueName := r.PublishData.QueueName
	if exchange != "" {
		exchangeName = exchange
	}
	if queue != "" {
		queueName = queue

	}
	queueAccessKey := queueName

	message := amqp.Publishing{ContentType: "application/json", Body: []byte(body), DeliveryMode: amqp.Persistent}

	for {
		r.Channel.semaphore.Lock()
		isConnectionDown := r.isConnectionDown()
		if isConnectionDown {
			compelteError := "***ERROR*** in " + errorFileIdentification + ": connection is down"
			log.Println(compelteError)
			r.Connection.semaphore.Lock()
		}

		notifyFlowChannel := NewRabbitMQ().SharesChannelWith(r).PopulatePublish(exchangeName, r.PublishData.ExchangeType, queueName, queueName).preparePublisher()

		select {
		case <-notifyFlowChannel:
			waitingTimeForFlow := 10 * time.Second
			log.Println("Queue '" + queueName + "' flow is closed, waiting " + waitingTimeForFlow.String() + " seconds to try publish again.")
			r.amqpChannelUnlock(isConnectionDown)
			time.Sleep(waitingTimeForFlow)
			continue

		default:
			confirmation, err := r.Channel.Channel.PublishWithDeferredConfirmWithContext(context.Background(), exchangeName, queueAccessKey, true, false, message)
			if err != nil {
				compelteError := "***ERROR*** error publishing message in " + errorFileIdentification + ": " + err.Error()
				log.Println(compelteError)
				r.amqpChannelUnlock(isConnectionDown)
				time.Sleep(time.Second)
				continue
			}

			success := confirmation.Wait()
			if success {
				log.Println("SUCCESS publishing  on queue '" + queueName + "' with delivery TAG '" + strconv.FormatUint(confirmation.DeliveryTag, 10) + "'.")
				confirmation.Confirm(true)
				r.amqpChannelUnlock(isConnectionDown)
				return nil

			} else {
				log.Println("FAILED publishing on queue '" + queueName + "' with delivery TAG '" + strconv.FormatUint(confirmation.DeliveryTag, 10) + "'.")
				r.amqpChannelUnlock(isConnectionDown)
				time.Sleep(time.Second)
				continue
			}
		}
	}
}
