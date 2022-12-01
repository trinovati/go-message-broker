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

queue and exchange are optional for a publishing in a diferent location than objects holds. Case it is empty string, will be used the location stored at object.
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

	r.PublishData.Channel.WaitForChannel()
	notifyFlowChannel := r.PublishData.Channel.Channel.NotifyFlow(make(chan bool))

	for {
		select {
		case <-notifyFlowChannel:
			log.Println("***ERROR*** in " + errorFileIdentification + ": queue '" + queueName + "' flow is closed")
			time.Sleep(time.Second)
			continue

		default:
			r.PublishData.Channel.WaitForChannel()
			confirmation, err := r.PublishData.Channel.Channel.PublishWithDeferredConfirmWithContext(context.Background(), exchangeName, queueAccessKey, true, false, message)
			if err != nil {
				log.Println("error publishing message in " + errorFileIdentification + ": " + err.Error())
				time.Sleep(time.Second)
				continue
			}

			success := confirmation.Wait()
			if success {
				confirmation.Confirm(true)
				log.Println("SUCCESS publishing on queue '" + queueName + "' with delivery TAG '" + strconv.FormatUint(confirmation.DeliveryTag, 10) + "'.")
				return nil

			} else {
				confirmation.Confirm(false)
				log.Println("FAILED publishing on queue '" + queueName + "' with delivery TAG '" + strconv.FormatUint(confirmation.DeliveryTag, 10) + "'.")
				time.Sleep(time.Second)
				continue
			}
		}
	}
}
