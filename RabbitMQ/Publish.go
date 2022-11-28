package rabbitmq

import (
	"context"
	"errors"
	"log"
	"strconv"

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

	log.Println("1  ** LOCKING CHANNEL")
	r.Channel.semaphore.Lock()
	isConnectionDown := r.isConnectionDown()
	if isConnectionDown {
		compelteError := "***ERROR*** in " + errorFileIdentification + ": connection is down"
		log.Println(compelteError)
		log.Println("1,5  ** LOCKING CHANNEL")
		r.Connection.semaphore.Lock()
	}

	log.Println("2  ** PREPARING PUBLISHER")
	notifyFlowChannel := NewRabbitMQ().SharesChannelWith(r).PopulatePublish(exchangeName, r.PublishData.ExchangeType, queueName, queueName).preparePublisher()

	select {
	case <-notifyFlowChannel:
		log.Println("3  ** UNLOCKING AT FLOW")
		r.amqpChannelUnlock(isConnectionDown)
		compelteError := "in " + errorFileIdentification + ": queue '" + queueName + "' flow is closed"
		return errors.New(compelteError)

	default:
		confirmation, err := r.Channel.Channel.PublishWithDeferredConfirmWithContext(context.Background(), exchangeName, queueAccessKey, true, false, message)
		if err != nil {
			log.Println("3  ** UNLOCKING AT PUBLISH ERROR")
			r.amqpChannelUnlock(isConnectionDown)
			compelteError := "error publishing message in " + errorFileIdentification + ": " + err.Error()
			return errors.New(compelteError)
		}

		success := confirmation.Wait()
		if success {
			confirmation.Confirm(true)
			log.Println("3  ** UNLOCKING AT SUCESS")
			r.amqpChannelUnlock(isConnectionDown)
			log.Println("SUCCESS publishing  on queue '" + queueName + "' with delivery TAG '" + strconv.FormatUint(confirmation.DeliveryTag, 10) + "'.")
			return nil

		} else {
			confirmation.Confirm(false)
			log.Println("3  ** UNLOCKING AT FAILURE")
			r.amqpChannelUnlock(isConnectionDown)
			log.Println("FAILED publishing on queue '" + queueName + "' with delivery TAG '" + strconv.FormatUint(confirmation.DeliveryTag, 10) + "'.")
			return nil
		}
	}
}
