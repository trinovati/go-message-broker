package rabbitmq

import (
	"context"
	"errors"
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
func (p *Publisher) Publish(body string, exchange string, queue string) (err error) {
	errorFileIdentification := "RabbitMQ.go at Publish()"

	exchangeName := p.ExchangeName
	queueName := p.QueueName
	if exchange != "" {
		exchangeName = exchange
	}
	if queue != "" {
		queueName = queue

	}
	queueAccessKey := queueName

	message := amqp.Publishing{ContentType: "application/json", Body: []byte(body), DeliveryMode: amqp.Persistent}

	p.Channel.WaitForChannel()
	notifyFlowChannel := p.Channel.Channel.NotifyFlow(make(chan bool))

	for {
		select {
		case flowNotify := <-notifyFlowChannel:
			if flowNotify {
				log.Println("***ERROR*** NOTIFY FLOW IS TRUE, AND IS BLOCKING THE FLOW!!!")
			}

			close(notifyFlowChannel)
			time.Sleep(time.Second)
			return errors.New("***ERROR*** in " + errorFileIdentification + ": queue '" + queueName + "' flow is closed")

		default:
			p.Channel.WaitForChannel()
			confirmation, err := p.Channel.Channel.PublishWithDeferredConfirmWithContext(context.Background(), exchangeName, queueAccessKey, true, false, message)
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
