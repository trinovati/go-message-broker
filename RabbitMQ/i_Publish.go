// this rabbitmq package is adapting the amqp091-go lib
package rabbitmq

import (
	"context"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	dto_pkg "github.com/trinovati/go-message-broker/v3/dto"
)

/*
Publish into RabbitMQ queue configured at RabbitMQPublisher object.
*/
func (publisher *RabbitMQPublisher) Publish(publishing dto_pkg.BrokerPublishing) (err error) {
	var success bool
	var confirmation *amqp.DeferredConfirmation

	message := amqp.Publishing{
		ContentType:     "application/json",
		ContentEncoding: "utf-8",
		DeliveryMode:    amqp.Persistent,
		Headers:         publishing.Header,
		Body:            publishing.Body,
	}

	for {
		publisher.channel.WaitForChannel()

		confirmation, err = publisher.channel.Channel.PublishWithDeferredConfirmWithContext(context.Background(), publisher.Queue.Exchange, publisher.Queue.AccessKey, true, false, message)
		if err != nil {
			if publisher.AlwaysRetry {
				log.Printf("error publishing message at channel %s at queue %s: %s\n", publisher.channel.ChannelId, publisher.Queue.Name, err)
				time.Sleep(time.Second)
				continue
			} else {
				return fmt.Errorf("error publishing message at with retry possible channel %s at queue %s: %w", publisher.channel.ChannelId, publisher.Queue.Name, err)
			}
		}

		success = confirmation.Wait()
		if success {
			log.Printf("SUCCESS publishing from channel id %s with delivery TAG %d at queue %s\n", publisher.channel.ChannelId, confirmation.DeliveryTag, publisher.Queue.Name)
			return nil

		} else {
			log.Printf("FAILED publishing from channel id %s with delivery TAG %d at queue %s\n", publisher.channel.ChannelId, confirmation.DeliveryTag, publisher.Queue.Name)

			if publisher.AlwaysRetry {
				log.Printf("error publishing message from channel %s at queue %s\n", publisher.channel.ChannelId, publisher.Queue.Name)
				time.Sleep(time.Second)
				continue
			} else {
				return fmt.Errorf("error publishing message from channel %s  with retry possible at queue %s", publisher.channel.ChannelId, publisher.Queue.Name)
			}
		}
	}
}
