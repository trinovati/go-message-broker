package rabbitmq

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	dto_pkg "github.com/trinovati/go-message-broker/v3/dto"
)

/*
Publish into RabbitMQ queue configured at RabbitMQPublisher object.
*/
func (publisher *RabbitMQPublisher) Publish(ctx context.Context, publishing dto_pkg.BrokerPublishing) (err error) {
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
		publisher.channel.WaitForChannel(ctx)

		confirmation, err = publisher.channel.Channel.PublishWithDeferredConfirmWithContext(context.Background(), publisher.Queue.Exchange, publisher.Queue.AccessKey, true, false, message)
		if err != nil {
			if publisher.AlwaysRetry {
				publisher.logger.ErrorContext(ctx, "error publishing", slog.Any("error", err), publisher.logGroup)
				time.Sleep(time.Second)
				continue
			} else {
				return fmt.Errorf("error publishing with %s from publisher %s at channel id %s and connection id %s at queue %s: %w", "retry possible", publisher.Name, publisher.channel.ChannelId, publisher.channel.Connection().ConnectionId, publisher.Queue.Name, err)
			}
		}

		success = confirmation.Wait()
		if success {
			publisher.logger.InfoContext(ctx, "success publishing", slog.Uint64("publish_tag", confirmation.DeliveryTag), publisher.logGroup)
			return nil

		} else {
			publisher.logger.WarnContext(ctx, "failed publishing confirmation", slog.Uint64("publish_tag", confirmation.DeliveryTag), publisher.logGroup)

			if publisher.AlwaysRetry {
				publisher.logger.ErrorContext(ctx, "failed publishing confirmation with retry", publisher.logGroup)
				time.Sleep(time.Second)
				continue
			} else {	
				return fmt.Errorf("error at publishing confirmation with %s from publisher %s at channel id %s and connection id %s at queue %s", "retry possible", publisher.Name, publisher.channel.ChannelId, publisher.channel.Connection().ConnectionId, publisher.Queue.Name)
			}
		}
	}
}
