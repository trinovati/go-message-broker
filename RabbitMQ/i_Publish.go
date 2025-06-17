// this rabbitmq package is adapting the amqp091-go lib.
package rabbitmq

import (
	"context"
	"fmt"
	"log/slog"

	dto_broker "github.com/trinovati/go-message-broker/v3/pkg/dto"
	error_broker "github.com/trinovati/go-message-broker/v3/pkg/error"

	amqp "github.com/rabbitmq/amqp091-go"
)

/*
Publish into RabbitMQ queue configured at RabbitMQPublisher object.
*/
func (publisher *RabbitMQPublisher) Publish(ctx context.Context, publishing dto_broker.BrokerPublishing) (err error) {
	var success bool
	var confirmation *amqp.DeferredConfirmation

	message := amqp.Publishing{
		ContentType:     "application/json",
		ContentEncoding: "utf-8",
		DeliveryMode:    amqp.Persistent,
		Headers:         publishing.Header,
		Body:            publishing.Body,
	}

	publisher.logger.DebugContext(ctx, "starting publish", publisher.logGroup)

	err = publisher.channel.WaitForChannel(ctx, true)
	if err != nil {
		return fmt.Errorf("error publishing from publisher %s at channel id %s and connection id %s at queue %s: %w", publisher.Name, publisher.channel.Id, publisher.channel.Connection().Id, publisher.Queue.Name, err)
	}

	confirmation, err = publisher.channel.Channel.PublishWithDeferredConfirmWithContext(context.Background(), publisher.Queue.Exchange, publisher.Queue.AccessKey, true, false, message)
	if err != nil {
		publisher.logger.ErrorContext(ctx, "error publishing", slog.Any("error", err), publisher.logGroup)

		return fmt.Errorf("error publishing with %w from publisher %s at channel id %s and connection id %s at queue %s: %w", error_broker.ErrRetryPossible, publisher.Name, publisher.channel.Id, publisher.channel.Connection().Id, publisher.Queue.Name, err)
	}

	success = confirmation.Wait()
	if !success {
		publisher.logger.ErrorContext(ctx, "failed publishing confirmation", slog.Uint64("publish_tag", confirmation.DeliveryTag), publisher.logGroup)

		return fmt.Errorf("error at publishing confirmation with %w from publisher %s at channel id %s and connection id %s at queue %s", error_broker.ErrRetryPossible, publisher.Name, publisher.channel.Id, publisher.channel.Connection().Id, publisher.Queue.Name)
	}

	publisher.logger.InfoContext(ctx, "success publishing", slog.Uint64("publish_tag", confirmation.DeliveryTag), publisher.logGroup)

	return nil
}
