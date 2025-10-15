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
		Headers:         alignHeaderAsAmqpTable(publishing.Header),
		Body:            publishing.Body,
	}

	publisher.logger.DebugContext(ctx, "starting publish", publisher.logGroup)

	err = publisher.channel.WaitForChannel(ctx, true)
	if err != nil {
		return fmt.Errorf("error publishing from publisher %s at queue %s: %w", publisher.Name, publisher.Queue.Name, err)
	}

	confirmation, err = publisher.channel.Channel.PublishWithDeferredConfirmWithContext(ctx, publisher.Queue.Exchange, publisher.Queue.AccessKey, true, false, message)
	if err != nil {
		publisher.logger.ErrorContext(ctx, "error publishing", slog.Any("error", err), publisher.logGroup)

		return fmt.Errorf("error publishing with %w from publisher %s at queue %s: %w", error_broker.ErrRetryPossible, publisher.Name, publisher.Queue.Name, err)
	}

	success = confirmation.Wait()
	if !success {
		return fmt.Errorf("error at publishing confirmation with %w from publisher %s at queue %s", error_broker.ErrRetryPossible, publisher.Name, publisher.Queue.Name)
	}

	publisher.logger.InfoContext(ctx, "success publishing", slog.Uint64("publish_tag", confirmation.DeliveryTag), publisher.logGroup)

	return nil
}

func alignHeaderAsAmqpTable(header map[string]any) (table amqp.Table) {
	if len(header) == 0 {
		return nil
	}

	table = make(amqp.Table, len(header))
	for key, value := range header {
		switch field := value.(type) {
		case map[string]any:
			table[key] = alignHeaderAsAmqpTable(field)
		default:
			table[key] = value
		}
	}

	return table
}
