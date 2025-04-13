package interfaces

import (
	"context"

	"github.com/trinovati/go-message-broker/v3/RabbitMQ/channel"
	"github.com/trinovati/go-message-broker/v3/RabbitMQ/connection"
)

/*
Basic behavior of object that interact with RabbitMQ.
*/
type Behavior interface {
	ShareChannel(Behavior Behavior) Behavior
	ShareConnection(ctx context.Context, Behavior Behavior) Behavior

	Connect(ctx context.Context) Behavior

	CloseChannel(ctx context.Context)
	CloseConnection(ctx context.Context)

	Connection() *connection.RabbitMQConnection
	Channel() *channel.RabbitMQChannel

	PrepareQueue(ctx context.Context, lock bool) error
}
