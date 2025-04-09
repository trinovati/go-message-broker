package interfaces

import (
	"github.com/trinovati/go-message-broker/v3/RabbitMQ/channel"
	"github.com/trinovati/go-message-broker/v3/RabbitMQ/connection"
)

/*
Basic behavior of object that interact with RabbitMQ.
*/
type Behavior interface {
	ShareChannel(Behavior Behavior) Behavior
	ShareConnection(Behavior Behavior) Behavior

	Connect() Behavior

	CloseChannel()
	CloseConnection()

	Connection() *connection.RabbitMQConnection
	Channel() *channel.RabbitMQChannel

	PrepareQueue() error
}
