package interfaces

import amqp "github.com/rabbitmq/amqp091-go"

type Channel interface {
	Id() uint64
	Access() *amqp.Channel
	Connection() Connection

	SetConnection(Connection) Channel

	Connect() Channel
	CloseChannel()
	CloseConnection()

	IsChannelDown() bool
	WaitForChannel()
	WithConnectionData(host string, port string, username string, password string) Channel
}
