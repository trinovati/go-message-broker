package interfaces

import "context"

type Connection interface {
	Id() uint64
	Connect() (Connection, context.Context)
	CloseConnection()

	IsConnectionDown() bool
	WaitForConnection()
	WithConnectionData(host string, port string, username string, password string) Connection
}
