package interfaces

type Connection interface {
	Id() uint64
	Connect() Connection
	CloseConnection()

	IsConnectionDown() bool
	WaitForConnection()
	WithConnectionData(host string, port string, username string, password string) Connection
}
