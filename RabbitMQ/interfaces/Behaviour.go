package interfaces

type Behaviour interface {
	Behaviour() string

	ShareChannel(behaviour Behaviour) Behaviour
	ShareConnection(behaviour Behaviour) Behaviour
	Connect() Behaviour
	CloseChannel()
	CloseConnection()

	Connection() Connection
	Channel() Channel

	WithConnectionData(host string, port string, username string, password string) Behaviour
	PrepareQueue(gobTarget []byte) error
}
