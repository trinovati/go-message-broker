package interfaces

import (
	channel "gitlab.com/aplicacao/trinovati-connector-message-brokers/RabbitMQ/channel"
	connection "gitlab.com/aplicacao/trinovati-connector-message-brokers/RabbitMQ/connection"
	"gitlab.com/aplicacao/trinovati-connector-message-brokers/RabbitMQ/dto"
)

type Behaviour interface {
	/*
		Change the address the service will try to connect.
	*/
	WithConnectionString(connetionString string) Behaviour

	GetConnection() *connection.ConnectionData

	CloseConnection()

	/*
		Will make both objects share the same connection information for assincronus access.
	*/
	SharesConnectionWith(behaviour Behaviour) Behaviour

	CreateChannel() Behaviour

	GetChannel() *channel.ChannelData

	/*
		Will make both objects share the same channel information for assincronus access.
	*/
	SharesChannelWith(behaviour Behaviour) Behaviour

	GetPopulatedDataFrom(behaviour Behaviour) Behaviour

	BehaviourToBehaviourDto() *dto.BehaviourDto

	Populate(behaviourDto *dto.BehaviourDto) Behaviour

	GetBehaviourType() string
}
