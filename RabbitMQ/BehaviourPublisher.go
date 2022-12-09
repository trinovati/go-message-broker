package rabbitmq

import (
	"log"
	"time"

	channel "gitlab.com/aplicacao/trinovati-connector-message-brokers/RabbitMQ/channel"
	"gitlab.com/aplicacao/trinovati-connector-message-brokers/RabbitMQ/config"
	connection "gitlab.com/aplicacao/trinovati-connector-message-brokers/RabbitMQ/connection"
	"gitlab.com/aplicacao/trinovati-connector-message-brokers/RabbitMQ/dto"
	"gitlab.com/aplicacao/trinovati-connector-message-brokers/RabbitMQ/interfaces"
)

/*
Object that holds all information needed for publishing into a RabbitMQ queue.
*/
type Publisher struct {
	BehaviourType     string
	Channel           *channel.ChannelData
	notifyFlowChannel *chan bool
	ExchangeName      string
	ExchangeType      string
	QueueName         string
	AccessKey         string
}

/*
Builds a new object that holds all information needed for publishing into a RabbitMQ queue.
*/
func NewPublisher() *Publisher {
	return &Publisher{
		BehaviourType:     config.RABBITMQ_PUBLISHER_BEHAVIOUR,
		Channel:           channel.NewChannelData(),
		notifyFlowChannel: nil,
	}
}

/*
Change the address the service will try to connect.
*/
func (publisher *Publisher) WithConnectionString(connectionString string) interfaces.Behaviour {
	publisher.Channel.Connection.SetConnectionString(connectionString)

	return publisher
}

func (publisher *Publisher) GetConnection() *connection.ConnectionData {
	return publisher.Channel.Connection
}

/*
Will make both objects share the same connection information for assincronus access.
*/
func (publisher *Publisher) SharesConnectionWith(behaviour interfaces.Behaviour) interfaces.Behaviour {
	publisher.Channel.Connection = behaviour.GetConnection()

	return publisher
}

func (publisher *Publisher) CloseConnection() {
	publisher.Channel.Connection.CloseConnection()
}

func (publisher *Publisher) CreateChannel() interfaces.Behaviour {
	publisher.Channel.CreateChannel()

	return publisher
}

/*
Will make both objects share the same channel and connection information for assincronus access.
*/
func (publisher *Publisher) SharesChannelWith(behaviour interfaces.Behaviour) interfaces.Behaviour {
	publisher.Channel = behaviour.GetChannel()

	return publisher
}

func (publisher *Publisher) GetChannel() *channel.ChannelData {
	return publisher.Channel
}

func (publisher *Publisher) CloseChannel() {
	publisher.Channel.CloseChannel()
}

/*
Populate the object for a publish behaviour.
*/
func (publisher *Publisher) Populate(behaviourDto *dto.BehaviourDto) interfaces.Behaviour {
	publisher.ExchangeName = behaviourDto.ExchangeName
	publisher.ExchangeType = behaviourDto.ExchangeType
	publisher.QueueName = behaviourDto.QueueName
	publisher.AccessKey = behaviourDto.AccessKey

	return publisher
}

/*
Will copy data not linked to connection or channels to the object. It have the same effect as PopulatePublish().

CAUTION:
Keep in mind that if a behaviour other than publisher is passed, there will be no changes.
*/
func (publisher *Publisher) GetPopulatedDataFrom(behaviour interfaces.Behaviour) interfaces.Behaviour {
	if publisher.BehaviourType != behaviour.GetBehaviourType() {
		return publisher
	}

	publisher.Populate(behaviour.BehaviourToBehaviourDto())

	return publisher
}

func (publisher *Publisher) BehaviourToBehaviourDto() *dto.BehaviourDto {
	behaviourDto := dto.NewBehaviourDto()
	behaviourDto.FillPublisherData(publisher.ExchangeName, publisher.ExchangeType, publisher.QueueName, publisher.AccessKey)

	return behaviourDto
}

func (publisher *Publisher) GetBehaviourType() string {
	return publisher.BehaviourType
}

/*
Prepare a queue linked to RabbitMQ channel for publishing.

In case of unexistent exchange, it will create the exchange.

In case of unexistent queue, it will create the queue.

In case of queue not beeing binded to any exchange, it will bind it to a exchange.
*/
func (publisher *Publisher) PreparePublishQueue() {
	errorFileIdentification := "RabbitMQ.go at PreparePublishQueue()"

	for {
		publisher.Channel.WaitForChannel()

		err := publisher.Channel.Channel.ExchangeDeclare(publisher.ExchangeName, publisher.ExchangeType, true, false, false, false, nil)
		if err != nil {
			log.Println("***ERROR*** error creating RabbitMQ exchange in " + errorFileIdentification + ": " + err.Error())
			time.Sleep(time.Second)
			continue
		}

		queue, err := publisher.Channel.Channel.QueueDeclare(publisher.QueueName, true, false, false, false, nil)
		if err != nil {
			log.Println("***ERROR*** error creating queue in " + errorFileIdentification + ": " + err.Error())
			time.Sleep(time.Second)
			continue
		}

		if queue.Name != publisher.QueueName {
			log.Println("***ERROR***v in " + errorFileIdentification + ": created queue name '" + queue.Name + "' and expected queue name '" + publisher.QueueName + "' are diferent")
			time.Sleep(time.Second)
			continue
		}

		err = publisher.Channel.Channel.QueueBind(publisher.QueueName, publisher.AccessKey, publisher.ExchangeName, false, nil)
		if err != nil {
			log.Println("***ERROR*** error binding queue in " + errorFileIdentification + ": " + err.Error())
			time.Sleep(time.Second)
			continue
		}

		return
	}
}
