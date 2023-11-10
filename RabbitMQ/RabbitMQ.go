package rabbitmq

import (
	"fmt"

	"gitlab.com/aplicacao/trinovati-connector-message-brokers/v2/RabbitMQ/config"
	"gitlab.com/aplicacao/trinovati-connector-message-brokers/v2/RabbitMQ/errors"
	"gitlab.com/aplicacao/trinovati-connector-message-brokers/v2/RabbitMQ/interfaces"

	amqp "github.com/rabbitmq/amqp091-go"
)

/*
Object containing methods to prepare a consumer or publisher to RabbitMQ service, operating as client, RPC client or RPC server.
*/
type RabbitMQ struct {
	Publisher interfaces.Publisher
	Consumer  interfaces.Consumer
}

/*
Build an object containing methods to prepare a Consumer or Publisher to RabbitMQ service.

Its highly recomended a single object per server, as Consumer behaviour uses the Publisher to report any problem to the server.
*/
func NewRabbitMQ() *RabbitMQ {
	return &RabbitMQ{
		Publisher: nil,
		Consumer:  nil,
	}
}

/*
Populate the adequate RabbitMQ behaviour with the argument.

Keep in mind that the Consumer behaviour will use the Publisher object to send any report to the server.
*/
func (rmq *RabbitMQ) Behave(behaviour interfaces.Behaviour) *RabbitMQ {
	switch behaviour.Behaviour() {
	case config.PUBLISHER:
		publisher, ok := behaviour.(interfaces.Publisher)
		if !ok {
			config.Error.New(fmt.Sprintf("type %T cannot be parsed into Publisher", behaviour)).Print()
			return rmq
		}

		if rmq.Publisher != nil {
			publisher.ShareConnection(rmq.Publisher)
			rmq.Publisher.CloseChannel()
		}

		rmq.Publisher = publisher

		if rmq.Consumer != nil {
			rmq.Consumer.SetPublisher(rmq.Publisher)
		}

	case config.CONSUMER:
		consumer, ok := behaviour.(interfaces.Consumer)
		if !ok {
			config.Error.New(fmt.Sprintf("type %T cannot be parsed into Publisher", behaviour)).Print()
			return rmq
		}

		if rmq.Consumer != nil {
			consumer.ShareConnection(rmq.Consumer)
			rmq.Consumer.CloseChannel()
		}

		rmq.Consumer = consumer

		if rmq.Publisher == nil {
			rmq.Behave(
				NewPublisher(
					rmq.Consumer.Channel().Name()+"_acknowledger",
					"",
					"",
					"",
					"",
				),
			)
		}

		rmq.Consumer.SetPublisher(rmq.Publisher)
	}

	return rmq
}

func (rmq *RabbitMQ) DeliveryChannel() (gobMessageChannel chan []byte) {
	if rmq.Consumer == nil {
		return nil
	}

	return rmq.Consumer.Deliveries()
}

/*
Change the address that the object will try to connect.

It will change both, Consumer and Publisher connections.
*/
func (rmq *RabbitMQ) WithConnectionData(host string, port string, username string, password string) *RabbitMQ {
	if rmq.Consumer != nil {
		rmq.Consumer.WithConnectionData(host, port, username, password)
	}

	if rmq.Publisher != nil {
		rmq.Publisher.WithConnectionData(host, port, username, password)
	}

	return rmq
}

/*
Will close all connections related to the object.
*/
func (rmq *RabbitMQ) CloseConnection() {
	if rmq.Publisher != nil && rmq.Publisher.Channel() != nil && rmq.Publisher.Channel().Connection() != nil {
		rmq.Publisher.Channel().CloseConnection()
	}

	if rmq.Consumer != nil && rmq.Consumer.Channel() != nil && rmq.Consumer.Channel().Connection() != nil {
		rmq.Consumer.Channel().CloseConnection()
	}
}

/*
Will close all channels related to the object.
*/
func (rmq *RabbitMQ) CloseChannel() {
	if rmq.Publisher != nil && rmq.Publisher.Channel() != nil {
		rmq.Publisher.Channel().CloseChannel()
	}

	if rmq.Consumer != nil && rmq.Consumer.Channel() != nil {
		rmq.Consumer.Channel().CloseChannel()
	}
}

/*
Will connect all sub-objects to the rabbitmq server.
*/
func (rmq *RabbitMQ) Connect() *RabbitMQ {
	if rmq.Publisher != nil {
		rmq.Publisher.Connect()
	}

	if rmq.Consumer != nil {
		rmq.Consumer.Connect()
	}

	return rmq
}

/*
UNSAFE!!!
FOR TEST PURPOSES ONLY!!!

Delete a queue and a exchange.
safePassword asserts that you're sure of it.
*/
func DeleteQueueAndExchange(channel *amqp.Channel, queueName string, exchangeName string, safePassword string) (err error) {
	if safePassword == "doit" {
		_, err = channel.QueueDelete(queueName, false, false, false)
		if err != nil {
			return (&errors.Error{}).New("can't delete queue: " + err.Error())
		}

		err = channel.ExchangeDelete(exchangeName, false, false)
		if err != nil {
			return config.Error.New("can't delete exchange: " + err.Error())
		}

	} else {
		return config.Error.New("can't delete: you seem not sure of it")
	}

	return nil
}
