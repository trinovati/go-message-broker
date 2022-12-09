package rabbitmq

import (
	"errors"

	amqp "github.com/rabbitmq/amqp091-go"
	"gitlab.com/aplicacao/trinovati-connector-message-brokers/RabbitMQ/config"
	"gitlab.com/aplicacao/trinovati-connector-message-brokers/RabbitMQ/interfaces"
)

/*
Object containing methods to prepare a consumer or publisher to RabbitMQ service, operating as client, RPC client or RPC server.
*/
type RabbitMQ struct {
	service           string
	Behaviour         []interfaces.Behaviour
	BehaviourTypeMap  map[int]string
	BehaviourQuantity int
}

/*
Build an object containing methods to prepare a consumer or publisher to RabbitMQ service.

terminateOnConnectionError defines if, at any moment, the connections fail or comes down, the service will panic or retry connection.

By default, the object will try to access the environmental variable RABBITMQ_SERVER for connection purpose, in case of unexistent, it will use 'amqp://guest:guest@localhost:5672/' address.

By default, the object will try to access the environmental variable RABBITMQ_SERVICE for behaviour purpose, in case of unexistent, it will use 'client' behaviour.
*/
func NewRabbitMQ() *RabbitMQ {
	return &RabbitMQ{
		service:           config.RABBITMQ_CLIENT,
		Behaviour:         nil,
		BehaviourTypeMap:  make(map[int]string),
		BehaviourQuantity: 0,
	}
}

func (r *RabbitMQ) AddBehaviour(behaviour interfaces.Behaviour) *RabbitMQ {
	r.Behaviour = append(r.Behaviour, behaviour)

	r.BehaviourTypeMap[len(r.Behaviour)-1] = behaviour.GetBehaviourType()

	r.BehaviourQuantity++

	return r
}

/*
Delete a queue and a exchange, thinked to use at tests.

safePassword asserts that you're sure of it.
*/
func DeleteQueueAndExchange(channel *amqp.Channel, queueName string, exchangeName string, safePassword string) (err error) {
	if safePassword == "doit" {
		_, err = channel.QueueDelete(queueName, false, false, false)
		if err != nil {
			return errors.New("can't delete queue: " + err.Error())
		}

		err = channel.ExchangeDelete(exchangeName, false, false)
		if err != nil {
			return errors.New("can't delete exchange: " + err.Error())
		}

	} else {
		return errors.New("can't delete: you seem not sure of it")
	}

	return nil
}
