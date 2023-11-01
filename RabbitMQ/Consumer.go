package rabbitmq

import (
	"bytes"
	"encoding/gob"
	"sync"

	"gitlab.com/aplicacao/trinovati-connector-message-brokers/RabbitMQ/channel"
	"gitlab.com/aplicacao/trinovati-connector-message-brokers/RabbitMQ/config"
	"gitlab.com/aplicacao/trinovati-connector-message-brokers/RabbitMQ/interfaces"
	"gitlab.com/aplicacao/trinovati-connector-message-brokers/dto"
)

/*
Object that holds all information needed for consuming from RabbitMQ queue.
*/
type Consumer struct {
	channel interfaces.Channel

	Publisher          interfaces.Publisher
	DeliveryChannel    chan []byte
	DeliveryMap        *sync.Map
	ExchangeName       string
	ExchangeType       string
	QueueName          string
	AccessKey          string
	FailedExchange     string
	FailedExchangeType string
	FailedQueue        string
	FailedAccessKey    string
	Qos                int
	Purge              bool
	AlwaysRetry        bool
}

/*
Builds a new object that holds all information needed for consuming from RabbitMQ queue.
*/
func NewConsumer(
	Publisher interfaces.Publisher,
	DeliveryChannel chan []byte,
	DeliveryMap *sync.Map,
	exchangeName string,
	exchangeType string,
	queueName string,
	accessKey string,
	qos int,
	purge bool,
) *Consumer {
	return &Consumer{
		Publisher:          Publisher,
		channel:            channel.NewChannel(),
		DeliveryChannel:    DeliveryChannel,
		DeliveryMap:        DeliveryMap,
		ExchangeName:       exchangeName,
		ExchangeType:       exchangeType,
		QueueName:          queueName,
		AccessKey:          accessKey,
		Qos:                qos,
		Purge:              purge,
		AlwaysRetry:        true,
		FailedExchange:     "failed",
		FailedExchangeType: "direct",
		FailedQueue:        "_" + exchangeName + "__failed_messages",
		FailedAccessKey:    "_" + exchangeName + "__failed_messages",
	}
}

func (consumer Consumer) Behaviour() (behaviour string) {
	return config.CONSUMER
}

func (consumer *Consumer) ShareChannel(behaviour interfaces.Behaviour) interfaces.Behaviour {
	consumer.channel = behaviour.Channel()

	return consumer
}

func (consumer *Consumer) ShareConnection(behaviour interfaces.Behaviour) interfaces.Behaviour {
	consumer.channel.SetConnection(behaviour.Connection())

	return consumer
}

func (consumer *Consumer) Connect() interfaces.Behaviour {
	if consumer.channel.IsChannelDown() || consumer.channel.Connection().IsConnectionDown() {
		consumer.channel.Connect()
	}

	return consumer
}

func (consumer *Consumer) CloseChannel() {
	consumer.channel.CloseChannel()
}

func (consumer *Consumer) CloseConnection() {
	consumer.channel.CloseConnection()
}

func (consumer Consumer) Connection() interfaces.Connection {
	return consumer.channel.Connection()
}

func (consumer Consumer) Channel() interfaces.Channel {
	return consumer.channel
}

func (consumer *Consumer) SetPublisher(publisher interfaces.Publisher) {
	consumer.Publisher = publisher
}

func (consumer *Consumer) WithConnectionData(host string, port string, username string, password string) interfaces.Behaviour {
	consumer.channel.WithConnectionData(host, port, username, password)

	return consumer
}

/*
Prepare a queue linked to RabbitMQ channel for consuming.

In case of unexistent exchange, it will create the exchange.

In case of unexistent queue, it will create the queue.

In case of queue not beeing binded to any exchange, it will bind it to a exchange.

It will set Qos to the channel.

It will purge the queue before consuming case ordered to.
*/
func (consumer *Consumer) PrepareQueue(gobTarget []byte) (err error) {
	var target dto.Target
	var buffer bytes.Buffer

	if gobTarget != nil {
		_, err = buffer.Write(gobTarget)
		if err != nil {
			return config.Error.Wrap(err, "error writing to buffer")
		}

		err = gob.NewDecoder(&buffer).Decode(&target)
		if err != nil {
			return config.Error.Wrap(err, "error decoding gob")
		}
	} else {
		target = dto.Target{
			Exchange:     consumer.ExchangeName,
			ExchangeType: consumer.ExchangeType,
			Queue:        consumer.QueueName,
			AccessKey:    consumer.QueueName,
			Qos:          consumer.Qos,
			Purge:        consumer.Purge,
		}
	}

	if consumer.channel.IsChannelDown() {
		return config.Error.New("channel dropped before declaring exchange")
	}

	err = consumer.channel.Access().ExchangeDeclare(target.Exchange, target.ExchangeType, true, false, false, false, nil)
	if err != nil {
		return config.Error.Wrap(err, "error creating RabbitMQ exchange")
	}

	if consumer.channel.IsChannelDown() {
		return config.Error.New("channel dropped before declaring queue")
	}

	queue, err := consumer.channel.Access().QueueDeclare(target.Queue, true, false, false, false, nil)
	if err != nil {
		return config.Error.Wrap(err, "error creating queue")
	}

	if queue.Name != consumer.QueueName {
		return config.Error.New("created queue name '" + queue.Name + "' and expected queue name '" + target.Queue + "' are diferent")
	}

	if consumer.channel.IsChannelDown() {
		return config.Error.New("channel dropped before queue binding")
	}

	err = consumer.channel.Access().QueueBind(target.Queue, target.AccessKey, target.Exchange, false, nil)
	if err != nil {
		return config.Error.Wrap(err, "error binding queue")
	}

	err = consumer.channel.Access().Qos(target.Qos, 0, false)
	if err != nil {
		return config.Error.Wrap(err, "error qos a channel to limiting the maximum message queue can hold")
	}

	if target.Purge {
		if consumer.channel.IsChannelDown() {
			return config.Error.New("channel dropped before purging queue")
		}

		_, err = consumer.channel.Access().QueuePurge(target.Queue, true)
		if err != nil {
			return config.Error.Wrap(err, "error purging queue '"+target.Queue+"'")
		}
	}

	target = dto.Target{
		Exchange:     consumer.FailedExchange,
		ExchangeType: consumer.FailedExchangeType,
		Queue:        consumer.FailedQueue,
		AccessKey:    consumer.FailedAccessKey,
	}

	err = gob.NewEncoder(&buffer).Encode(&target)
	if err != nil {
		return config.Error.Wrap(err, "error encoding gob")
	}

	err = consumer.Publisher.PrepareQueue(buffer.Bytes())
	if err != nil {
		return config.Error.Wrap(err, "error preparing failed messages queue")
	}

	return nil
}
