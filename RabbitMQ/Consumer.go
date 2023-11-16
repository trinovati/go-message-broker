package rabbitmq

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"sync"

	"gitlab.com/aplicacao/trinovati-connector-message-brokers/v2/RabbitMQ/channel"
	"gitlab.com/aplicacao/trinovati-connector-message-brokers/v2/RabbitMQ/config"
	"gitlab.com/aplicacao/trinovati-connector-message-brokers/v2/RabbitMQ/interfaces"
	"gitlab.com/aplicacao/trinovati-connector-message-brokers/v2/dto"
)

/*
Object that holds all information needed for consuming from RabbitMQ queue.
*/
type Consumer struct {
	channel            interfaces.Channel
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

func NewConsumer(
	name string,
	Publisher interfaces.Publisher,
	exchangeName string,
	exchangeType string,
	queueName string,
	accessKey string,
	qos int,
	purge bool,
) *Consumer {
	return &Consumer{
		Publisher:          Publisher,
		channel:            channel.NewChannel(name),
		DeliveryChannel:    make(chan []byte),
		DeliveryMap:        &sync.Map{},
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

func (consumer *Consumer) Deliveries() (gobMessageChannel chan []byte) {
	return consumer.DeliveryChannel
}

func (consumer *Consumer) ShareChannel(behaviour interfaces.Behaviour) interfaces.Behaviour {
	consumer.channel = behaviour.ChannelOf(consumer.Behaviour())

	consumer.Publisher.ShareChannel(behaviour)

	return consumer
}

func (consumer *Consumer) ShareConnection(behaviour interfaces.Behaviour) interfaces.Behaviour {
	consumer.channel.SetConnection(behaviour.ConnectionOf(consumer.Behaviour()))

	consumer.Publisher.ShareConnection(behaviour)

	return consumer
}

func (consumer *Consumer) Connect() interfaces.Behaviour {
	if consumer.channel.IsChannelDown() {
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

func (consumer Consumer) ConnectionOf(behaviour string) interfaces.Connection {
	var connection interfaces.Connection = nil

	if consumer.Behaviour() == behaviour {
		connection = consumer.channel.Connection()
	} else if consumer.Publisher.Behaviour() == behaviour {
		connection = consumer.Publisher.ConnectionOf(behaviour)
	}

	return connection
}

func (consumer Consumer) Connection() interfaces.Connection {
	return consumer.channel.Connection()
}

func (consumer Consumer) ChannelOf(behaviour string) interfaces.Channel {
	var channel interfaces.Channel = nil

	if consumer.Behaviour() == behaviour {
		channel = consumer.channel
	} else if consumer.Publisher.Behaviour() == behaviour {
		channel = consumer.Publisher.ChannelOf(behaviour)
	}

	return channel
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
			return config.Error.Wrap(err, fmt.Sprintf("error writing to buffer at channel '%s'", consumer.channel.Name()))
		}

		err = gob.NewDecoder(&buffer).Decode(&target)
		if err != nil {
			return config.Error.Wrap(err, fmt.Sprintf("error decoding gob at channel '%s'", consumer.channel.Name()))
		}
	} else {
		target = dto.Target{
			Exchange:     consumer.ExchangeName,
			ExchangeType: consumer.ExchangeType,
			Queue:        consumer.QueueName,
			AccessKey:    consumer.AccessKey,
			Qos:          consumer.Qos,
			Purge:        consumer.Purge,
		}
	}

	if consumer.channel.IsChannelDown() {
		return config.Error.New(fmt.Sprintf("channel dropped before declaring exchange at channel '%s'", consumer.channel.Name()))
	}

	err = consumer.channel.Access().ExchangeDeclare(target.Exchange, target.ExchangeType, true, false, false, false, nil)
	if err != nil {
		return config.Error.Wrap(err, fmt.Sprintf("error creating RabbitMQ exchange at channel '%s'", consumer.channel.Name()))
	}

	if consumer.channel.IsChannelDown() {
		return config.Error.New(fmt.Sprintf("channel dropped before declaring queue at channel '%s'", consumer.channel.Name()))
	}

	_, err = consumer.channel.Access().QueueDeclare(target.Queue, true, false, false, false, nil)
	if err != nil {
		return config.Error.Wrap(err, fmt.Sprintf("error creating queue at channel '%s'", consumer.channel.Name()))
	}

	if consumer.channel.IsChannelDown() {
		return config.Error.New(fmt.Sprintf("channel dropped before queue binding at channel '%s'", consumer.channel.Name()))
	}

	err = consumer.channel.Access().QueueBind(target.Queue, target.AccessKey, target.Exchange, false, nil)
	if err != nil {
		return config.Error.Wrap(err, fmt.Sprintf("error binding queue at channel '%s'", consumer.channel.Name()))
	}

	err = consumer.channel.Access().Qos(target.Qos, 0, false)
	if err != nil {
		return config.Error.Wrap(err, fmt.Sprintf("error qos a channel to limiting the maximum message queue can hold at channel '%s'", consumer.channel.Name()))
	}

	if target.Purge {
		if consumer.channel.IsChannelDown() {
			return config.Error.New(fmt.Sprintf("channel dropped before purging queue at channel '%s'", consumer.channel.Name()))
		}

		_, err = consumer.channel.Access().QueuePurge(target.Queue, true)
		if err != nil {
			return config.Error.Wrap(err, fmt.Sprintf("error purging queue '%s' at channel '%s'", target.Queue, consumer.channel.Name()))
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
		return config.Error.Wrap(err, fmt.Sprintf("error encoding gob at channel '%s'", consumer.channel.Name()))
	}

	err = consumer.Publisher.PrepareQueue(buffer.Bytes())
	if err != nil {
		return config.Error.Wrap(err, fmt.Sprintf("error preparing failed messages queue at channel '%s'", consumer.channel.Name()))
	}

	return nil
}
