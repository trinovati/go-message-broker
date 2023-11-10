package rabbitmq

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"log"
	"time"

	"gitlab.com/aplicacao/trinovati-connector-message-brokers/v2/RabbitMQ/channel"
	"gitlab.com/aplicacao/trinovati-connector-message-brokers/v2/RabbitMQ/config"
	"gitlab.com/aplicacao/trinovati-connector-message-brokers/v2/RabbitMQ/interfaces"
	"gitlab.com/aplicacao/trinovati-connector-message-brokers/v2/dto"

	amqp "github.com/rabbitmq/amqp091-go"
)

/*
Object that holds all information needed for publishing into a RabbitMQ queue.
*/
type Publisher struct {
	channel           interfaces.Channel
	notifyFlowChannel *chan bool
	ExchangeName      string
	ExchangeType      string
	QueueName         string
	AccessKey         string
	AlwaysRetry       bool
}

func NewPublisher(
	name string,
	exchangeName string,
	exchangeType string,
	queueName string,
	accessKey string,
) *Publisher {
	return &Publisher{
		channel:           channel.NewChannel(name),
		ExchangeName:      exchangeName,
		ExchangeType:      exchangeType,
		QueueName:         queueName,
		AccessKey:         accessKey,
		notifyFlowChannel: nil,
		AlwaysRetry:       false,
	}
}

func (publisher Publisher) Behaviour() (behaviour string) {
	return config.PUBLISHER
}

func (publisher *Publisher) ShareChannel(behaviour interfaces.Behaviour) interfaces.Behaviour {
	publisher.channel = behaviour.ChannelOf(publisher.Behaviour())

	return publisher
}

func (publisher *Publisher) ShareConnection(behaviour interfaces.Behaviour) interfaces.Behaviour {
	publisher.channel.SetConnection(behaviour.ConnectionOf(publisher.Behaviour()))

	return publisher
}

func (publisher *Publisher) Connect() interfaces.Behaviour {
	if publisher.channel.IsChannelDown() {
		publisher.channel.Connect()
	}

	return publisher
}

func (publisher *Publisher) CloseChannel() {
	publisher.channel.CloseChannel()
}

func (publisher *Publisher) CloseConnection() {
	publisher.channel.CloseConnection()
}

func (publisher Publisher) ConnectionOf(behaviour string) interfaces.Connection {
	var connection interfaces.Connection = nil

	if publisher.Behaviour() == behaviour {
		connection = publisher.channel.Connection()
	}

	return connection
}

func (publisher Publisher) Connection() interfaces.Connection {
	return publisher.channel.Connection()
}

func (publisher Publisher) ChannelOf(behaviour string) interfaces.Channel {
	var channel interfaces.Channel = nil

	if publisher.Behaviour() == behaviour {
		channel = publisher.channel
	}

	return channel
}

func (publisher Publisher) Channel() interfaces.Channel {
	return publisher.channel
}

func (publisher *Publisher) WithConnectionData(host string, port string, username string, password string) interfaces.Behaviour {
	publisher.channel.WithConnectionData(host, port, username, password)

	return publisher
}

/*
Prepare a queue linked to RabbitMQ channel for publishing.

In case of unexistent exchange, it will create the exchange.

In case of unexistent queue, it will create the queue.

In case of queue not beeing binded to any exchange, it will bind it to a exchange.
*/
func (publisher *Publisher) PrepareQueue(gobTarget []byte) (err error) {
	var target dto.Target
	var buffer bytes.Buffer
	var tolerance int

	if gobTarget != nil {
		_, err = buffer.Write(gobTarget)
		if err != nil {
			return config.Error.Wrap(err, fmt.Sprintf("error writing to buffer at channel '%s'", publisher.channel.Name()))
		}

		err = gob.NewDecoder(&buffer).Decode(&target)
		if err != nil {
			return config.Error.Wrap(err, fmt.Sprintf("error decoding gob at channel '%s'", publisher.channel.Name()))
		}
	} else {
		target = dto.Target{
			Exchange:     publisher.ExchangeName,
			ExchangeType: publisher.ExchangeType,
			Queue:        publisher.QueueName,
			AccessKey:    publisher.QueueName,
		}
	}

	for tolerance = 0; publisher.AlwaysRetry || tolerance <= 5; tolerance++ {
		publisher.channel.WaitForChannel()

		err = publisher.channel.Access().ExchangeDeclare(target.Exchange, target.ExchangeType, true, false, false, false, nil)
		if err != nil {
			config.Error.Wrap(err, fmt.Sprintf("error creating RabbitMQ exchange at channel '%s'", publisher.channel.Name())).Print()
			time.Sleep(time.Second)
			continue
		}

		_, err = publisher.channel.Access().QueueDeclare(target.Queue, true, false, false, false, nil)
		if err != nil {
			config.Error.Wrap(err, fmt.Sprintf("error creating queue at channel '%s'", publisher.channel.Name())).Print()
			time.Sleep(time.Second)
			continue
		}

		err = publisher.channel.Access().QueueBind(target.Queue, target.AccessKey, target.Exchange, false, nil)
		if err != nil {
			config.Error.Wrap(err, fmt.Sprintf("error binding queue at channel '%s'", publisher.channel.Name())).Print()
			time.Sleep(time.Second)
			continue
		}

		return nil
	}

	if err == nil {
		err = config.Error.New(fmt.Sprintf("could not prepare publish queue for unknown reason at channel '%s'", publisher.channel.Name()))
	}

	return err
}

/*
Publish data to the queue linked to RabbitMQ.PublishData object.
*/
func (publisher *Publisher) Publish(body []byte, gobTarget []byte) (err error) {
	var buffer bytes.Buffer
	var exchangeName string = publisher.ExchangeName
	var queueName string = publisher.QueueName
	var accessKey string = publisher.AccessKey
	var target dto.Target
	var success bool
	var confirmation *amqp.DeferredConfirmation

	if gobTarget != nil {
		_, err = buffer.Write(gobTarget)
		if err != nil {
			return config.Error.Wrap(err, fmt.Sprintf("error writing to buffer at channel '%s'", publisher.channel.Name()))
		}

		err = gob.NewDecoder(&buffer).Decode(&target)
		if err != nil {
			return config.Error.Wrap(err, fmt.Sprintf("error decoding gob at channel '%s'", publisher.channel.Name()))
		}
	}

	if target.Queue != "" {
		exchangeName = target.Exchange
		queueName = target.Queue
		accessKey = target.AccessKey
	}

	message := amqp.Publishing{
		ContentType:  "application/json",
		Body:         body,
		DeliveryMode: amqp.Persistent,
	}

	for {
		publisher.channel.WaitForChannel()

		confirmation, err = publisher.channel.Access().PublishWithDeferredConfirmWithContext(context.Background(), exchangeName, accessKey, true, false, message)
		if err != nil {
			if publisher.AlwaysRetry {
				config.Error.Wrap(err, fmt.Sprintf("error publishing message at channel '%s'", publisher.channel.Name())).Print()
				time.Sleep(time.Second)
				continue
			} else {
				return config.Error.Wrap(err, fmt.Sprintf("error publishing message at channel '%s'", publisher.channel.Name())).SetStatus(config.RETRY_POSSILBE)
			}
		}

		success = confirmation.Wait()
		if success {
			log.Printf("SUCCESS publishing on queue '%s' with delivery TAG '%d' at channel '%s'\n", queueName, confirmation.DeliveryTag, publisher.channel.Name())
			return nil

		} else {
			log.Printf("FAILED publishing on queue '%s' with delivery TAG '%d' at channel '%s'\n", queueName, confirmation.DeliveryTag, publisher.channel.Name())

			if publisher.AlwaysRetry {
				config.Error.New(fmt.Sprintf("error publishing message on queue '%s' at channel '%s'", queueName, publisher.channel.Name())).Print()
				time.Sleep(time.Second)
				continue
			} else {
				return config.Error.New(fmt.Sprintf("error publishing message on queue '%s' at channel '%s'", queueName, publisher.channel.Name())).SetStatus(config.RETRY_POSSILBE)
			}
		}
	}
}
