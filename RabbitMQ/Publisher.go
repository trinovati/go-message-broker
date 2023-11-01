package rabbitmq

import (
	"bytes"
	"context"
	"encoding/gob"
	"log"
	"strconv"
	"time"

	"gitlab.com/aplicacao/trinovati-connector-message-brokers/RabbitMQ/channel"
	"gitlab.com/aplicacao/trinovati-connector-message-brokers/RabbitMQ/config"
	"gitlab.com/aplicacao/trinovati-connector-message-brokers/RabbitMQ/interfaces"
	"gitlab.com/aplicacao/trinovati-connector-message-brokers/dto"

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

/*
Builds a new object that holds all information needed for publishing into a RabbitMQ queue.
*/
func NewPublisher(
	exchangeName string,
	exchangeType string,
	queueName string,
	accessKey string,
) *Publisher {
	return &Publisher{
		channel:           channel.NewChannel(),
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
	publisher.channel = behaviour.Channel()

	return publisher
}

func (publisher *Publisher) ShareConnection(behaviour interfaces.Behaviour) interfaces.Behaviour {
	publisher.channel.SetConnection(behaviour.Connection())

	return publisher
}

func (publisher *Publisher) Connect() interfaces.Behaviour {
	publisher.channel.Connect()

	return publisher
}

func (publisher *Publisher) CloseChannel() {
	publisher.channel.CloseChannel()
}

func (publisher *Publisher) CloseConnection() {
	publisher.channel.CloseConnection()
}

func (publisher Publisher) Connection() interfaces.Connection {
	return publisher.channel.Connection()
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
	var queue amqp.Queue
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
			Exchange:     publisher.ExchangeName,
			ExchangeType: publisher.ExchangeType,
			Queue:        publisher.QueueName,
			AccessKey:    publisher.QueueName,
		}
	}

	for tolerance := 0; publisher.AlwaysRetry || tolerance <= 5; tolerance++ {
		publisher.channel.WaitForChannel()

		err = publisher.channel.Access().ExchangeDeclare(target.Exchange, target.ExchangeType, true, false, false, false, nil)
		if err != nil {
			config.Error.Wrap(err, "error creating RabbitMQ exchange").Print()
			time.Sleep(time.Second)
			continue
		}

		queue, err = publisher.channel.Access().QueueDeclare(target.Queue, true, false, false, false, nil)
		if err != nil {
			config.Error.Wrap(err, "error creating queue").Print()
			time.Sleep(time.Second)
			continue
		}

		if queue.Name != target.Queue {
			err = config.Error.New("created queue name '" + queue.Name + "' and expected queue name '" + target.Queue + "' are diferent")
			err.(interfaces.Error).Print()
			time.Sleep(time.Second)
			continue
		}

		err = publisher.channel.Access().QueueBind(target.Queue, target.AccessKey, target.Exchange, false, nil)
		if err != nil {
			config.Error.Wrap(err, "error binding queue").Print()
			time.Sleep(time.Second)
			continue
		}

		return nil
	}

	if err == nil {
		err = config.Error.New("could not prepare publish queue for unknown reason")
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

	if gobTarget != nil {
		_, err = buffer.Write(gobTarget)
		if err != nil {
			return config.Error.Wrap(err, "error writing to buffer")
		}

		err = gob.NewDecoder(&buffer).Decode(&target)
		if err != nil {
			return config.Error.Wrap(err, "error decoding gob")
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

	publisher.channel.WaitForChannel()
	notifyFlowChannel := publisher.channel.Access().NotifyFlow(make(chan bool))

	for {
		select {
		case flowNotify := <-notifyFlowChannel:
			if flowNotify {
				continue
			}

			close(notifyFlowChannel)
			return config.Error.New("queue '" + queueName + "' flow is closed").SetStatus(config.RETRY_POSSILBE)

		default:
			publisher.channel.WaitForChannel()
			confirmation, err := publisher.channel.Access().PublishWithDeferredConfirmWithContext(context.Background(), exchangeName, accessKey, true, false, message)
			if err != nil {
				if publisher.AlwaysRetry {
					config.Error.Wrap(err, "error publishing message").Print()
					time.Sleep(time.Second)
					continue
				} else {
					return config.Error.Wrap(err, "error publishing message").SetStatus(config.RETRY_POSSILBE)
				}
			}

			success := confirmation.Wait()
			if success {
				log.Println("SUCCESS publishing on queue '" + queueName + "' with delivery TAG '" + strconv.FormatUint(confirmation.DeliveryTag, 10) + "'.")
				return nil

			} else {
				log.Println("FAILED publishing on queue '" + queueName + "' with delivery TAG '" + strconv.FormatUint(confirmation.DeliveryTag, 10) + "'.")

				if publisher.AlwaysRetry {
					config.Error.New("error publishing message").Print()
					time.Sleep(time.Second)
					continue
				} else {
					return config.Error.New("error publishing message").SetStatus(config.RETRY_POSSILBE)
				}
			}
		}
	}
}
