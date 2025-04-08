// this rabbitmq package is adapting the amqp091-go lib
package rabbitmq

import (
	"fmt"
	"log"
	"time"

	"github.com/pkg/errors"
	"github.com/trinovati/go-message-broker/RabbitMQ/channel"
	"github.com/trinovati/go-message-broker/RabbitMQ/config"
	"github.com/trinovati/go-message-broker/RabbitMQ/connection"
	"github.com/trinovati/go-message-broker/RabbitMQ/dto"
	"github.com/trinovati/go-message-broker/RabbitMQ/interfaces"
)

/*
Adapter that handle publish to RabbitMQ.
*/
type RabbitMQPublisher struct {
	Queue             dto.RabbitMQQueue
	notifyFlowChannel *chan bool
	channel           *channel.RabbitMQChannel
	AlwaysRetry       bool
}

/*
Builder of RabbitMQPublisher.
*/
func NewRabbitMQPublisher(
	env config.RABBITMQ_CONFIG,
	name string,
	queue dto.RabbitMQQueue,
) *RabbitMQPublisher {
	return &RabbitMQPublisher{
		Queue:             queue,
		notifyFlowChannel: nil,
		channel:           channel.NewRabbitMQChannel(env),
		AlwaysRetry:       false,
	}
}

/*
Will force this object to use the same channel present on the basic behavior of the argument.

Using shared channel is implicit that is using the same connection too.
*/
func (publisher *RabbitMQPublisher) ShareChannel(Behavior interfaces.Behavior) interfaces.Behavior {
	publisher.channel = Behavior.Channel()

	return publisher
}

/*
Will force this object to use the same connection present on the basic behavior of the argument.
*/
func (publisher *RabbitMQPublisher) ShareConnection(Behavior interfaces.Behavior) interfaces.Behavior {
	publisher.channel.SetConnection(Behavior.Connection())

	return publisher
}

/*
Open the amqp.Connection and amqp.Channel if not already open.
*/
func (publisher *RabbitMQPublisher) Connect() interfaces.Behavior {
	if publisher.channel.IsChannelDown() {
		publisher.channel.Connect()
	}

	return publisher
}

/*
Close the context of this channel reference.

Keep in mind that this will drop channel for all the shared objects.
*/
func (publisher *RabbitMQPublisher) CloseChannel() {
	publisher.channel.CloseChannel()
}

/*
Close the context of this connection reference.

Keep in mind that this will drop connection for all the shared objects.
Keep in mind that different channels sharing connection will be dropped as well.
*/
func (publisher *RabbitMQPublisher) CloseConnection() {
	publisher.channel.CloseConnection()
}

/*
Returns the reference of the RabbitMQConnection.
*/
func (publisher RabbitMQPublisher) Connection() *connection.RabbitMQConnection {
	return publisher.channel.Connection()
}

/*
Returns the reference of RabbitMQChannel
*/
func (publisher RabbitMQPublisher) Channel() *channel.RabbitMQChannel {
	return publisher.channel
}

/*
Prepare a queue linked to RabbitMQ channel for publishing.

In case of non-existent exchange, it will create the exchange.

In case of non-existent queue, it will create the queue.

In case of queue not being binded to any exchange, it will bind it to a exchange.
*/
func (publisher *RabbitMQPublisher) PrepareQueue() (err error) {
	var tolerance int

	for tolerance = 0; publisher.AlwaysRetry || tolerance <= 5; tolerance++ {
		publisher.channel.WaitForChannel()

		err = publisher.channel.Channel.ExchangeDeclare(publisher.Queue.Exchange, publisher.Queue.ExchangeType, true, false, false, false, nil)
		if err != nil {
			log.Println(errors.Wrap(err, fmt.Sprintf("error creating RabbitMQ exchange at channel %s for queue %s", publisher.channel.ChannelId.String(), publisher.Queue.Name)))
			time.Sleep(time.Second)
			continue
		}

		_, err = publisher.channel.Channel.QueueDeclare(publisher.Queue.Name, true, false, false, false, nil)
		if err != nil {
			log.Println(errors.Wrap(err, fmt.Sprintf("error creating queue at channel %s for queue %s", publisher.channel.ChannelId.String(), publisher.Queue.Name)))
			time.Sleep(time.Second)
			continue
		}

		err = publisher.channel.Channel.QueueBind(publisher.Queue.Name, publisher.Queue.AccessKey, publisher.Queue.Exchange, false, nil)
		if err != nil {
			log.Println(errors.Wrap(err, fmt.Sprintf("error binding queue at channel %s for queue %s", publisher.channel.ChannelId.String(), publisher.Queue.Name)))
			time.Sleep(time.Second)
			continue
		}

		return nil
	}

	if err == nil {
		err = errors.New(fmt.Sprintf("could not prepare publish queue for unknown reason at channel %s for queue %s", publisher.channel.ChannelId.String(), publisher.Queue.Name))
	}

	return err
}
