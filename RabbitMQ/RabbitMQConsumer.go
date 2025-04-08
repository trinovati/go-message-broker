// this rabbitmq package is adapting the amqp091-go lib
package rabbitmq

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/pkg/errors"

	"github.com/trinovati/go-message-broker/RabbitMQ/channel"
	"github.com/trinovati/go-message-broker/RabbitMQ/config"
	"github.com/trinovati/go-message-broker/RabbitMQ/connection"
	"github.com/trinovati/go-message-broker/RabbitMQ/dto"
	"github.com/trinovati/go-message-broker/RabbitMQ/interfaces"
	dto_pkg "github.com/trinovati/go-message-broker/dto"
)

/*
Adapter that handle consume from RabbitMQ.
*/
type RabbitMQConsumer struct {
	Name               string
	deadletter         interfaces.Publisher
	Queue              dto.RabbitMQQueue
	DeliveryChannel    chan dto_pkg.BrokerDelivery
	AcknowledgeChannel chan dto_pkg.BrokerAcknowledge
	DeliveryMap        *sync.Map
	channel            *channel.RabbitMQChannel
	AlwaysRetry        bool

	consumerCtx    context.Context
	consumerCancel context.CancelFunc
}

/*
Builder of RabbitMQConsumer.
*/
func NewRabbitMQConsumer(
	env config.RABBITMQ_CONFIG,
	name string,
	deadletter interfaces.Publisher,
	queue dto.RabbitMQQueue,
) *RabbitMQConsumer {
	var consumer *RabbitMQConsumer = &RabbitMQConsumer{
		Name:               name,
		deadletter:         deadletter,
		Queue:              queue,
		channel:            channel.NewRabbitMQChannel(env),
		DeliveryChannel:    make(chan dto_pkg.BrokerDelivery, 50),
		AcknowledgeChannel: make(chan dto_pkg.BrokerAcknowledge),
		DeliveryMap:        &sync.Map{},
		AlwaysRetry:        true,
	}

	if deadletter != nil {
		consumer.ShareConnection(deadletter)
	} else {
		log.Printf("WARNING    NO DEADLETTER QUEUE CONFIGURED    DEADLETTER COMMANDS WILL BE IGNORED AT CONSUMER: %s\n", consumer.Name)
	}

	return consumer
}

/*
Returns the channel that ConsumeForever() will deliver the messages.
*/
func (consumer *RabbitMQConsumer) Deliveries() (deliveries chan dto_pkg.BrokerDelivery) {
	return consumer.DeliveryChannel
}

/*
Will force this object to use the same channel present on the basic behavior of the argument.

Using shared channel is implicit that is using the same connection too.
*/
func (consumer *RabbitMQConsumer) ShareChannel(behavior interfaces.Behavior) interfaces.Behavior {
	consumer.channel = behavior.Channel()

	return consumer
}

/*
Will force this object to use the same connection present on the basic behavior of the argument.
*/
func (consumer *RabbitMQConsumer) ShareConnection(behavior interfaces.Behavior) interfaces.Behavior {
	consumer.channel.SetConnection(behavior.Connection())

	return consumer
}

/*
Open the amqp.Connection and amqp.Channel if not already open.
*/
func (consumer *RabbitMQConsumer) Connect() interfaces.Behavior {
	if consumer.channel.IsChannelDown() {
		consumer.channel.Connect()
	}

	if consumer.deadletter != nil {
		consumer.deadletter.Connect()
	} else {
		log.Println("WARNING    NO DEADLETTER QUEUE CONFIGURED TO CONNECT")
	}

	return consumer
}

/*
Close the context of this channel reference.

Keep in mind that this will drop channel for all the shared objects.
*/
func (consumer *RabbitMQConsumer) CloseChannel() {
	consumer.channel.CloseChannel()
}

/*
Close the context of this connection reference.

Keep in mind that this will drop connection for all the shared objects.
Keep in mind that different channels sharing connection will be dropped as well.
*/
func (consumer *RabbitMQConsumer) CloseConnection() {
	consumer.channel.CloseConnection()
}

/*
Returns the reference of the RabbitMQConnection.
*/
func (consumer *RabbitMQConsumer) Connection() *connection.RabbitMQConnection {
	return consumer.channel.Connection()
}

/*
Returns the reference of RabbitMQChannel
*/
func (consumer *RabbitMQConsumer) Channel() *channel.RabbitMQChannel {
	return consumer.channel
}

/*
Prepare a queue linked to RabbitMQ channel for consuming.

In case of non-existent exchange, it will create the exchange.

In case of non-existent queue, it will create the queue.

In case of queue not being binded to any exchange, it will bind it to a exchange.

It will set Qos to the channel.

It will purge the queue before consuming case ordered to.
*/
func (consumer *RabbitMQConsumer) PrepareQueue() (err error) {
	if consumer.channel.IsChannelDown() {
		return errors.New(fmt.Sprintf("channel dropped before declaring exchange at channel %s for queue %s", consumer.channel.ChannelId.String(), consumer.Queue.Name))
	}

	err = consumer.channel.Channel.ExchangeDeclare(consumer.Queue.Exchange, consumer.Queue.ExchangeType, true, false, false, false, nil)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("error creating RabbitMQ exchange at channel %s for queue %s", consumer.channel.ChannelId.String(), consumer.Queue.Name))
	}

	if consumer.channel.IsChannelDown() {
		return errors.New(fmt.Sprintf("channel dropped before declaring queue at channel %s for queue %s", consumer.channel.ChannelId.String(), consumer.Queue.Name))
	}

	_, err = consumer.channel.Channel.QueueDeclare(consumer.Queue.Name, true, false, false, false, nil)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("error creating queue at channel %s for queue %s", consumer.channel.ChannelId.String(), consumer.Queue.Name))
	}

	if consumer.channel.IsChannelDown() {
		return errors.New(fmt.Sprintf("channel dropped before queue binding at channel %s for queue %s", consumer.channel.ChannelId.String(), consumer.Queue.Name))
	}

	err = consumer.channel.Channel.QueueBind(consumer.Queue.Name, consumer.Queue.AccessKey, consumer.Queue.Exchange, false, nil)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("error binding queue at channel %s for queue %s", consumer.channel.ChannelId.String(), consumer.Queue.Name))
	}

	err = consumer.channel.Channel.Qos(consumer.Queue.Qos, 0, false)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("error qos a channel to limiting the maximum message queue can hold at channel %s for queue %s", consumer.channel.ChannelId.String(), consumer.Queue.Name))
	}

	if consumer.Queue.Purge {
		if consumer.channel.IsChannelDown() {
			return errors.New(fmt.Sprintf("channel dropped before purging queue at channel %s for queue %s", consumer.channel.ChannelId.String(), consumer.Queue.Name))
		}

		_, err = consumer.channel.Channel.QueuePurge(consumer.Queue.Name, true)
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("error purging queue %s at channel %s for queue %s", consumer.Queue.Name, consumer.channel.ChannelId.String(), consumer.Queue.Name))
		}
	}

	if consumer.deadletter != nil {
		err = consumer.deadletter.PrepareQueue()
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("error preparing deadletter queue at channel %s for queue %s", consumer.channel.ChannelId.String(), consumer.Queue.Name))
		}
	} else {
		log.Printf("WARNING    NO DEADLETTER QUEUE PREPARED    DEADLETTER COMMANDS WILL BE IGNORED AT CONSUMER: %s\n", consumer.Name)
	}

	return nil
}
