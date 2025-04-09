package rabbitmq

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"sync"

	"github.com/trinovati/go-message-broker/v3/RabbitMQ/channel"
	"github.com/trinovati/go-message-broker/v3/RabbitMQ/config"
	"github.com/trinovati/go-message-broker/v3/RabbitMQ/connection"
	rabbitmqdto "github.com/trinovati/go-message-broker/v3/RabbitMQ/dto"
	"github.com/trinovati/go-message-broker/v3/RabbitMQ/interfaces"
	dto_pkg "github.com/trinovati/go-message-broker/v3/dto"
)

/*
Adapter that handle consume from RabbitMQ.
*/
type RabbitMQConsumer struct {
	Name               string
	deadletter         interfaces.Publisher
	Queue              rabbitmqdto.RabbitMQQueue
	DeliveryChannel    chan dto_pkg.BrokerDelivery
	AcknowledgeChannel chan dto_pkg.BrokerAcknowledge
	DeliveryMap        *sync.Map
	channel            *channel.RabbitMQChannel
	AlwaysRetry        bool

	consumerCtx    context.Context
	consumerCancel context.CancelFunc

	logger   *slog.Logger
	logGroup slog.Attr
}

/*
Builder of RabbitMQConsumer.

env is the connection data configurations.

name is a internal name for logging purposes.

deadletter is a Publisher that wil be used for acknowledges.This is optional and can be nil.

queue is a RabbitMQQueue dto that hold the information of the queue this object will consume from.

deliveriesBufferSize and acknowledgerBufferSize set a buffer to the channel so it have a size limit.
Keep in mind that buffering the channels may block the pushes until freeing a new position.
Keep in mind that unbuffered channels can't use len() to check quantity of data waiting.
*/
func NewRabbitMQConsumer(
	ctx context.Context,
	env config.RABBITMQ_CONFIG,
	name string,
	deadletter interfaces.Publisher,
	queue rabbitmqdto.RabbitMQQueue,
	deliveriesBufferSize int,
	acknowledgerBufferSize int,
	logger *slog.Logger,
) *RabbitMQConsumer {
	if logger == nil {
		log.Panicf("RabbitMQConsumer object have received a null logger dependency")
	}

	var channel *channel.RabbitMQChannel = channel.NewRabbitMQChannel(env, logger)

	var consumer *RabbitMQConsumer = &RabbitMQConsumer{
		Name:               name,
		deadletter:         deadletter,
		Queue:              queue,
		channel:            channel,
		DeliveryChannel:    make(chan dto_pkg.BrokerDelivery, deliveriesBufferSize),
		AcknowledgeChannel: make(chan dto_pkg.BrokerAcknowledge, acknowledgerBufferSize),
		DeliveryMap:        &sync.Map{},
		AlwaysRetry:        true,

		logger: logger,
	}

	consumer.produceConsumerLogGroup()

	if deadletter != nil {
		consumer.ShareConnection(deadletter)
	} else {
		consumer.logger.WarnContext(ctx, "NO DEADLETTER QUEUE CONFIGURED    DEADLETTER COMMANDS WILL BE IGNORED IN THIS CONSUMER", consumer.logGroup)
	}

	return consumer
}

func (consumer *RabbitMQConsumer) produceConsumerLogGroup() {
	consumer.logGroup = slog.Any(
		"message_broker",
		[]slog.Attr{
			{
				Key:   "adapter",
				Value: slog.StringValue("RabbitMQ"),
			},
			{
				Key:   "consumer",
				Value: slog.StringValue(consumer.Name),
			},
			{
				Key:   "queue",
				Value: slog.StringValue(consumer.Queue.Name),
			},
			{
				Key:   "channel_id",
				Value: slog.StringValue(consumer.channel.ChannelId.String()),
			},
			{
				Key:   "connection_id",
				Value: slog.StringValue(consumer.channel.Connection().ConnectionId.String()),
			},
		},
	)
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

	consumer.produceConsumerLogGroup()

	return consumer
}

/*
Will force this object to use the same connection present on the basic behavior of the argument.
*/
func (consumer *RabbitMQConsumer) ShareConnection(behavior interfaces.Behavior) interfaces.Behavior {
	consumer.channel.SetConnection(behavior.Connection())

	consumer.produceConsumerLogGroup()

	return consumer
}

/*
Open the amqp.Connection and amqp.Channel if not already open.
*/
func (consumer *RabbitMQConsumer) Connect(ctx context.Context) interfaces.Behavior {
	if consumer.channel.IsChannelDown() {
		consumer.channel.Connect(ctx)
	}

	if consumer.deadletter != nil {
		consumer.deadletter.Connect(ctx)
	} else {
		consumer.logger.WarnContext(ctx, "NO DEADLETTER QUEUE CONFIGURED TO CONNECT", consumer.logGroup)
	}

	return consumer
}

/*
Close the context of this channel reference.

Keep in mind that this will drop channel for all the shared objects.
*/
func (consumer *RabbitMQConsumer) CloseChannel(ctx context.Context) {
	consumer.channel.CloseChannel(ctx)
}

/*
Close the context of this connection reference.

Keep in mind that this will drop connection for all the shared objects.
Keep in mind that different channels sharing connection will be dropped as well.
*/
func (consumer *RabbitMQConsumer) CloseConnection(ctx context.Context) {
	consumer.channel.CloseConnection(ctx)
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
func (consumer *RabbitMQConsumer) PrepareQueue(ctx context.Context) (err error) {
	if consumer.channel.IsChannelDown() {
		return fmt.Errorf("channel dropped before declaring exchange %s from consumer %s at channel id %s and connection id %s", consumer.Queue.Exchange, consumer.Name, consumer.channel.ChannelId, consumer.channel.Connection().ConnectionId)
	}

	err = consumer.channel.Channel.ExchangeDeclare(consumer.Queue.Exchange, consumer.Queue.ExchangeType, true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("error declaring RabbitMQ exchange %s from consumer %s at channel id %s and connection id %s: %w", consumer.Queue.Exchange, consumer.Name, consumer.channel.ChannelId, consumer.channel.Connection().ConnectionId, err)
	}

	if consumer.channel.IsChannelDown() {
		return fmt.Errorf("channel dropped before declaring queue %s from consumer %s at channel id %s and connection id %s", consumer.Queue.Name, consumer.Name, consumer.channel.ChannelId, consumer.channel.Connection().ConnectionId)
	}

	_, err = consumer.channel.Channel.QueueDeclare(consumer.Queue.Name, true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("error declaring queue %s from consumer %s at channel id %s and connection id %s: %w", consumer.Queue.Name, consumer.Name, consumer.channel.ChannelId, consumer.channel.Connection().ConnectionId, err)
	}

	if consumer.channel.IsChannelDown() {
		return fmt.Errorf("channel dropped before binding of queue %s from consumer %s at channel id %s and connection id %s", consumer.Queue.Name, consumer.Name, consumer.channel.ChannelId, consumer.channel.Connection().ConnectionId)
	}

	err = consumer.channel.Channel.QueueBind(consumer.Queue.Name, consumer.Queue.AccessKey, consumer.Queue.Exchange, false, nil)
	if err != nil {
		return fmt.Errorf("error binding queue %s from consumer %s at channel id %s and connection id %s: %w", consumer.Queue.Name, consumer.Name, consumer.channel.ChannelId, consumer.channel.Connection().ConnectionId, err)
	}

	err = consumer.channel.Channel.Qos(consumer.Queue.Qos, 0, false)
	if err != nil {
		return fmt.Errorf("error setting qos of %d for queue %s from consumer %s at channel id %s and connection id %s: %w", consumer.Queue.Qos, consumer.Queue.Name, consumer.Name, consumer.channel.ChannelId, consumer.channel.Connection().ConnectionId, err)
	}

	if consumer.Queue.Purge {
		if consumer.channel.IsChannelDown() {
			return fmt.Errorf("channel dropped before purging queue %s from consumer %s at channel id %s and connection id %s", consumer.Queue.Name, consumer.Name, consumer.channel.ChannelId, consumer.channel.Connection().ConnectionId)
		}

		_, err = consumer.channel.Channel.QueuePurge(consumer.Queue.Name, true)
		if err != nil {
			return fmt.Errorf("error purging queue %s from consumer %s at channel id %s and connection id %s: %w", consumer.Queue.Name, consumer.Name, consumer.channel.ChannelId, consumer.channel.Connection().ConnectionId, err)
		}
	}

	if consumer.deadletter != nil {
		err = consumer.deadletter.PrepareQueue(ctx)
		if err != nil {
			return fmt.Errorf("error preparing deadletter queue %s from publisher %s at channel id %s and connection id %s: %w", consumer.Queue.Name, consumer.Name, consumer.channel.ChannelId, consumer.channel.Connection().ConnectionId, err)
		}
	} else {
		consumer.logger.WarnContext(ctx, "NO DEADLETTER QUEUE PREPARED    DEADLETTER COMMANDS WILL BE IGNORED BY THIS CONSUMER", consumer.logGroup)
	}

	return nil
}
