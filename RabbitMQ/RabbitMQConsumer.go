package rabbitmq

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/trinovati/go-message-broker/v3/RabbitMQ/channel"
	"github.com/trinovati/go-message-broker/v3/RabbitMQ/config"
	"github.com/trinovati/go-message-broker/v3/RabbitMQ/connection"
	dto_rabbitmq "github.com/trinovati/go-message-broker/v3/RabbitMQ/dto"
	"github.com/trinovati/go-message-broker/v3/RabbitMQ/interfaces"
	dto_broker "github.com/trinovati/go-message-broker/v3/pkg/dto"

	"github.com/google/uuid"
)

/*
Adapter that handle consume from RabbitMQ.
*/
type RabbitMQConsumer struct {
	Name  string
	Id    uuid.UUID
	Queue dto_rabbitmq.RabbitMQQueue

	isRunning   bool
	isConsuming bool

	DeliveryChannel    chan dto_broker.BrokerDelivery
	DeliveryMap        *sync.Map
	AcknowledgeChannel chan dto_broker.BrokerAcknowledge

	deadletter interfaces.Publisher

	channel              *channel.RabbitMQChannel
	channelClosureNotify <-chan struct{}
	channelTimesCreated  uint64

	mutex *sync.Mutex

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
Keep in mind that buffering the channels may block the pushes after its size is filled.
Keep in mind that unbuffered channels block even on the first push, and can't use len() on them to
check quantity of data waiting.
*/
func NewRabbitMQConsumer(
	ctx context.Context,
	env config.RABBITMQ_CONFIG,
	name string,
	deadletter interfaces.Publisher,
	queue dto_rabbitmq.RabbitMQQueue,
	deliveriesBufferSize int,
	acknowledgerBufferSize int,
	logger *slog.Logger,
) *RabbitMQConsumer {
	if logger == nil {
		logger = slog.Default()
		logger.Warn("no logger have been passed to rabbitmq consumer adapter constructor, using slog.Default")
	}

	var channel *channel.RabbitMQChannel = channel.NewRabbitMQChannel(env, logger)

	var consumer *RabbitMQConsumer = &RabbitMQConsumer{
		Name:  name,
		Id:    uuid.New(),
		Queue: queue,

		DeliveryChannel:    make(chan dto_broker.BrokerDelivery, deliveriesBufferSize),
		DeliveryMap:        &sync.Map{},
		AcknowledgeChannel: make(chan dto_broker.BrokerAcknowledge, acknowledgerBufferSize),

		deadletter: deadletter,

		channel:             channel,
		channelTimesCreated: 0,

		mutex: &sync.Mutex{},

		logger: logger,
	}

	consumer.produceConsumerLogGroup()

	if deadletter != nil {
		consumer.ShareConnection(deadletter)
	} else {
		consumer.logger.WarnContext(ctx, "NO DEADLETTER QUEUE CONFIGURED, DEADLETTER COMMANDS WILL BE IGNORED BY THIS CONSUMER", consumer.logGroup)
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
				Key:   "consumer_id",
				Value: slog.StringValue(consumer.Id.String()),
			},
			{
				Key:   "channel_id",
				Value: slog.StringValue(consumer.channel.Id.String()),
			},
			{
				Key:   "connection_id",
				Value: slog.StringValue(consumer.channel.Connection().Id.String()),
			},
		},
	)
}

/*
Returns the channel that ConsumeForever() will deliver the messages.
*/
func (consumer *RabbitMQConsumer) Deliveries() (deliveries chan dto_broker.BrokerDelivery) {
	return consumer.DeliveryChannel
}

func (consumer *RabbitMQConsumer) IsConsuming() bool {
	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()

	return consumer.isRunning && consumer.channel.IsChannelUp(true)
}

func (consumer *RabbitMQConsumer) IsRunning() bool {
	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()

	return consumer.isRunning
}

/*
Will force this object to use the same channel present on the basic behavior of the argument.

Using shared channel is implicit that is using the same connection too.
*/
func (consumer *RabbitMQConsumer) ShareChannel(behavior interfaces.Behavior) interfaces.Behavior {
	consumer.channel = behavior.Channel()

	consumer.produceConsumerLogGroup()

	consumer.channelTimesCreated = consumer.channel.TimesCreated

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
	if !consumer.channel.IsActive(true) {
		err := consumer.channel.Connect(ctx)
		if err != nil {
			panic(err)
		}
	}
	consumer.channelTimesCreated = consumer.channel.TimesCreated

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
	consumer.channel.Close(ctx)
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

In case of queue not being bind to any exchange, it will bind it to a exchange.

It will set Qos to the channel.

It will purge the queue before consuming case ordered to.
*/
func (consumer *RabbitMQConsumer) PrepareQueue(ctx context.Context, lock bool) (err error) {
	if !consumer.channel.IsChannelUp(lock) {
		return fmt.Errorf("channel dropped before declaring exchange %s from consumer %s", consumer.Queue.Exchange, consumer.Name)
	}

	err = consumer.channel.Channel.ExchangeDeclare(consumer.Queue.Exchange, consumer.Queue.ExchangeType, true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("error declaring RabbitMQ exchange %s from consumer %s: %w", consumer.Queue.Exchange, consumer.Name, err)
	}

	_, err = consumer.channel.Channel.QueueDeclare(consumer.Queue.Name, true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("error declaring queue %s from consumer %s: %w", consumer.Queue.Name, consumer.Name, err)
	}

	err = consumer.channel.Channel.QueueBind(consumer.Queue.Name, consumer.Queue.AccessKey, consumer.Queue.Exchange, false, nil)
	if err != nil {
		return fmt.Errorf("error binding queue %s from consumer %s: %w", consumer.Queue.Name, consumer.Name, err)
	}

	err = consumer.channel.Channel.Qos(consumer.Queue.Qos, 0, false)
	if err != nil {
		return fmt.Errorf("error setting qos of %d for queue %s from consumer %s: %w", consumer.Queue.Qos, consumer.Queue.Name, consumer.Name, err)
	}

	if consumer.Queue.Purge {
		_, err = consumer.channel.Channel.QueuePurge(consumer.Queue.Name, true)
		if err != nil {
			return fmt.Errorf("error purging queue %s from consumer %s: %w", consumer.Queue.Name, consumer.Name, err)
		}
	}

	if consumer.deadletter != nil {
		err = consumer.deadletter.PrepareQueue(ctx, lock)
		if err != nil {
			return fmt.Errorf("error preparing deadletter queue %s from publisher %s: %w", consumer.Queue.Name, consumer.Name, err)
		}
	} else {
		consumer.logger.WarnContext(ctx, "NO DEADLETTER QUEUE PREPARED, DEADLETTER COMMANDS WILL BE IGNORED BY THIS CONSUMER", consumer.logGroup)
	}

	return nil
}
