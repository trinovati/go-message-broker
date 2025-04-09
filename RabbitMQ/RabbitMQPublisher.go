// this rabbitmq package is adapting the amqp091-go lib.
package rabbitmq

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"time"

	"github.com/trinovati/go-message-broker/v3/RabbitMQ/channel"
	"github.com/trinovati/go-message-broker/v3/RabbitMQ/config"
	"github.com/trinovati/go-message-broker/v3/RabbitMQ/connection"
	dto_rabbitmq "github.com/trinovati/go-message-broker/v3/RabbitMQ/dto"
	"github.com/trinovati/go-message-broker/v3/RabbitMQ/interfaces"
)

/*
Adapter that handle publish to RabbitMQ.
*/
type RabbitMQPublisher struct {
	Name              string
	Queue             dto_rabbitmq.RabbitMQQueue
	notifyFlowChannel *chan bool
	channel           *channel.RabbitMQChannel
	AlwaysRetry       bool

	logger   *slog.Logger
	logGroup slog.Attr
}

/*
Builder of RabbitMQConsumer.

env is the connection data configurations.

name is a internal name for logging purposes.

queue is a RabbitMQQueue dto that hold the information of the queue this object will publish to.
*/
func NewRabbitMQPublisher(
	env config.RABBITMQ_CONFIG,
	name string,
	queue dto_rabbitmq.RabbitMQQueue,
	logger *slog.Logger,
) *RabbitMQPublisher {
	if logger == nil {
		log.Panicf("RabbitMQPublisher object have received a null logger dependency")
	}

	var channel *channel.RabbitMQChannel = channel.NewRabbitMQChannel(env, logger)

	var publisher *RabbitMQPublisher = &RabbitMQPublisher{
		Name:              name,
		Queue:             queue,
		notifyFlowChannel: nil,
		channel:           channel,
		AlwaysRetry:       false,

		logger: logger,
	}

	publisher.producePublisherLogGroup()

	return publisher
}

func (publisher *RabbitMQPublisher) producePublisherLogGroup() {
	publisher.logGroup = slog.Any(
		"message_broker",
		[]slog.Attr{
			{
				Key:   "adapter",
				Value: slog.StringValue("RabbitMQ"),
			},
			{
				Key:   "publisher",
				Value: slog.StringValue(publisher.Name),
			},
			{
				Key:   "queue",
				Value: slog.StringValue(publisher.Queue.Name),
			},
			{
				Key:   "channel_id",
				Value: slog.StringValue(publisher.channel.ChannelId.String()),
			},
			{
				Key:   "connection_id",
				Value: slog.StringValue(publisher.channel.Connection().ConnectionId.String()),
			},
		},
	)
}

/*
Will force this object to use the same channel present on the basic behavior of the argument.

Using shared channel is implicit that is using the same connection too.
*/
func (publisher *RabbitMQPublisher) ShareChannel(Behavior interfaces.Behavior) interfaces.Behavior {
	publisher.channel = Behavior.Channel()

	publisher.producePublisherLogGroup()

	return publisher
}

/*
Will force this object to use the same connection present on the basic behavior of the argument.
*/
func (publisher *RabbitMQPublisher) ShareConnection(Behavior interfaces.Behavior) interfaces.Behavior {
	publisher.channel.SetConnection(Behavior.Connection())

	publisher.producePublisherLogGroup()

	return publisher
}

/*
Open the amqp.Connection and amqp.Channel if not already open.
*/
func (publisher *RabbitMQPublisher) Connect(ctx context.Context) interfaces.Behavior {
	if publisher.channel.IsChannelDown() {
		publisher.channel.Connect(ctx)
	}

	return publisher
}

/*
Close the context of this channel reference.

Keep in mind that this will drop channel for all the shared objects.
*/
func (publisher *RabbitMQPublisher) CloseChannel(ctx context.Context) {
	publisher.channel.CloseChannel(ctx)
}

/*
Close the context of this connection reference.

Keep in mind that this will drop connection for all the shared objects.
Keep in mind that different channels sharing connection will be dropped as well.
*/
func (publisher *RabbitMQPublisher) CloseConnection(ctx context.Context) {
	publisher.channel.CloseConnection(ctx)
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

In case of queue not being bind to any exchange, it will bind it to a exchange.
*/
func (publisher *RabbitMQPublisher) PrepareQueue(ctx context.Context) (err error) {
	var tolerance int

	for tolerance = 0; publisher.AlwaysRetry || tolerance <= 5; tolerance++ {
		publisher.channel.WaitForChannel(ctx)

		err = publisher.channel.Channel.ExchangeDeclare(publisher.Queue.Exchange, publisher.Queue.ExchangeType, true, false, false, false, nil)
		if err != nil {
			publisher.logger.ErrorContext(ctx, "error creating RabbitMQ exchange", slog.Any("error", err), publisher.logGroup)
			time.Sleep(time.Second)
			continue
		}

		_, err = publisher.channel.Channel.QueueDeclare(publisher.Queue.Name, true, false, false, false, nil)
		if err != nil {
			publisher.logger.ErrorContext(ctx, "error creating queue", slog.Any("error", err), publisher.logGroup)
			time.Sleep(time.Second)
			continue
		}

		err = publisher.channel.Channel.QueueBind(publisher.Queue.Name, publisher.Queue.AccessKey, publisher.Queue.Exchange, false, nil)
		if err != nil {
			publisher.logger.ErrorContext(ctx, "error binding queue", slog.Any("error", err), publisher.logGroup)
			time.Sleep(time.Second)
			continue
		}

		return nil
	}

	if err == nil {
		err = fmt.Errorf("could not prepare publish queue %s for unknown reason from publisher %s at channel id %s and connection id %s", publisher.Queue.Name, publisher.Name, publisher.channel.ChannelId, publisher.channel.Connection().ConnectionId)
	}

	return err
}
