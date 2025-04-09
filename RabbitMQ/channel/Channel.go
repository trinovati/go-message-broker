package channel

import (
	"context"
	"log"
	"log/slog"
	"time"

	"github.com/trinovati/go-message-broker/v3/RabbitMQ/config"
	"github.com/trinovati/go-message-broker/v3/RabbitMQ/connection"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

/*
Object used to reference a amqp.Channel address.

Since the connections have intimate relation with the address of amqp.Channel, it could not be moved to another memory position for
shared channel purposes, so all shared channels point to a single RabbitMQChannel object.

The purpose of this abstraction is minimize the quantity of open amqp.Channel to RabbitMQ and at the same time make use of a keep alive
and reconnect technique.
*/
type RabbitMQChannel struct {
	ChannelCount               uint64
	ChannelId                  uuid.UUID
	connection                 *connection.RabbitMQConnection
	Channel                    *amqp.Channel
	isOpen                     bool
	closureNotificationChannel chan *amqp.Error
	lastChannelError           *amqp.Error
	Context                    context.Context
	CancelContext              context.CancelFunc

	logger   *slog.Logger
	logGroup slog.Attr
}

/*
Builder of RabbitMQChannel object.
*/
func NewRabbitMQChannel(
	env config.RABBITMQ_CONFIG,
	logger *slog.Logger,
) *RabbitMQChannel {
	if logger == nil {
		log.Panicf("RabbitMQChannel object have received a null logger dependency")
	}

	var channel *RabbitMQChannel = &RabbitMQChannel{
		ChannelId:                  uuid.New(),
		connection:                 connection.NewRabbitMQConnection(env, logger),
		Channel:                    nil,
		isOpen:                     false,
		closureNotificationChannel: nil,
		lastChannelError:           nil,
		CancelContext:              nil,
		ChannelCount:               0,

		logger: logger,
	}

	channel.produceChannelLogGroup()

	return channel
}

func (c *RabbitMQChannel) produceChannelLogGroup() {
	c.logGroup = slog.Any(
		"message_broker",
		[]slog.Attr{
			{
				Key:   "adapter",
				Value: slog.StringValue("RabbitMQ"),
			},
			{
				Key:   "channel_id",
				Value: slog.StringValue(c.ChannelId.String()),
			},
			{
				Key:   "connection_id",
				Value: slog.StringValue(c.Connection().ConnectionId.String()),
			},
		},
	)
}

/*
Setter with the purpose of share RabbitMQConnection between multiple RabbitMQChannel.
*/
func (c *RabbitMQChannel) SetConnection(conn *connection.RabbitMQConnection) *RabbitMQChannel {
	c.connection = conn

	c.produceChannelLogGroup()

	return c
}

/*
Getter of the RabbitMQConnection.
*/
func (c RabbitMQChannel) Connection() *connection.RabbitMQConnection {
	return c.connection
}

/*
Create and keep alive a amqp.Channel linked to RabbitMQ connection.

If channel is dropped for any reason it will try to remake the channel.
To terminate the channel, use CloseChannel() method, it will close the channel via context.Done().

It puts the channel in confirm mode, so any publishing done will have a response from the server.
*/
func (c *RabbitMQChannel) Connect(ctx context.Context) *RabbitMQChannel {
	var err error
	var channel *amqp.Channel

	c.connection.Mutex.Lock()
	defer c.connection.Mutex.Unlock()

	if c.connection.Connection == nil {
		c.connection.Connect(ctx)
	} else if c.isOpen {
		return c
	}

	for {
		c.connection.WaitForConnection(ctx)

		channel, err = c.connection.Connection.Channel()
		if err != nil {
			c.logger.ErrorContext(ctx, "error creating RabbitMQ channel", slog.Any("error", err), c.logGroup)
			time.Sleep(time.Second)
			continue
		}

		err = channel.Confirm(false)
		if err != nil {
			c.logger.ErrorContext(ctx, "error configuring channel with confirm protocol", slog.Any("error", err), c.logGroup)
			continue
		}

		c.updateChannel(channel)
		c.logger.InfoContext(ctx, "channel successfully opened", c.logGroup)
		c.isOpen = true

		c.Context, c.CancelContext = context.WithCancel(ctx)

		go c.keepChannel(ctx)

		return c
	}
}

/*
Refresh the closureNotificationChannel for healthiness.

Reference the newly created amqp.Channel, assuring assincronus concurrent access to multiple objects.

Refresh the channel count for control of references.
*/
func (c *RabbitMQChannel) updateChannel(channel *amqp.Channel) {
	c.closureNotificationChannel = channel.NotifyClose(make(chan *amqp.Error))

	c.Channel = channel
	c.ChannelCount++
}

/*
Method for maintenance of a amqp.Channel.

It will close the channel if the RabbitMQConnection signal its closure.

It will stop to maintain the amqp.Channel if the RabbitMQChannel signal its closure.

It will reconnect if receive a signal of dropped connection.
*/
func (c *RabbitMQChannel) keepChannel(ctx context.Context) {
	select {
	case <-c.connection.Context.Done():
		c.logger.InfoContext(ctx, "connection, and consequently the channel, were gracefully closed", c.logGroup)
		c.CloseChannel(ctx)

	case <-c.Context.Done():
		c.logger.InfoContext(ctx, "channel have been gracefully closed", c.logGroup)

	case closeNotification := <-c.closureNotificationChannel:
		c.isOpen = false
		c.Channel.Close()

		if closeNotification != nil {
			c.lastChannelError = closeNotification
			c.logger.ErrorContext(ctx, "channel have been closed", slog.String("reason", closeNotification.Reason), slog.String("error", closeNotification.Error()), slog.Int("status", closeNotification.Code), c.logGroup)

		} else {
			c.logger.ErrorContext(ctx, "connection have been closed with no specified reason", c.logGroup)
		}

		c.Connect(ctx)
	}
}

/*
Method for closing the channel via context.

Keep in mind that this will affect all objects that shares channel with this one.
*/
func (c *RabbitMQChannel) CloseChannel(ctx context.Context) {
	c.isOpen = false

	if c.CancelContext != nil {
		c.CancelContext()
	}

	c.Channel.Close()
}

/*
Method for closing the channel and connection via context.

Keep in mind that this will affect all objects that shares channel or connection with this one, including closing
other non-shared channels that shares this connection.
*/
func (c *RabbitMQChannel) CloseConnection(ctx context.Context) {
	c.CloseChannel(ctx)
	c.connection.CloseConnection(ctx)
}

/*
Check the RabbitMQChannel availability.
*/
func (c *RabbitMQChannel) IsChannelDown() bool {
	return !c.isOpen
}

/*
Block the process until the channel is open.
*/
func (c *RabbitMQChannel) WaitForChannel(ctx context.Context) {
	for {
		if c.isOpen {
			return
		}

		c.logger.InfoContext(ctx, "waiting for rabbitmq channel", c.logGroup)
		time.Sleep(500 * time.Millisecond)
	}
}
