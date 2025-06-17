// this rabbitmq package is adapting the amqp091-go lib.
package channel

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/trinovati/go-message-broker/v3/RabbitMQ/config"
	"github.com/trinovati/go-message-broker/v3/RabbitMQ/connection"
	error_broker "github.com/trinovati/go-message-broker/v3/pkg/error"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

type consumerRegistry struct {
	Id                  uuid.UUID
	ClosureNotification chan struct{}
}

/*
Object used to reference a amqp.Channel address.

Since the connections have intimate relation with the address of amqp.Channel, it could not be moved to another memory position for
shared channel purposes, so all shared channels point to a single RabbitMQChannel object.

The purpose of this abstraction is minimize the quantity of open amqp.Channel to RabbitMQ and at the same time make use of a keep alive
and reconnect technique.
*/
type RabbitMQChannel struct {
	amqpLastError     *amqp.Error
	amqpClosureNotify chan *amqp.Error

	Channel      *amqp.Channel
	Id           uuid.UUID
	TimesCreated uint64
	consumers    []consumerRegistry

	connection              *connection.RabbitMQConnection
	connectionClosureNotify <-chan struct{}

	isOpen   bool
	isActive bool

	ctx       context.Context
	cancelCtx context.CancelFunc

	mutex *sync.Mutex

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

	var connection *connection.RabbitMQConnection = connection.NewRabbitMQConnection(env, logger)

	var channel *RabbitMQChannel = &RabbitMQChannel{
		Id:           uuid.New(),
		Channel:      nil,
		TimesCreated: 0,
		connection:   connection,

		mutex: &sync.Mutex{},

		logger: logger,
	}

	channel.connectionClosureNotify = channel.connection.RegisterChannel(channel.Id)

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
				Value: slog.StringValue(c.Id.String()),
			},
			{
				Key:   "connection_id",
				Value: slog.StringValue(c.connection.Id.String()),
			},
		},
	)
}

func (c *RabbitMQChannel) Lock() {
	c.mutex.Lock()
}

func (c *RabbitMQChannel) Unlock() {
	c.mutex.Unlock()
}

/*
Setter with the purpose of share RabbitMQConnection between multiple RabbitMQChannel.
*/
func (c *RabbitMQChannel) SetConnection(ctx context.Context, conn *connection.RabbitMQConnection) *RabbitMQChannel {
	c.connection.UnregisterChannel(ctx, c.Id)

	c.connection = conn
	c.connectionClosureNotify = c.connection.RegisterChannel(c.Id)

	c.produceChannelLogGroup()

	return c
}

/*
Getter of the RabbitMQConnection.
*/
func (c *RabbitMQChannel) Connection() *connection.RabbitMQConnection {
	return c.connection
}

/*
Create and keep alive a amqp.Channel linked to RabbitMQ connection.

If channel is dropped for any reason it will try to remake the channel.
To terminate the channel, use Close() method, it will close the channel via context.Done().

It puts the channel in confirm mode, so any publishing done will have a response from the server.
*/
func (c *RabbitMQChannel) Connect(ctx context.Context) (err error) {
	c.Lock()
	defer c.Unlock()

	if !c.connection.IsActive(true) {
		err = c.connection.Connect(ctx)
	}

	if c.IsChannelUp(false) {
		return nil
	}

	err = c.connect(ctx)
	if err != nil {
		return err
	}

	c.isActive = true

	go c.keepChannel(ctx)

	return nil
}

func (c *RabbitMQChannel) connect(ctx context.Context) (err error) {
	var channel *amqp.Channel

	for {
		select {
		case <-ctx.Done():
			c.logger.DebugContext(ctx, "stop creating channel due to context closure", c.logGroup)
			return

		default:
			err = c.connection.WaitForConnection(ctx, true)
			if err != nil {
				if errors.Is(err, error_broker.ErrClosedConnection) {
					return fmt.Errorf("waiting on closed connection: %w", err)
				}

				c.logger.ErrorContext(ctx, "error waiting for channel", slog.Any("error", err), c.logGroup)
				time.Sleep(500 * time.Millisecond)
				continue
			}

			channel, err = c.connection.Connection.Channel()
			if err != nil {
				c.logger.ErrorContext(ctx, "error creating amqp.Channel", slog.Any("error", err), c.logGroup)
				time.Sleep(500 * time.Millisecond)
				continue
			}

			err = channel.Confirm(false)
			if err != nil {
				c.logger.ErrorContext(ctx, "error configuring amqp.Channel with confirm protocol", slog.Any("error", err), c.logGroup)
				time.Sleep(500 * time.Millisecond)
				continue
			}
		}

		c.logger.InfoContext(ctx, "channel successfully opened", c.logGroup)
		break
	}

	c.amqpClosureNotify = channel.NotifyClose(make(chan *amqp.Error, 1))
	c.Channel = channel
	c.TimesCreated++
	c.ctx, c.cancelCtx = context.WithCancel(ctx)
	c.isOpen = true

	return nil
}

/*
Method for maintenance of a amqp.Channel.

It will close the channel if the RabbitMQConnection signal its closure.

It will stop to maintain the amqp.Channel if the RabbitMQChannel signal its closure.

It will reconnect if receive a signal of dropped connection.
*/
func (c *RabbitMQChannel) keepChannel(ctx context.Context) {
	timer := time.NewTimer(500 * time.Millisecond) // The timer is needed due to c.connectionClosureNotify being transitory because RabbitMQConnection may change.
	defer timer.Stop()

	for {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(500 * time.Millisecond)

		select {
		case <-c.connectionClosureNotify:
			c.logger.InfoContext(ctx, "connection has closed, channel should stop as well, stopping keeping channel worker", c.logGroup)

			c.mutex.Lock()
			c.close(ctx)
			c.mutex.Unlock()

			return

		case <-c.ctx.Done():
			c.logger.InfoContext(ctx, "closing keep channel worker", c.logGroup)

			c.mutex.Lock()
			c.close(ctx)
			c.mutex.Unlock()

			return

		case <-timer.C:
			continue

		case closeNotification := <-c.amqpClosureNotify:
			c.logger.WarnContext(ctx, "channel has dropped, trying recreate it", c.logGroup)

			c.Lock()

			c.isOpen = false

			if closeNotification != nil {
				c.amqpLastError = closeNotification
				c.logger.ErrorContext(ctx, "channel have been closed", slog.String("reason", closeNotification.Reason), slog.String("error", closeNotification.Error()), slog.Int("status", closeNotification.Code), c.logGroup)

			} else {
				c.logger.ErrorContext(ctx, "channel have been closed with no specified reason", c.logGroup)
			}

			err := c.connect(ctx)
			if err != nil {
				c.logger.ErrorContext(ctx, "error remaking RabbitMQ channel", slog.Any("error", err))
			}

			c.Unlock()
		}
	}
}

/*
Method for closing the channel via context.

Keep in mind that this will affect all objects that shares channel with this one.
*/
func (c *RabbitMQChannel) Close(ctx context.Context) {
	c.Lock()
	defer c.Unlock()

	if !c.isActive {
		return
	}

	if c.cancelCtx != nil {
		c.cancelCtx()
		c.cancelCtx = nil
	}

	c.close(ctx)
}

func (c *RabbitMQChannel) close(ctx context.Context) {
	c.logger.InfoContext(ctx, "closing channel", c.logGroup)

	if c.Channel != nil {
		c.isOpen = false
		c.isActive = false

		err := c.Channel.Close()
		if err != nil {
			c.logger.WarnContext(ctx, "error while closing channel (should treat as closed regardless)", slog.Any("error", err), c.logGroup)
		}

		c.Channel = nil

		c.BroadcastClosure()
	}
}

/*
Method for closing the channel and connection via context.

Keep in mind that this will affect all objects that shares channel or connection with this one, including closing
other non-shared channels that shares this connection.
*/
func (c *RabbitMQChannel) CloseConnection(ctx context.Context) {
	c.Lock()
	defer c.Unlock()

	c.connection.Close(ctx)
}

func (c *RabbitMQChannel) IsActive(lock bool) bool {
	if lock {
		c.mutex.Lock()
		defer c.mutex.Unlock()
	}

	return c.isActive
}

func (c *RabbitMQChannel) RegisterConsumer(id uuid.UUID) (closureNotification <-chan struct{}) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.consumers = append(c.consumers,
		consumerRegistry{
			Id:                  id,
			ClosureNotification: make(chan struct{}, 10),
		},
	)

	return c.consumers[len(c.consumers)-1].ClosureNotification
}

func (c *RabbitMQChannel) UnregisterConsumer(ctx context.Context, id uuid.UUID) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for i, consumer := range c.consumers {
		if consumer.Id == id {
			c.consumers = slices.Delete(c.consumers, i, i+1)
			return
		}
	}

	c.logger.WarnContext(ctx, fmt.Sprintf("no registered consumer found with id %s", id), c.logGroup)
}

func (c *RabbitMQChannel) BroadcastClosure() {
	for _, consumer := range c.consumers {
		consumer.ClosureNotification <- struct{}{}
	}
}

/*
Check the RabbitMQChannel availability.
*/
func (c *RabbitMQChannel) IsChannelUp(lock bool) bool {
	if lock {
		c.Lock()
		defer c.Unlock()
	}

	return c.isOpen && c.isActive && c.Channel != nil
}

/*
Block the process until the channel is open.
*/
func (c *RabbitMQChannel) WaitForChannel(ctx context.Context, lock bool) error {
	for {
		select {
		case <-ctx.Done():
			c.logger.InfoContext(ctx, "context canceled while waiting for RabbitMQ channel", c.logGroup)
			return fmt.Errorf("waiting on closed RabbitMQ channel id %s and connection id %s: %w", c.Id, c.connection.Id, error_broker.ErrClosedConnection)

		default:
			if c.IsChannelUp(lock) {
				return nil
			}

			if !c.IsActive(lock) {
				return fmt.Errorf("waiting on closed RabbitMQ channel id %s and connection id %s", c.Id, c.connection.Id)
			} else {
				c.logger.InfoContext(ctx, "waiting for RabbitMQ channel", c.logGroup)
			}

			time.Sleep(500 * time.Millisecond)
		}
	}
}
