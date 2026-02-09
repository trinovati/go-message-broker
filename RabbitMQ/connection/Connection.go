package connection

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/trinovati/go-message-broker/v3/RabbitMQ/config"
	error_broker "github.com/trinovati/go-message-broker/v3/pkg/error"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

type channelRegistry struct {
	Id                  uuid.UUID
	ClosureNotification chan struct{}
}

/*
Object used to reference a amqp.Connection address.

Since the connections have intimate relation with the address of amqp.Connection, it could not be moved to another memory position for
shared connection purposes, so all shared connections points to a single RabbitMQConnection object.

The purpose of this abstraction is minimize the quantity of open amqp.Connection to RabbitMQ and at the same time make use of a keep alive
and reconnect technique.
*/
type RabbitMQConnection struct {
	env config.RABBITMQ_CONFIG

	amqpLastError     *amqp.Error
	amqpClosureNotify chan *amqp.Error

	Connection     *amqp.Connection
	Id             uuid.UUID
	TimesConnected uint64
	channels       []channelRegistry

	isOpen   bool
	isActive bool

	ctx       context.Context
	cancelCtx context.CancelFunc

	mutex *sync.Mutex

	logger   *slog.Logger
	logGroup slog.Attr
}

/*
Builder of RabbitMQConnection object.
*/
func NewRabbitMQConnection(
	env config.RABBITMQ_CONFIG,
	logger *slog.Logger,
) *RabbitMQConnection {
	if logger == nil {
		logger = slog.Default()
		logger.Warn("no logger have been passed to rabbitmq connector adapter constructor, using slog.Default")
	}

	var connectionId uuid.UUID = uuid.New()

	return &RabbitMQConnection{
		env: env,

		Connection:     nil,
		TimesConnected: 0,
		Id:             connectionId,

		mutex: &sync.Mutex{},

		ctx: context.Background(),

		logger: logger,
		logGroup: slog.Any(
			"message_broker",
			[]slog.Attr{
				{
					Key:   "adapter",
					Value: slog.StringValue("RabbitMQ"),
				},
				{
					Key:   "connection_id",
					Value: slog.StringValue(connectionId.String()),
				},
			},
		),
	}
}

func (c *RabbitMQConnection) Env() config.RABBITMQ_CONFIG {
	return c.env
}

/*
Create and keep alive a amqp.Connection linked to RabbitMQ connection.

If connection is dropped for any reason it will try to remake the connection.
To terminate the connection, use Close() method, it will close the connection via context.Done().
*/
func (c *RabbitMQConnection) Connect(ctx context.Context) (err error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.IsConnected(false) {
		return nil
	}

	err = c.connect(ctx)
	if err != nil {
		return err
	}

	c.isActive = true

	go c.keepConnection()

	return nil
}

func (c *RabbitMQConnection) connect(ctx context.Context) (err error) {
	var connection *amqp.Connection
	var retries int = 5

	for attempt := range retries {
		select {
		case <-ctx.Done():
			return fmt.Errorf("stop connecting due to context closure")

		case <-c.ctx.Done():
			return fmt.Errorf("stop connecting due to graceful connection closure")

		default:
			connection, err = amqp.Dial(c.env.PROTOCOL + "://" + c.env.USERNAME + ":" + c.env.PASSWORD + "@" + c.env.HOST + ":" + c.env.PORT + "/")
			if err != nil {
				c.logger.ErrorContext(c.ctx, "error creating a amqp.Connection server", slog.Any("error", err), c.logGroup)
				time.Sleep(500 * time.Millisecond)

				if attempt <= retries {
					return fmt.Errorf("error creating a amqp.Connection server host %s: %w", c.env.HOST, err)
				}

				continue
			}
		}

		c.logger.InfoContext(c.ctx, "connection successfully opened", c.logGroup)
		break
	}

	c.ctx, c.cancelCtx = context.WithCancel(ctx)
	c.amqpClosureNotify = connection.NotifyClose(make(chan *amqp.Error, 1))
	c.Connection = connection
	c.TimesConnected++
	c.isOpen = true

	return nil
}

/*
Method for maintenance of a amqp.Connection.

It will close the connection if the RabbitMQConnection signal its closure.

It will reconnect if receive a signal of dropped connection.
*/
func (c *RabbitMQConnection) keepConnection() {
	for {
		select {
		case <-c.ctx.Done():
			c.logger.InfoContext(c.ctx, "closing keep connection worker", c.logGroup)

			c.mutex.Lock()
			c.close(c.ctx)
			c.mutex.Unlock()
			return

		case closureNotification := <-c.amqpClosureNotify:
			c.mutex.Lock()

			c.isOpen = false

			if c.Connection != nil {
				c.Connection.Close()
				c.Connection = nil
			}

			if closureNotification != nil {
				c.amqpLastError = closureNotification
				c.logger.ErrorContext(c.ctx, "connection have been closed", slog.String("reason", closureNotification.Reason), slog.String("error", closureNotification.Error()), slog.Int("status", closureNotification.Code), c.logGroup)

			} else {
				c.logger.ErrorContext(c.ctx, "connection have been closed with no specified reason", c.logGroup)
			}

			err := c.connect(c.ctx)
			if err != nil {
				c.logger.ErrorContext(c.ctx, "error reconnecting to RabbitMQ", slog.Any("error", err))
			}

			c.mutex.Unlock()
		}
	}
}

/*
Method for closing the connection via context.

Keep in mind that this will affect all objects that shares connection with this one.
*/
func (c *RabbitMQConnection) Close(ctx context.Context) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if !c.isActive {
		return
	}

	if c.cancelCtx != nil {
		c.cancelCtx()
		c.cancelCtx = nil
	}

	c.close(ctx)
}

func (c *RabbitMQConnection) close(ctx context.Context) {
	c.logger.InfoContext(c.ctx, "closing connection", c.logGroup)

	if c.Connection != nil {
		c.isOpen = false
		c.isActive = false

		err := c.Connection.Close()
		if err != nil {
			c.logger.WarnContext(c.ctx, "error while closing connection (should treat as closed regardless)", slog.Any("error", err), c.logGroup)
		}

		c.Connection = nil

		c.BroadcastClosure()
	}
}

func (c *RabbitMQConnection) RegisterChannel(id uuid.UUID) (closureNotification <-chan struct{}) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.channels = append(c.channels,
		channelRegistry{
			Id:                  id,
			ClosureNotification: make(chan struct{}, 10),
		},
	)

	return c.channels[len(c.channels)-1].ClosureNotification
}

func (c *RabbitMQConnection) UnregisterChannel(id uuid.UUID) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for i, channel := range c.channels {
		if channel.Id == id {
			c.channels = slices.Delete(c.channels, i, i+1)
			return
		}
	}

	c.logger.WarnContext(c.ctx, fmt.Sprintf("no registered channel found with id %s", id), c.logGroup)
}

func (c *RabbitMQConnection) BroadcastClosure() {
	for _, channel := range c.channels {
		channel.ClosureNotification <- struct{}{}
	}
}

func (c *RabbitMQConnection) IsActive(lock bool) bool {
	if lock {
		c.mutex.Lock()
		defer c.mutex.Unlock()
	}

	return c.isActive
}

/*
Check the RabbitMQConnection availability.
*/
func (c *RabbitMQConnection) IsConnected(lock bool) bool {
	if lock {
		c.mutex.Lock()
		defer c.mutex.Unlock()
	}

	return c.isOpen && c.isActive && c.Connection != nil
}

/*
Block the process until the connection is open.
*/
func (c *RabbitMQConnection) WaitForConnection(ctx context.Context, lock bool) error {
	for {
		select {
		case <-ctx.Done():
			c.logger.InfoContext(c.ctx, "context canceled while waiting for RabbitMQ connection", c.logGroup)
			return fmt.Errorf("context canceled while waiting for RabbitMQ connection id %s", c.Id)

		case <-c.ctx.Done():
			c.logger.InfoContext(c.ctx, "closed connection while waiting for RabbitMQ connection", c.logGroup)
			return fmt.Errorf("closed connection while waiting for RabbitMQ connection id %s: %w", c.Id, error_broker.ErrClosedConnection)

		default:
			if c.IsConnected(lock) {
				return nil
			}

			if !c.IsActive(lock) {
				c.logger.WarnContext(c.ctx, "waiting on a closed RabbitMQ connection", c.logGroup)
				return fmt.Errorf("waiting on a closed RabbitMQ connection id %s: %w", c.Id, error_broker.ErrClosedConnection)
			} else {
				c.logger.InfoContext(c.ctx, "waiting for RabbitMQ connection", c.logGroup)
			}

			time.Sleep(500 * time.Millisecond)
		}
	}
}
