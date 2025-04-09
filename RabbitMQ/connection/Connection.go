// this rabbitmq package is adapting the amqp091-go lib.
package connection

import (
	"context"
	"log"
	"log/slog"
	"sync"
	"time"

	"github.com/trinovati/go-message-broker/v3/RabbitMQ/config"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

/*
Object used to reference a amqp.Connection address.

Since the connections have intimate relation with the address of amqp.Connection, it could not be moved to another memory position for
shared connection purposes, so all shared connections points to a single RabbitMQConnection object.

The purpose of this abstraction is minimize the quantity of open amqp.Connection to RabbitMQ and at the same time make use of a keep alive
and reconnect technique.
*/
type RabbitMQConnection struct {
	env                        config.RABBITMQ_CONFIG
	isOpen                     bool
	Connection                 *amqp.Connection
	lastConnectionError        *amqp.Error
	closureNotificationChannel chan *amqp.Error
	Context                    context.Context
	CancelContext              context.CancelFunc
	ConnectionCount            uint64
	ConnectionId               uuid.UUID
	Mutex                      *sync.Mutex

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
		log.Panicf("RabbitMQConnection object have received a null logger dependency")
	}

	var connectionId uuid.UUID = uuid.New()

	return &RabbitMQConnection{
		env:                        env,
		Connection:                 nil,
		isOpen:                     false,
		lastConnectionError:        nil,
		closureNotificationChannel: nil,
		ConnectionCount:            0,
		ConnectionId:               connectionId,
		Mutex:                      &sync.Mutex{},

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
To terminate the connection, use CloseConnection() method, it will close the connection via context.Done().
*/
func (c *RabbitMQConnection) Connect(ctx context.Context) *RabbitMQConnection {
	var err error
	var connection *amqp.Connection

	if c.isOpen {
		return c
	}

	for {
		connection, err = amqp.Dial(c.env.PROTOCOL + "://" + c.env.USERNAME + ":" + c.env.PASSWORD + "@" + c.env.HOST + ":" + c.env.PORT + "/")
		if err != nil {
			c.logger.ErrorContext(ctx, "error creating a connection to RabbitMQ", slog.Any("error", err), c.logGroup)
			time.Sleep(time.Second)
			continue
		}

		c.updateConnection(connection)
		c.logger.InfoContext(ctx, "connection successfully opened", c.logGroup)

		c.isOpen = true

		c.Context, c.CancelContext = context.WithCancel(ctx)

		go c.keepConnection(ctx)

		return c
	}
}

/*
Refresh the closureNotificationChannel for healthiness.

Reference the newly created amqp.Connection, assuring assincronus concurrent access to multiple objects.

Refresh the connection id for control of references.
*/
func (c *RabbitMQConnection) updateConnection(connection *amqp.Connection) {
	c.closureNotificationChannel = connection.NotifyClose(make(chan *amqp.Error))

	c.Connection = connection
	c.ConnectionCount++
}

/*
Method for maintenance of a amqp.Connection.

It will close the connection if the RabbitMQConnection signal its closure.

It will reconnect if receive a signal of dropped connection.
*/
func (c *RabbitMQConnection) keepConnection(ctx context.Context) {
	select {
	case <-c.Context.Done():
		c.logger.InfoContext(ctx, "connection have been gracefully closed", c.logGroup)

	case closeNotification := <-c.closureNotificationChannel:
		c.isOpen = false
		c.Connection.Close()

		if closeNotification != nil {
			c.lastConnectionError = closeNotification
			c.logger.ErrorContext(ctx, "connection have been closed", slog.String("reason", closeNotification.Reason), slog.String("error", closeNotification.Error()), slog.Int("status", closeNotification.Code), c.logGroup)

		} else {
			c.logger.ErrorContext(ctx, "connection have been closed with no specified reason", c.logGroup)
		}

		c.Connect(ctx)
	}
}

/*
Method for closing the connection via context.

Keep in mind that this will affect all objects that shares connection with this one.
*/
func (c *RabbitMQConnection) CloseConnection(ctx context.Context) {
	c.logger.InfoContext(ctx, "closing connection at host", c.logGroup)

	c.isOpen = false

	if c.CancelContext != nil {
		c.CancelContext()
	}

	c.Connection.Close()

	c.Connection = nil
}

/*
Check the RabbitMQConnection availability.
*/
func (c *RabbitMQConnection) IsConnectionDown() bool {
	return !c.isOpen
}

/*
Block the process until the connection is open.
*/
func (c *RabbitMQConnection) WaitForConnection(ctx context.Context) {
	for {
		if c.isOpen {
			return
		}

		c.logger.InfoContext(ctx, "waiting for rabbitmq connection", c.logGroup)
		time.Sleep(500 * time.Millisecond)
	}
}
