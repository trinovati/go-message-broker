package connection

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"sync"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/trinovati/go-message-broker/v2/RabbitMQ/config"
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
}

/*
Builder of RabbitMQConnection object.
*/
func NewRabbitMQConnection(env config.RABBITMQ_CONFIG) *RabbitMQConnection {
	return &RabbitMQConnection{
		env:                        env,
		Connection:                 nil,
		isOpen:                     false,
		lastConnectionError:        nil,
		closureNotificationChannel: nil,
		ConnectionCount:            0,
		ConnectionId:               uuid.New(),
		Mutex:                      &sync.Mutex{},
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
func (c *RabbitMQConnection) Connect() *RabbitMQConnection {
	var err error
	var connection *amqp.Connection

	if c.isOpen {
		return c
	}

	for {
		connection, err = amqp.Dial(c.env.RABBITMQ_PROTOCOL + "://" + c.env.RABBITMQ_USERNAME + ":" + c.env.RABBITMQ_PASSWORD + "@" + c.env.RABBITMQ_HOST + ":" + c.env.RABBITMQ_PORT + "/")
		if err != nil {
			log.Printf(fmt.Sprintf("error creating a connection to RabbitMQ server: %s: %s\n", c.env.RABBITMQ_HOST, err))
			time.Sleep(time.Second)
			continue
		}

		c.updateConnection(connection)
		log.Printf("opened connection id %s at server %s\n", c.ConnectionId.String(), c.env.RABBITMQ_HOST)

		c.isOpen = true

		c.Context, c.CancelContext = context.WithCancel(context.Background())

		go c.keepConnection(c.Context)

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
	case <-ctx.Done():
		log.Printf("connection context of connection id %s at server %s have been closed\n", c.ConnectionId.String(), c.env.RABBITMQ_HOST)

	case closeNotification := <-c.closureNotificationChannel:
		c.isOpen = false
		c.Connection.Close()

		if closeNotification != nil {
			c.lastConnectionError = closeNotification
			log.Printf("connection id %s at server %s have closed with\nreason: '%s'\nerror: '%s'\nstatus code: '%d'\n", c.ConnectionId.String(), c.env.RABBITMQ_HOST, closeNotification.Reason, closeNotification.Error(), closeNotification.Code)

		} else {
			log.Printf("connection id %s at server %s have closed with no specified reason\n", c.ConnectionId.String(), c.env.RABBITMQ_HOST)
		}

		c.Connect()
	}

	runtime.Goexit()
}

/*
Method for closing the connection via context.

Keep in mind that this will affect all objects that shares connection with this one.
*/
func (c *RabbitMQConnection) CloseConnection() {
	c.isOpen = false

	if c.CancelContext != nil {
		c.CancelContext()
	}

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
func (c *RabbitMQConnection) WaitForConnection() {
	for {
		if c.isOpen {
			return
		}

		log.Println("waiting for rabbitmq connection")
		time.Sleep(500 * time.Millisecond)
	}
}
