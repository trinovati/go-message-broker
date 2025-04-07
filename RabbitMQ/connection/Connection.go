package connection

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"runtime"
	"sync"
	"time"

	"github.com/trinovati/go-message-broker/RabbitMQ/config"
	"github.com/trinovati/go-message-broker/RabbitMQ/interfaces"

	amqp "github.com/rabbitmq/amqp091-go"
)

/*
Object used to reference a amqp.Connection and store all the data needed to keep track of its health.
*/
type Connection struct {
	protocol                   string
	user                       string
	password                   string
	ServerAddress              string
	port                       string
	isOpen                     bool
	Connection                 *amqp.Connection
	lastConnectionError        *amqp.Error
	closureNotificationChannel chan *amqp.Error
	Context                    context.Context
	CancelContext              context.CancelFunc
	Mutex                      *sync.Mutex
	ConnectionId               uint64
}

/*
Build an object used to reference a amqp.Connection and store all the data needed to keep track of its health.
*/
func NewConnection() *Connection {
	var protocol string = config.RABBITMQ_PROTOCOL
	var user string = url.QueryEscape(config.RABBITMQ_USERNAME)
	var password string = url.QueryEscape(config.RABBITMQ_PASSWORD)
	var ServerAddress string = url.QueryEscape(config.RABBITMQ_HOST)
	var port string = url.QueryEscape(config.RABBITMQ_PORT)

	return &Connection{
		protocol:                   protocol,
		user:                       user,
		password:                   password,
		ServerAddress:              ServerAddress,
		port:                       port,
		Connection:                 nil,
		isOpen:                     false,
		lastConnectionError:        nil,
		closureNotificationChannel: nil,
		ConnectionId:               0,
		Mutex:                      &sync.Mutex{},
	}
}

func (c *Connection) WithConnectionData(host string, port string, username string, password string) interfaces.Connection {
	c.protocol = config.RABBITMQ_PROTOCOL

	c.user = url.QueryEscape(username)
	c.password = url.QueryEscape(password)
	c.ServerAddress = url.QueryEscape(host)
	c.port = url.QueryEscape(port)

	return c
}

func (c Connection) Id() uint64 {
	return c.ConnectionId
}

/*
Connect to the RabbitMQ server and open a goroutine for the connection maintance.

If terminanteOnConnectionError is true at RabbitMQ object, any problem with connection will cause a panic.
If false, it will retry connection on the same server every time it is lost.

It is safe to share connection by multiple objects.
*/
func (c *Connection) Connect() interfaces.Connection {
	var err error
	var connection *amqp.Connection

	if c.isOpen {
		return c
	}

	for {
		connection, err = amqp.Dial(c.protocol + "://" + c.user + ":" + c.password + "@" + c.ServerAddress + ":" + c.port + "/")
		if err != nil {
			config.Error.Wrap(err, "error creating a connection linked to RabbitMQ server '"+c.ServerAddress+"'").Print()
			time.Sleep(time.Second)
			continue
		}

		c.updateConnection(connection)
		log.Printf("Successfully opened connection id '%d' at server '%s'", c.ConnectionId, c.ServerAddress)

		c.isOpen = true

		c.Context, c.CancelContext = context.WithCancel(context.Background())

		go c.keepConnection(c.Context)

		return c
	}
}

/*
Refresh the closureNotificationChannel for helthyness.

Reference the newly created amqp.Connection, assuring assincronus concurrent access to multiple objects.

Refresh the connection id for controll of references.
*/
func (c *Connection) updateConnection(connection *amqp.Connection) {
	c.closureNotificationChannel = connection.NotifyClose(make(chan *amqp.Error))

	c.Connection = connection
	c.ConnectionId++
}

/*
Method for reconnection in case of RabbitMQ server drops.
*/
func (c *Connection) keepConnection(ctx context.Context) {
	select {
	case <-ctx.Done():
		log.Printf("connection context of connection id '%d' at server '%s' have been closed", c.ConnectionId, c.ServerAddress)

	case closeNotification := <-c.closureNotificationChannel:
		c.isOpen = false
		c.Connection.Close()

		if closeNotification != nil {
			c.lastConnectionError = closeNotification
			config.Error.New(fmt.Sprintf("connection of connection id '%d' at server '%s' have closed with\nreason: '%s'\nerror: '%s'\nstatus code: '%d'", c.ConnectionId, c.ServerAddress, closeNotification.Reason, closeNotification.Error(), closeNotification.Code)).Print()

		} else {
			config.Error.New(fmt.Sprintf("connection of connection id '%d' at server '%s' have closed with no specified reason", c.ConnectionId, c.ServerAddress)).Print()
		}

		c.Connect()
	}

	runtime.Goexit()
}

/*
Method for closing the connection via context, sending  signal for all objects sharring connection to terminate its process.
*/
func (c *Connection) CloseConnection() {
	c.isOpen = false

	if c.CancelContext != nil {
		c.CancelContext()
	}

	c.Connection = nil
}

/*
Check the connection, returning true if its down and unavailble.
*/
func (c *Connection) IsConnectionDown() bool {
	return !c.isOpen
}

/*
Block the process until the connection is open.
*/
func (c *Connection) WaitForConnection() {
	for {
		if c.isOpen {
			return
		}

		log.Println("waiting for rabbitmq connection")
		time.Sleep(500 * time.Millisecond)
	}
}
