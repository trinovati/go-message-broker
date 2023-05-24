package rabbitmqconnection

import (
	"context"
	"log"
	"runtime"
	"strconv"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"gitlab.com/aplicacao/trinovati-connector-message-brokers/RabbitMQ/config"
)

/*
Object used to reference a amqp.Connection and store all the data needed to keep track of its health.
*/
type ConnectionData struct {
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
	ConnectionId               uint64
}

/*
Build an object used to reference a amqp.Connection and store all the data needed to keep track of its health.
*/
func NewConnectionData() *ConnectionData {
	connectionContext, cancelContext := context.WithCancel(context.Background())

	protocol := config.RABBITMQ_PROTOCOL
	user := config.RABBITMQ_USERNAME
	password := config.RABBITMQ_PASSWORD
	ServerAddress := config.RABBITMQ_HOST
	port := config.RABBITMQ_PORT

	return &ConnectionData{
		protocol:                   protocol,
		user:                       user,
		password:                   password,
		ServerAddress:              ServerAddress,
		port:                       port,
		Connection:                 nil,
		isOpen:                     false,
		lastConnectionError:        nil,
		closureNotificationChannel: nil,
		Context:                    connectionContext,
		CancelContext:              cancelContext,
		ConnectionId:               0,
	}
}

func (c *ConnectionData) SetConnectionString(connectionString string) *ConnectionData {
	c.protocol = config.RABBITMQ_PROTOCOL

	c.user = strings.Split(strings.Split(connectionString, "@")[0], ":")[0]
	c.password = strings.Split(strings.Split(connectionString, "@")[0], ":")[1]
	c.ServerAddress = strings.Split(strings.Split(connectionString, "@")[1], ":")[0]
	c.port = strings.Split(strings.Split(connectionString, "@")[1], ":")[1]

	return c
}

/*
Connect to the RabbitMQ server and open a goroutine for the connection maintance.

If terminanteOnConnectionError is true at RabbitMQ object, any problem with connection will cause a panic.
If false, it will retry connection on the same server every time it is lost.

It is safe to share connection by multiple objects.
*/
func (c *ConnectionData) Connect() *ConnectionData {
	errorFileIdentification := "RabbitMQ at Connect()"

	for {
		connection, err := amqp.Dial(c.protocol + "://" + c.user + ":" + c.password + "@" + c.ServerAddress + ":" + c.port + "/")
		if err != nil {

			completeError := "***ERROR*** error creating a connection linked to RabbitMQ server '" + c.ServerAddress + "' in " + errorFileIdentification + ": " + err.Error()
			log.Println(completeError)
			time.Sleep(time.Second)
			continue
		}

		time.Sleep(2 * time.Second)

		c.updateConnection(connection)
		log.Println("Successful RabbitMQ connection with id " + strconv.FormatUint(c.ConnectionId, 10) + " at server '" + c.ServerAddress + "'")

		c.isOpen = true

		go c.keepConnection()

		return c
	}
}

/*
Refresh the closureNotificationChannel for helthyness.

Reference the newly created amqp.Connection, assuring assincronus concurrent access to multiple objects.

Refresh the connection id for controll of references.
*/
func (c *ConnectionData) updateConnection(connection *amqp.Connection) {
	c.closureNotificationChannel = connection.NotifyClose(make(chan *amqp.Error))

	c.Connection = connection
	c.ConnectionId++
}

/*
Method for reconnection in case of RabbitMQ server drops.
*/
func (c *ConnectionData) keepConnection() {
	errorFileIdentification := "RabbitMQ at keepConnection()"

	select {
	case <-c.Context.Done():
		break

	case closeNotification := <-c.closureNotificationChannel:
		c.isOpen = false
		c.Connection.Close()

		var closureReason string = ""
		if closeNotification != nil {
			c.lastConnectionError = closeNotification
			closureReason = closeNotification.Reason
		}
		log.Println("***ERROR*** in " + errorFileIdentification + ": connection with RabbitMQ server '" + c.ServerAddress + "' have closed with reason: '" + closureReason + "'")

		c.Connect()
	}

	runtime.Goexit()
}

/*
Method for closing the connection via context, sending  signal for all objects sharring connection to terminate its process.
*/
func (c *ConnectionData) CloseConnection() {
	c.isOpen = false

	c.CancelContext()
}

/*
Check the connection, returning true if its down and unavailble.
*/
func (c *ConnectionData) IsConnectionDown() bool {
	return !c.isOpen
}

/*
Block the process until the connection is open.
*/
func (c *ConnectionData) WaitForConnection() {
	for {
		if c.isOpen {
			return
		}

		log.Println("waiting for rabbitmq connection")
		time.Sleep(500 * time.Millisecond)
	}
}
