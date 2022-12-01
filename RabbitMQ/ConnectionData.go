package rabbitmq

import (
	"context"
	"log"
	"runtime"
	"strconv"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

/*
Object used to reference a amqp.Connection and store all the data needed to keep track of its health.
*/
type ConnectionData struct {
	serverAddress              string
	terminateOnConnectionError bool
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
func newConnectionData() *ConnectionData {
	connectionContext, cancelContext := context.WithCancel(context.Background())

	return &ConnectionData{
		serverAddress:              RABBITMQ_SERVER,
		Connection:                 &amqp.Connection{},
		isOpen:                     false,
		terminateOnConnectionError: false,
		lastConnectionError:        nil,
		closureNotificationChannel: nil,
		Context:                    connectionContext,
		CancelContext:              cancelContext,
		ConnectionId:               0,
	}
}

/*
Connect to the RabbitMQ server and open a goroutine for the connection maintance.

If terminanteOnConnectionError is true at RabbitMQ object, any problem with connection will cause a panic.
If false, it will retry connection on the same server every time it is lost.

It is safe to share connection by multiple objects.
*/
func (r *RabbitMQ) Connect() *RabbitMQ {
	errorFileIdentification := "RabbitMQ.go at Connect()"

	serverAddress := strings.Split(strings.Split(r.Connection.serverAddress, "@")[1], ":")[0]

	for {
		connection, err := amqp.Dial(r.Connection.serverAddress)
		if err != nil {

			if r.Connection.terminateOnConnectionError {
				completeMessage := "error creating a connection linked to RabbitMQ server '" + serverAddress + "' in " + errorFileIdentification + ": " + err.Error() + "\nStopping service as requested!"
				log.Panic(completeMessage)

			} else {
				completeError := "***ERROR*** error creating a connection linked to RabbitMQ server '" + serverAddress + "' in " + errorFileIdentification + ": " + err.Error()
				log.Println(completeError)
				time.Sleep(time.Second)
				continue
			}
		}

		time.Sleep(2 * time.Second)

		r.Connection.updateConnection(connection)
		log.Println("Successful RabbitMQ connection with id " + strconv.FormatUint(r.Connection.ConnectionId, 10) + " at server '" + serverAddress + "'")

		r.Connection.isOpen = true

		go r.keepConnection()

		return r
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
func (r *RabbitMQ) keepConnection() {
	errorFileIdentification := "RabbitMQ.go at keepConnection()"

	serverAddress := strings.Split(strings.Split(r.Connection.serverAddress, "@")[1], ":")[0]

	select {
	case <-r.Connection.Context.Done():
		break

	case closeNotification := <-r.Connection.closureNotificationChannel:
		if r.Connection.terminateOnConnectionError {
			completeMessage := "in " + errorFileIdentification + ": connection with RabbitMQ server '" + serverAddress + "' have closed with reason: '" + closeNotification.Reason + "'\nStopping service as requested!"
			log.Panic(completeMessage)

		} else {
			r.Connection.isOpen = false
			r.Connection.lastConnectionError = closeNotification
			log.Println("***ERROR*** in " + errorFileIdentification + ": connection with RabbitMQ server '" + serverAddress + "' have closed with reason: '" + closeNotification.Reason + "'")

			err := r.Connection.Connection.Close()
			if err != nil {
				completeError := "***ERROR*** error closing a connection linked to RabbitMQ in " + errorFileIdentification + ": " + err.Error()
				log.Println(completeError)
			}

			*r.Connection = ConnectionData{
				serverAddress:              r.Connection.serverAddress,
				Connection:                 &amqp.Connection{},
				isOpen:                     false,
				terminateOnConnectionError: r.Connection.terminateOnConnectionError,
				lastConnectionError:        r.Connection.lastConnectionError,
				closureNotificationChannel: nil,
				Context:                    r.Connection.Context,
				ConnectionId:               r.Connection.ConnectionId,
			}

			r.Connect()
		}
	}

	runtime.Goexit()
}

/*
Method for closing the connection via context, sending  signal for all objects sharring connection to terminate its process.
*/
func (r *RabbitMQ) CloseConnection() {
	r.Connection.CancelContext()

	r.Connection.Connection.Close()
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
