package rabbitmq

import (
	"log"
	"runtime"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

/*
Connect to the RabbitMQ server and open a goroutine for the connection maintance.

If terminanteOnConnectionError variable is true at RabbitMQ object, any problem with connection will cause a panic.
If false, it will retry connection on the same server every time it is lost.
*/
func (r *RabbitMQ) Connect() *RabbitMQ {
	errorFileIdentification := "RabbitMQ.go at Connect()"

	for {
		connection, err := amqp.Dial(r.Connection.serverAddress)
		if err != nil {
			if r.Connection.terminateOnConnectionError {
				completeMessage := "error creating a connection linked to RabbitMQ server '" + r.Connection.serverAddress + "' in " + errorFileIdentification + ": " + err.Error() + "\nStopping service as requested!"
				log.Panic(completeMessage)

			} else {
				completeError := "***ERROR*** error creating a connection linked to RabbitMQ server '" + r.Connection.serverAddress + "' in " + errorFileIdentification + ": " + err.Error()
				log.Println(completeError)
				time.Sleep(time.Second)
				continue
			}
		}

		log.Println("Successful connection with RabbitMQ server '" + r.Connection.serverAddress + "'")

		r.updateConnection(connection)

		go r.keepConnection()

		return r
	}
}

func (r *RabbitMQ) updateConnection(connection *amqp.Connection) {
	r.Connection.closureNotificationChannel = connection.NotifyClose(make(chan *amqp.Error))

	r.Connection.Connection = connection

	r.Connection.UpdatedConnectionId++

	r.Connection.isOpen = true
}

/*
Method for reconnection in case of RabbitMQ server drops.
*/
func (r *RabbitMQ) keepConnection() {
	errorFileIdentification := "RabbitMQ.go at keepConnection()"

	closeNotification := <-r.Connection.closureNotificationChannel

	if r.Connection.terminateOnConnectionError {
		completeMessage := "in " + errorFileIdentification + ": connection with RabbitMQ server '" + r.Connection.serverAddress + "' have closed with reason: '" + closeNotification.Reason + "'\nStopping service as requested!"
		log.Panic(completeMessage)

	} else {
		r.Connection.isOpen = false
		r.Connection.lastConnectionError = closeNotification
		log.Println("***ERROR*** in " + errorFileIdentification + ": connection with RabbitMQ server '" + r.Connection.serverAddress + "' have closed with reason: '" + closeNotification.Reason + "'")

		err := r.Connection.Connection.Close()
		if err != nil {
			completeError := "***ERROR*** error closing a connection linked to RabbitMQ in " + errorFileIdentification + ": " + err.Error()
			log.Println(completeError)
		}

		*r.Connection = ConnectionData{
			serverAddress:              r.Connection.serverAddress,
			UpdatedConnectionId:        r.Connection.UpdatedConnectionId,
			Connection:                 new(amqp.Connection),
			semaphore:                  r.Connection.semaphore,
			isOpen:                     false,
			terminateOnConnectionError: r.Connection.terminateOnConnectionError,
			lastConnectionError:        r.Connection.lastConnectionError,
			closureNotificationChannel: nil,
		}

		r.Connect()

		runtime.Goexit()
	}
}

func (r *RabbitMQ) isConnectionDown() bool {
	return !r.Connection.isOpen
}
