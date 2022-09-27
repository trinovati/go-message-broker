package rabbitmq

import (
	"log"
	"time"

	"github.com/streadway/amqp"
)

/*
Connect to the RabbitMQ server.

if terminanteOnConnectionError variable is true at RabbitMQ object, any problem with connection will cause a panic.
if false, it will retry connection on the same server every time it is lost.
*/
func (r *RabbitMQ) Connect() {
	errorFileIdentification := "RabbitMQ.go at Connect()"

	for {
		err := r.connect()
		if err != nil {
			completeError := "***ERROR*** error creating a connection linked to RabbitMQ in " + errorFileIdentification + ": " + err.Error()

			if r.terminanteOnConnectionError {
				log.Panic(completeError)

			} else {
				log.Println(completeError)
				time.Sleep(time.Second)
				continue
			}
		}

		r.keepConnection()
	}
}

/*
Method for reconnection in case of RabbitMQ server drops.
*/
func (r *RabbitMQ) keepConnection() {
	errorFileIdentification := "RabbitMQ.go at keepConnection()"

	closeNotifyChannel := r.Connection.NotifyClose(make(chan *amqp.Error))

	for closeNotification := range closeNotifyChannel {
		completeError := "***ERROR*** in " + errorFileIdentification + ": connection with RabbitMQ server '" + r.serverAddress + "' have closed with reason: '" + closeNotification.Reason + "'"

		if r.terminanteOnConnectionError {
			log.Panic(completeError)

		} else {
			log.Println(completeError)

			err := r.Connection.Close()
			if err != nil {
				completeError = "***ERROR*** error closing a connection linked to RabbitMQ in " + errorFileIdentification + ": " + err.Error()
				log.Println(completeError)
			}

			for {
				err = r.connect()
				if err != nil {
					completeError = "***ERROR*** error creating a connection linked to RabbitMQ in " + errorFileIdentification + ": " + err.Error()
					log.Println(completeError)
					time.Sleep(time.Second)
					continue
				}

				r.Connection.NotifyClose(closeNotifyChannel)

				break
			}
		}
	}
}

/*
Dial for the server, storring the connection on the RabbitMQ object and returning nil in case of success.
*/
func (r *RabbitMQ) connect() (err error) {
	r.Connection, err = amqp.Dial(r.serverAddress)

	return err
}
