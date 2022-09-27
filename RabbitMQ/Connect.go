package rabbitmq

import (
	"log"
	"time"

	"github.com/streadway/amqp"
)

/*
Connect to the RabbitMQ server and open a goroutine for the connection maintance.

If terminanteOnConnectionError variable is true at RabbitMQ object, any problem with connection will cause a panic.
If false, it will retry connection on the same server every time it is lost.
*/
func (r *RabbitMQ) Connect() {
	errorFileIdentification := "RabbitMQ.go at Connect()"

	for {
		err := r.connect()
		if err != nil {

			if r.terminanteOnConnectionError {
				completeMessage := "error creating a connection linked to RabbitMQ server '" + r.serverAddress + "' in " + errorFileIdentification + ": " + err.Error() + "\nStopping service as requested!"
				log.Panic(completeMessage)

			} else {
				completeError := "***ERROR*** error creating a connection linked to RabbitMQ server '" + r.serverAddress + "' in " + errorFileIdentification + ": " + err.Error()
				log.Println(completeError)
				time.Sleep(time.Second)
				continue
			}
		}

		log.Println("Successful connection with RabbitMQ server '" + r.serverAddress + "'")

		go r.keepConnection()
	}
}

/*
Method for reconnection in case of RabbitMQ server drops.
*/
func (r *RabbitMQ) keepConnection() {
	errorFileIdentification := "RabbitMQ.go at keepConnection()"

	closeNotifyChannel := r.Connection.NotifyClose(make(chan *amqp.Error))

	for closeNotification := range closeNotifyChannel {

		if r.terminanteOnConnectionError {
			completeMessage := "in " + errorFileIdentification + ": connection with RabbitMQ server '" + r.serverAddress + "' have closed with reason: '" + closeNotification.Reason + "'\nStopping service as requested!"
			log.Panic(completeMessage)

		} else {
			completeError := "***ERROR*** in " + errorFileIdentification + ": connection with RabbitMQ server '" + r.serverAddress + "' have closed with reason: '" + closeNotification.Reason + "'"
			log.Println(completeError)

			err := r.Connection.Close()
			if err != nil {
				completeError = "***ERROR*** error closing a connection linked to RabbitMQ in " + errorFileIdentification + ": " + err.Error()
				log.Println(completeError)
			}

			for {
				err = r.connect()
				if err != nil {
					completeError = "***ERROR*** error creating a connection linked to RabbitMQ server '" + r.serverAddress + "' in " + errorFileIdentification + ": " + err.Error()
					log.Println(completeError)
					time.Sleep(time.Second)
					continue
				}

				r.Connection.NotifyClose(closeNotifyChannel)

				log.Println("Successful reconnected with RabbitMQ server '" + r.serverAddress + "'")

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
