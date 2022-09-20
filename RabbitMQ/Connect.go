package rabbitmq

import (
	"log"
	"time"

	"github.com/streadway/amqp"
)

/*
Connect to the RabbitMQ server.
*/
func (r *RabbitMQ) Connect() {
	errorFileIdentification := "RabbitMQ.go at Connect()"
	var err error

	for {
		r.Connection, err = amqp.Dial(r.serverAddress)
		if err != nil {
			log.Println("error creating a connection linked to RabbitMQ in " + errorFileIdentification + ": " + err.Error())
			time.Sleep(time.Second)
			continue
		}

		break
	}
}
