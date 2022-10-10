package rabbitmq

import (
	"errors"
	"log"
	"time"
)

/*
Object that holds all information needed for publishing into a RabbitMQ queue.
*/
type RMQPublish struct {
	notifyFlowChannel *chan bool
	ExchangeName      string
	ExchangeType      string
	QueueName         string
	AccessKey         string
}

/*
Builds a new object that holds all information needed for publishing into a RabbitMQ queue.
*/
func newRMQPublish() *RMQPublish {
	return &RMQPublish{
		notifyFlowChannel: nil,
	}
}

/*
Insert data into the object used for RabbitMQ queue publish.
*/
func (p *RMQPublish) populate(exchangeName string, exchangeType string, QueueName string, AccessKey string) {
	p.ExchangeName = exchangeName
	p.ExchangeType = exchangeType
	p.QueueName = QueueName
	p.AccessKey = AccessKey
}

/*
Will create a connection, prepare channels, declare queue and exchange case needed.

If an error occurs, it will restart and retry all the process until the publisher is fully prepared.

Return a channel of flow notifications.
*/
func (r *RabbitMQ) preparePublisher() (notifyFlowChannel chan bool) {
	errorFileIdentification := "RMQPublish.go at preparePublisher()"

	for {
		err := r.prepareChannel()
		if err != nil {
			log.Println("***ERROR*** error preparing channel in " + errorFileIdentification + ": " + err.Error())
			time.Sleep(time.Second)
			continue
		}

		if r.isConnectionDown() {
			log.Println("***ERROR*** in " + errorFileIdentification + ": connection dropped before preparing notify flow channel, trying again soon")
			time.Sleep(time.Second)
			continue
		}

		notifyFlowChannel := r.Channel.Channel.NotifyFlow(make(chan bool))

		err = r.PublishData.prepareQueue(r)
		if err != nil {
			log.Println("***ERROR*** error preparing queue in " + errorFileIdentification + ": " + err.Error())
			time.Sleep(time.Second)
			continue
		}

		return notifyFlowChannel
	}
}

/*
Prepare a queue linked to RabbitMQ channel for publishing.

In case of unexistent exchange, it will create the exchange.

In case of unexistent queue, it will create the queue.

In case of queue not beeing binded to any exchange, it will bind it to a exchange.
*/
func (p *RMQPublish) prepareQueue(rabbitmq *RabbitMQ) (err error) {
	errorFileIdentification := "RMQPublish.go at prepareQueue()"

	if rabbitmq.isConnectionDown() {
		completeError := "in " + errorFileIdentification + ": connection dropped before declaring exchange, trying again soon"
		return errors.New(completeError)
	}

	err = rabbitmq.Channel.Channel.ExchangeDeclare(p.ExchangeName, p.ExchangeType, true, false, false, false, nil)
	if err != nil {
		return errors.New("error creating RabbitMQ exchange in " + errorFileIdentification + ": " + err.Error())
	}

	queue, err := rabbitmq.Channel.Channel.QueueDeclare(p.QueueName, true, false, false, false, nil)
	if err != nil {
		return errors.New("error creating queue in " + errorFileIdentification + ": " + err.Error())
	}

	if queue.Name != p.QueueName {
		return errors.New("in " + errorFileIdentification + ": created queue name '" + queue.Name + "' and expected queue name '" + p.QueueName + "' are diferent")
	}

	err = rabbitmq.Channel.Channel.QueueBind(p.QueueName, p.AccessKey, p.ExchangeName, false, nil)
	if err != nil {
		return errors.New("error binding queue in " + errorFileIdentification + ": " + err.Error())
	}

	return nil
}
