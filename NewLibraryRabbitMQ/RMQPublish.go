package rabbitmq

import (
	"errors"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

/*
Object that holds information needed for publishing into RabbitMQ.
*/
type RMQPublish struct {
	Channel           *amqp.Channel
	notifyFlowChannel *chan bool
	ExchangeName      string
	ExchangeType      string
	QueueName         string
	AccessKey         string
}

/*
Create a new object that can hold all information needed to consume from a RabbitMQ queue.
*/
func newRMQPublish() *RMQPublish {
	return &RMQPublish{
		Channel:           nil,
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

func (p *RMQPublish) preparePublisher(rabbitmq *RabbitMQ) (err error) {
	errorFileIdentification := "RMQPublish.go at preparePublisher()"

	for {
		err = p.prepareChannel(rabbitmq)
		if err != nil {
			log.Println("***ERROR*** error preparing channel in " + errorFileIdentification + ": " + err.Error())
			time.Sleep(time.Second)
			continue
		}

		err = p.prepareQueue(rabbitmq)
		if err != nil {
			log.Println("***ERROR*** error preparing queue in " + errorFileIdentification + ": " + err.Error())
			time.Sleep(time.Second)
			continue
		}

		return nil
	}
}

/*
Prepare a channel linked to RabbitMQ connection for publishing.

In case of unexistent exchange, it will create the exchange.
*/
func (p *RMQPublish) prepareChannel(rabbitmq *RabbitMQ) (err error) {
	errorFileIdentification := "RMQPublish.go at prepareChannel()"

	if p.Channel == nil || p.Channel.IsClosed() {
		if rabbitmq.isConnectionDown() {
			return errors.New("in " + errorFileIdentification + ": connection dropped before creating publish channel, trying again soon")
		}

		p.Channel, err = rabbitmq.Connection.Connection.Channel()
		if err != nil {
			return errors.New("error creating a channel linked to RabbitMQ in " + errorFileIdentification + ": " + err.Error())
		}

		if rabbitmq.isConnectionDown() {
			return errors.New("in " + errorFileIdentification + ": connection dropped before configuring publish channel, trying again soon")
		}

		err = p.Channel.Confirm(false)
		if err != nil {
			return errors.New("error configuring channel with Confirm() protocol in " + errorFileIdentification + ": " + err.Error())
		}

		notifyFlowChannel := p.Channel.NotifyFlow(make(chan bool))

		p.notifyFlowChannel = &notifyFlowChannel
	}

	if rabbitmq.isConnectionDown() {
		return errors.New("in " + errorFileIdentification + ": connection dropped before declaring exchange, trying again soon")
	}

	err = p.Channel.ExchangeDeclare(p.ExchangeName, p.ExchangeType, true, false, false, false, nil)
	if err != nil {
		return errors.New("error creating RabbitMQ exchange in " + errorFileIdentification + ": " + err.Error())
	}

	return nil
}

/*
Prepare a queue linked to RabbitMQ channel for publishing.

In case of unexistent queue, it will create the queue.

In case of queue not beeing binded to any exchange, it will bind it to a exchange.
*/
func (p *RMQPublish) prepareQueue(rabbitmq *RabbitMQ) (err error) {
	errorFileIdentification := "RMQPublish.go at prepareQueue()"

	queue, err := p.Channel.QueueDeclare(p.QueueName, true, false, false, false, nil)
	if err != nil {
		return errors.New("error creating queue in " + errorFileIdentification + ": " + err.Error())
	}

	if queue.Name != p.QueueName {
		return errors.New("created queue name and expected queue name are diferent in " + errorFileIdentification + "")
	}

	err = p.Channel.QueueBind(p.QueueName, p.AccessKey, p.ExchangeName, false, nil)
	if err != nil {
		return errors.New("error binding queue in " + errorFileIdentification + ": " + err.Error())
	}

	return nil
}
