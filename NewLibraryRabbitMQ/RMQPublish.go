package rabbitmq

import (
	"errors"
	"log"
	"time"
)

/*
Object that holds information needed for publishing into RabbitMQ.
*/
type RMQPublish struct {
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

func (r *RabbitMQ) preparePublisher() (notifyFlowChannel chan bool) {
	errorFileIdentification := "RMQPublish.go at preparePublisher()"

	for {
		UpdatedConnectionId := r.Connection.UpdatedConnectionId

		err := r.PublishData.prepareChannel(r)
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

		r.ConnectionId = UpdatedConnectionId

		return notifyFlowChannel
	}
}

/*
Prepare a channel linked to RabbitMQ connection for publishing.

In case of unexistent exchange, it will create the exchange.
*/
func (p *RMQPublish) prepareChannel(rabbitmq *RabbitMQ) (err error) {
	errorFileIdentification := "RMQPublish.go at prepareChannel()"

	if rabbitmq.Channel == nil || rabbitmq.Channel.Channel == nil || rabbitmq.Channel.Channel.IsClosed() {

		if rabbitmq.isConnectionDown() {
			log.Println("CONEXAO REFEITA AQUI")
			completeError := "in " + errorFileIdentification + ": connection dropped before creating channel, trying again soon"
			return errors.New(completeError)
		} else {
			log.Println("CONEXAO NAO PRECISOU SER REFEITA")
		}

		channel, err := rabbitmq.Connection.Connection.Channel()
		if err != nil {
			return errors.New("error creating a channel linked to RabbitMQ in " + errorFileIdentification + ": " + err.Error())
		}
		rabbitmq.Channel.Channel = channel
		log.Println("CANAL REFEITO AQUI")

		if rabbitmq.isConnectionDown() {
			return errors.New("in " + errorFileIdentification + ": connection dropped before configuring channel, trying again soon")
		}

		err = rabbitmq.Channel.Channel.Confirm(false)
		if err != nil {
			return errors.New("error configuring channel with Confirm() protocol in " + errorFileIdentification + ": " + err.Error())
		}

	} else {
		log.Println("CANAL E CONEXAO NAO PRECISOU SER REFEITA")
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
		return errors.New("created queue name and expected queue name are diferent in " + errorFileIdentification + "")
	}

	err = rabbitmq.Channel.Channel.QueueBind(p.QueueName, p.AccessKey, p.ExchangeName, false, nil)
	if err != nil {
		return errors.New("error binding queue in " + errorFileIdentification + ": " + err.Error())
	}

	return nil
}
