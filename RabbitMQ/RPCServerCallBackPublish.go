package rabbitmq

import (
	"errors"
	"log"
	"strconv"
	"time"

	"github.com/streadway/amqp"
)

/*
Publish a response message at Remote Procedure Call response queue.
*/
func (r *RabbitMQ) RPCServerCallbackPublish(body string, correlationId string, callbackQueueName string) (err error) {
	errorFileIdentification := "RabbitMQ.go at RPCServerCallbackPublish()"

	publisher := r.MakeCopy()

	publisher.semaphore.GetPermission(1)
	defer publisher.semaphore.ReleasePermission(1)
	publisher.Connect()
	defer publisher.Connection.Close()

	err = publisher.RemoteProcedureCallData.RPCServer.Callback.prepareChannel(publisher)
	if err != nil {
		return errors.New("error preparing channel in " + errorFileIdentification + ": " + err.Error())
	}
	defer publisher.RemoteProcedureCallData.RPCServer.Callback.Channel.Close()

	err = publisher.RemoteProcedureCallData.RPCServer.Callback.prepareQueue(publisher)
	if err != nil {
		return errors.New("error preparing queue in " + errorFileIdentification + ": " + err.Error())
	}

	err = publisher.RemoteProcedureCallData.RPCServer.Callback.Channel.Confirm(false)
	if err != nil {
		return errors.New("error configuring channel with Confirm() protocol in " + errorFileIdentification + ": " + err.Error())
	}

	notifyFlowChannel := publisher.RemoteProcedureCallData.RPCServer.Callback.Channel.NotifyFlow(make(chan bool))
	notifyAck, notifyNack := publisher.RemoteProcedureCallData.RPCServer.Callback.Channel.NotifyConfirm(make(chan uint64), make(chan uint64))

	message := amqp.Publishing{
		ContentType:   "application/json",
		Body:          []byte(body),
		DeliveryMode:  amqp.Persistent,
		ReplyTo:       r.RemoteProcedureCallData.RPCServer.Callback.QueueName,
		CorrelationId: correlationId,
	}

	for {
		select {
		case <-notifyFlowChannel:
			waitingTimeForFlow := 10 * time.Second
			log.Println("Queue '" + publisher.RemoteProcedureCallData.RPCServer.Callback.QueueName + "' flow is closed, waiting " + waitingTimeForFlow.String() + " seconds to try publish again.")
			time.Sleep(waitingTimeForFlow)
			continue

		default:
			err = publisher.RemoteProcedureCallData.RPCServer.Callback.Channel.Publish(r.RemoteProcedureCallData.RPCServer.Callback.ExchangeName, publisher.RemoteProcedureCallData.RPCServer.Callback.AccessKey, true, false, message)
			if err != nil {
				return errors.New("error publishing message in " + errorFileIdentification + ": " + err.Error())
			}

			select {
			case deniedNack := <-notifyNack:
				waitingTimeForRedelivery := 10 * time.Second
				log.Println("Publishing Nack" + strconv.Itoa(int(deniedNack)) + " denied by '" + publisher.RemoteProcedureCallData.RPCServer.Callback.QueueName + "' queue, waiting " + waitingTimeForRedelivery.String() + " seconds to try redeliver.")
				time.Sleep(waitingTimeForRedelivery)
				continue

			case successAck := <-notifyAck:
				log.Println("Publishing Ack" + strconv.Itoa(int(successAck)) + " recieved at '" + publisher.RemoteProcedureCallData.RPCServer.Callback.QueueName + "'.")
				return nil
			}
		}
	}
}
