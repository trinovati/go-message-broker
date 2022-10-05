package rabbitmq

// import (
// 	"errors"
// 	"log"
// 	"strconv"
// 	"time"

// 	"github.com/streadway/amqp"
// )

// /*
// Publish a request message at Remote Procedure Call request queue with a produced correlationId returned.
// */
// func (r *RabbitMQ) RPCClientRequestPublish(body string) (correlationId string, err error) {
// 	errorFileIdentification := "RabbitMQ.go at RPCClientRequestPublish()"

// 	publisher := r.MakeCopy()

// 	publisher.semaphore.GetPermission(1)
// 	defer publisher.semaphore.ReleasePermission(1)
// 	publisher.Connect()
// 	defer publisher.Connection.Close()

// 	err = publisher.RemoteProcedureCallData.RPCClient.Publisher.prepareChannel(publisher)
// 	if err != nil {
// 		return "", errors.New("error preparing channel in " + errorFileIdentification + ": " + err.Error())
// 	}
// 	defer publisher.RemoteProcedureCallData.RPCClient.Publisher.Channel.Close()

// 	err = publisher.RemoteProcedureCallData.RPCClient.Publisher.prepareQueue(publisher)
// 	if err != nil {
// 		return "", errors.New("error preparing queue in " + errorFileIdentification + ": " + err.Error())
// 	}

// 	err = publisher.RemoteProcedureCallData.RPCClient.Publisher.Channel.Confirm(false)
// 	if err != nil {
// 		return "", errors.New("error configuring channel with Confirm() protocol in " + errorFileIdentification + ": " + err.Error())
// 	}

// 	notifyFlowChannel := publisher.RemoteProcedureCallData.RPCClient.Publisher.Channel.NotifyFlow(make(chan bool))
// 	notifyAck, notifyNack := publisher.RemoteProcedureCallData.RPCClient.Publisher.Channel.NotifyConfirm(make(chan uint64), make(chan uint64))

// 	correlationId = r.RemoteProcedureCallData.RPCClient.TagProducerManager.MakeUniqueTag()
// 	message := amqp.Publishing{
// 		ContentType:   "application/json",
// 		Body:          []byte(body),
// 		DeliveryMode:  amqp.Persistent,
// 		ReplyTo:       r.RemoteProcedureCallData.RPCClient.Callback.QueueName,
// 		CorrelationId: correlationId,
// 	}

// 	for {
// 		select {
// 		case <-notifyFlowChannel:
// 			waitingTimeForFlow := 10 * time.Second
// 			log.Println("Queue '" + publisher.RemoteProcedureCallData.RPCClient.Publisher.QueueName + "' flow is closed, waiting " + waitingTimeForFlow.String() + " seconds to try publish again.")
// 			time.Sleep(waitingTimeForFlow)
// 			continue

// 		default:
// 			err = publisher.RemoteProcedureCallData.RPCClient.Publisher.Channel.Publish(r.RemoteProcedureCallData.RPCClient.Publisher.ExchangeName, publisher.RemoteProcedureCallData.RPCClient.Publisher.AccessKey, true, false, message)
// 			if err != nil {
// 				return "", errors.New("error publishing message in " + errorFileIdentification + ": " + err.Error())
// 			}

// 			select {
// 			case deniedNack := <-notifyNack:
// 				waitingTimeForRedelivery := 10 * time.Second
// 				log.Println("Publishing Nack" + strconv.Itoa(int(deniedNack)) + " denied by '" + publisher.RemoteProcedureCallData.RPCClient.Publisher.QueueName + "' queue, waiting " + waitingTimeForRedelivery.String() + " seconds to try redeliver.")
// 				time.Sleep(waitingTimeForRedelivery)
// 				continue

// 			case successAck := <-notifyAck:
// 				log.Println("Publishing Ack" + strconv.Itoa(int(successAck)) + " recieved at '" + publisher.RemoteProcedureCallData.RPCClient.Publisher.QueueName + "'.")
// 				return correlationId, nil
// 			}
// 		}
// 	}
// }
