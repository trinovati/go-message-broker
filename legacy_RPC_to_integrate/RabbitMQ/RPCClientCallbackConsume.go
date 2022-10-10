package rabbitmq

// import (
// 	"errors"
// 	"log"
// )

// /*
// Consume a single message from Remote Procedure Call response queue that matches the correlationId passed as argument.
// */
// func (r *RabbitMQ) RPCClientCallbackConsume(correlationId string) (callbackResponse []byte, err error) {
// 	errorFileIdentification := "RabbitMQ.go at RPCClientCallbackConsume()"

// 	queueMessages, closeNotifyChannel := r.RemoteProcedureCallData.RPCClient.Callback.connectConsumer(r)

// 	for {
// 		select {
// 		case delivery := <-queueMessages:
// 			if correlationId != delivery.CorrelationId {
// 				err = delivery.Acknowledger.Nack(delivery.DeliveryTag, false, true)
// 				if err != nil {
// 					return nil, errors.New("error with negatively acknowledge in " + errorFileIdentification + ": " + err.Error())
// 				}

// 				continue
// 			}

// 			callbackResponse = delivery.Body

// 			err = delivery.Acknowledger.Ack(delivery.DeliveryTag, false)
// 			if err != nil {
// 				log.Println("error positive acknowlodging message in " + errorFileIdentification + ": " + err.Error())
// 			}

// 			return callbackResponse, nil

// 		case notify := <-closeNotifyChannel:
// 			log.Println("Consumed queue '" + r.RemoteProcedureCallData.RPCClient.Callback.QueueName + "' have closed with reason: '" + notify.Reason + "'")

// 			queueMessages, closeNotifyChannel = r.RemoteProcedureCallData.RPCClient.Callback.connectConsumer(r)
// 		}
// 	}
// }
