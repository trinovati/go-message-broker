package rabbitmq

import (
	"bytes"
	"encoding/base64"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"gitlab.com/aplicacao/trinovati-connector-message-brokers/RabbitMQ/config"
	"gitlab.com/aplicacao/trinovati-connector-message-brokers/dto"

	amqp "github.com/rabbitmq/amqp091-go"
)

func (rmq *RabbitMQ) Acknowledge(gobAcknowledge []byte) (err error) {
	if rmq.Consumer == nil {
		log.Panic(config.Error.New("no consumer behaviour have been staged").String())
	}

	return rmq.Consumer.Acknowledge(gobAcknowledge)
}

/*
Acknowledge a message, sinalizing the RabbitMQ server that the message can be eliminated of the queue, or requeued on a error queue.
*/
func (consumer *Consumer) Acknowledge(gobAcknowledge []byte) (err error) {
	var buffer bytes.Buffer
	var target bytes.Buffer
	var acknowledge dto.Acknowledge

	if consumer.Publisher == nil {
		log.Panic(config.Error.New("no publisher have been staged for failed messages nack").String())
	}

	_, err = buffer.Write(gobAcknowledge)
	if err != nil {
		return config.Error.Wrap(err, "error writing to buffer")
	}

	err = gob.NewDecoder(&buffer).Decode(&acknowledge)
	if err != nil {
		return config.Error.Wrap(err, "error decoding gob")
	}

	mapObject, found := consumer.DeliveryMap.Load(acknowledge.Id)
	if !found {
		return config.Error.New("failed loading message id '" + acknowledge.Id + "' from map")
	}
	consumer.DeliveryMap.Delete(acknowledge.Id)

	switch message := mapObject.(type) {
	case amqp.Delivery:
		isMessageNotFound := message.Body == nil
		if isMessageNotFound {
			return config.Error.New("message id '" + acknowledge.Id + "' not found")
		}

		if acknowledge.Success {
			err = message.Acknowledger.Ack(message.DeliveryTag, false)
			if err != nil {
				return config.Error.Wrap(err, "error positive acknowlodging message")
			}

		} else if acknowledge.Requeue {
			err = message.Acknowledger.Nack(message.DeliveryTag, false, true)
			if err != nil {
				return config.Error.Wrap(err, "error negative acknowlodging message")
			}

		} else if acknowledge.Enqueue {
			err = message.Acknowledger.Nack(message.DeliveryTag, false, false)
			if err != nil {
				return config.Error.Wrap(err, "error negative acknowlodging message")
			}

			if acknowledge.Target.Queue != "" {
				acknowledge.Target.Exchange = consumer.FailedExchange
				acknowledge.Target.ExchangeType = consumer.FailedExchangeType
				acknowledge.Target.Queue = consumer.FailedQueue
				acknowledge.Target.AccessKey = consumer.FailedAccessKey
			}

			err = gob.NewEncoder(&target).Encode(acknowledge.Target)
			if err != nil {
				return config.Error.Wrap(err, "error encoding gob")
			}

			message, err := json.Marshal(
				map[string]interface{}{
					"message":          base64.StdEncoding.EncodeToString(message.Body),
					"acknowledge_time": time.Now().Format("2006-01-02T15:04:05Z07:00"),
					"motive":           acknowledge.Data,
				},
			)
			if err != nil {
				return config.Error.Wrap(err, "error marshalling failure message")
			}

			err = consumer.Publisher.Persist(message, target.Bytes())
			if err != nil {
				return config.Error.Wrap(err, "error publishing to failure queue")
			}

		} else {
			err = message.Acknowledger.Nack(message.DeliveryTag, false, false)
			if err != nil {
				return config.Error.Wrap(err, "error negative acknowlodging message")
			}
		}

	default:
		return config.Error.New(fmt.Sprintf("message id '%s' have come with untreatable '%T' format ", acknowledge.Id, mapObject))
	}

	return nil
}
