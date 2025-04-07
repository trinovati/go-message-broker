package rabbitmq

import (
	"bytes"
	"encoding/base64"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/trinovati/go-message-broker/RabbitMQ/config"
	"github.com/trinovati/go-message-broker/dto"

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
		log.Panic(config.Error.New(fmt.Sprintf("no publisher have been staged for failed messages nack at channel '%s'", consumer.Channel().Name())).String())
	}

	_, err = buffer.Write(gobAcknowledge)
	if err != nil {
		return config.Error.Wrap(err, fmt.Sprintf("error writing to buffer at channel '%s'", consumer.Channel().Name()))
	}

	err = gob.NewDecoder(&buffer).Decode(&acknowledge)
	if err != nil {
		return config.Error.Wrap(err, fmt.Sprintf("error decoding gob at channel '%s'", consumer.Channel().Name()))
	}

	mapObject, found := consumer.DeliveryMap.Load(acknowledge.Id)
	if !found {
		return config.Error.New(fmt.Sprintf("failed loading message id '%s' from map at channel '%s'", acknowledge.Id, consumer.Channel().Name()))
	}
	consumer.DeliveryMap.Delete(acknowledge.Id)

	switch message := mapObject.(type) {
	case amqp.Delivery:
		isMessageNotFound := message.Body == nil
		if isMessageNotFound {
			return config.Error.New(fmt.Sprintf("message id '%s' not found at channel '%s'", acknowledge.Id, consumer.Channel().Name()))
		}

		if acknowledge.Success {
			err = message.Acknowledger.Ack(message.DeliveryTag, false)
			if err != nil {
				return config.Error.Wrap(err, fmt.Sprintf("error positive acknowlodging message at channel '%s'", consumer.Channel().Name()))
			}

		} else if acknowledge.Requeue {
			err = message.Acknowledger.Nack(message.DeliveryTag, false, true)
			if err != nil {
				return config.Error.Wrap(err, fmt.Sprintf("error negative acknowlodging message at channel '%s'", consumer.Channel().Name()))
			}

		} else if acknowledge.Enqueue {
			err = message.Acknowledger.Nack(message.DeliveryTag, false, false)
			if err != nil {
				return config.Error.Wrap(err, fmt.Sprintf("error negative acknowlodging message at channel '%s'", consumer.Channel().Name()))
			}

			if acknowledge.Target.Queue != "" {
				acknowledge.Target.Exchange = consumer.FailedExchange
				acknowledge.Target.ExchangeType = consumer.FailedExchangeType
				acknowledge.Target.Queue = consumer.FailedQueue
				acknowledge.Target.AccessKey = consumer.FailedAccessKey
			}

			err = gob.NewEncoder(&target).Encode(acknowledge.Target)
			if err != nil {
				return config.Error.Wrap(err, fmt.Sprintf("error encoding gob at channel '%s'", consumer.Channel().Name()))
			}

			message, err := json.Marshal(
				map[string]interface{}{
					"message":          base64.StdEncoding.EncodeToString(message.Body),
					"acknowledge_time": time.Now().Format("2006-01-02T15:04:05Z07:00"),
					"motive":           string(acknowledge.Data),
				},
			)
			if err != nil {
				return config.Error.Wrap(err, fmt.Sprintf("error marshalling failure message at channel '%s'", consumer.Channel().Name()))
			}

			err = consumer.Publisher.Persist(message, target.Bytes())
			if err != nil {
				return config.Error.Wrap(err, fmt.Sprintf("error publishing to failure queue at channel '%s'", consumer.Channel().Name()))
			}

		} else {
			err = message.Acknowledger.Nack(message.DeliveryTag, false, false)
			if err != nil {
				return config.Error.Wrap(err, fmt.Sprintf("error negative acknowlodging message at channel '%s'", consumer.Channel().Name()))
			}
		}

	default:
		return config.Error.New(fmt.Sprintf("message id '%s' have come with untreatable '%T' format at channel '%s'", acknowledge.Id, mapObject, consumer.Channel().Name()))
	}

	return nil
}
