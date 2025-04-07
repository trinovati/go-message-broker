package rabbitmq

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/trinovati/go-message-broker/RabbitMQ/config"
	"github.com/trinovati/go-message-broker/dto"
)

func (rmq *RabbitMQ) ConsumeForever() {
	if rmq.Consumer == nil {
		log.Panic(config.Error.New("no consumer behaviour have been staged").String())
	}

	rmq.Consumer.ConsumeForever()
}

/*
Infinite loop consuming the queue linked to the RabbitMQ.ConsumeData object, preparing the data and sending it towards a channel into the system.

Use only in goroutines, otherwise the system will be forever blocked in the infinite loop trying to push into the channel.

Safe to share amqp.Connection and amqp.Channel for assincronus concurent access.

In case of the connections and/or channel comes down, it prepres for consuming as soon as the channel is up again.

CAUTION:
Keep in mind that if a behaviour other than consumer is staged, it will panic the service.
Keep in mind that if Publisher is not staged and running, it will panic the service.
*/
func (consumer *Consumer) ConsumeForever() {
	var err error
	var delivery amqp.Delivery
	var buffer bytes.Buffer
	var messageId string
	var message []byte

	if consumer.Publisher == nil {
		log.Panic(config.Error.New("no publisher have been staged for failed messages nack").String())
	}

	consumeChannelSinalizer := make(chan bool)
	incomingDeliveryChannel, err := consumer.prepareLoopingConsumer()
	if err != nil {
		log.Panic(config.Error.Wrap(err, "error preparing consumer queue").String())
	}

	go consumer.amqpChannelMonitor(consumeChannelSinalizer)

	for {
		select {
		case <-consumeChannelSinalizer:
			consumer.channel.WaitForChannel()
			incomingDeliveryChannel, err = consumer.prepareLoopingConsumer()
			if err != nil {
				log.Panic(config.Error.New("error preparing consumer queue").String())
			}

			consumeChannelSinalizer <- true

		case delivery = <-incomingDeliveryChannel:
			if delivery.Body == nil {
				continue
			}

			messageId = strconv.FormatUint(delivery.DeliveryTag, 10)

			buffer.Reset()
			err = gob.NewEncoder(&buffer).Encode(
				dto.Message{
					Id:   messageId,
					Data: delivery.Body,
				},
			)
			if err != nil {
				config.Error.Wrap(err, fmt.Sprintf("error encoding gob at channel '%s'", consumer.channel.Name())).Print()
			}

			message = append([]byte{}, buffer.Bytes()...)

			consumer.DeliveryMap.Store(messageId, delivery)
			consumer.DeliveryChannel <- message
		}
	}
}

/*
Prepare the consumer in case of the channel comming down.
*/
func (consumer *Consumer) amqpChannelMonitor(consumeChannelSinalizer chan bool) {
	for {
		if consumer.channel.IsChannelDown() || consumer.channel.Connection().IsConnectionDown() {
			consumeChannelSinalizer <- false
			<-consumeChannelSinalizer

		} else {
			time.Sleep(250 * time.Millisecond)
			continue
		}
	}
}

/*
Will create a connection, prepare channels, declare queue and exchange case needed.

If an error occurs, it will restart and retry all the process until the consumer is fully prepared.

Return a channel of incoming deliveries.
*/
func (consumer *Consumer) prepareLoopingConsumer() (incomingDeliveryChannel <-chan amqp.Delivery, err error) {
	var tolerance int
	for tolerance = 0; tolerance >= 5 || consumer.AlwaysRetry; tolerance++ {
		consumer.channel.WaitForChannel()

		err = consumer.PrepareQueue(nil)
		if err != nil {
			config.Error.Wrap(err, fmt.Sprintf("error preparing queue at channel '%s'", consumer.channel.Name())).Print()
			time.Sleep(time.Second)
			continue
		}

		if consumer.channel.IsChannelDown() {
			config.Error.New(fmt.Sprintf("connection dropped before preparing consume at channel '%s'", consumer.channel.Name())).Print()
			time.Sleep(time.Second)
			continue
		}

		incomingDeliveryChannel, err = consumer.channel.Access().Consume(consumer.QueueName, "", false, false, false, false, nil)
		if err != nil {
			config.Error.Wrap(err, fmt.Sprintf("error producing consume channel at channel '%s'", consumer.channel.Name())).Print()
			time.Sleep(time.Second)
			continue
		}

		return incomingDeliveryChannel, nil
	}

	if err == nil {
		err = config.Error.New(fmt.Sprintf("could not prepare consumer for unknown reason at channel '%s'", consumer.channel.Name()))
	}

	return nil, err
}
