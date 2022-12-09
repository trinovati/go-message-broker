package rabbitmqchannel

import (
	"context"
	"log"
	"runtime"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	connection "gitlab.com/aplicacao/trinovati-connector-message-brokers/RabbitMQ/connection"
)

/*
Object used to reference a amqp.Channel address.

Since the connection have a intimate relation with the address of amqp.Channel, it could not be moved to another memory position for
shared channel purposes, so all shared channels ChannelData objects points toward one object, and in case of channel remake, ChannelData object will point towards it.
*/
type ChannelData struct {
	Connection                 *connection.ConnectionData
	Channel                    *amqp.Channel
	isOpen                     bool
	closureNotificationChannel chan *amqp.Error
	lastChannelError           *amqp.Error
	Context                    context.Context
	CancelContext              context.CancelFunc
	ChannelId                  uint64
}

/*
Build an object used to reference a amqp.Channel and store all the data needed to keep track of its health.
*/
func NewChannelData() *ChannelData {
	channelContext, cancelContext := context.WithCancel(context.Background())

	return &ChannelData{
		Connection:                 connection.NewConnectionData(),
		Channel:                    nil,
		isOpen:                     false,
		closureNotificationChannel: nil,
		lastChannelError:           nil,
		Context:                    channelContext,
		CancelContext:              cancelContext,
		ChannelId:                  0,
	}
}

/*
Create and keep a channel linked to RabbitMQ connection.

If channel is dropped for any reason it will try remake the channel.
To terminante the channel, use CloseChannel() method, it will close the channel via context.Done().

It puts the channel in confirm mode, so any publishing done will have a response from the server.
*/
func (c *ChannelData) CreateChannel() *ChannelData {
	errorFileIdentification := "RabbitMQ at CreateChannel()"

	if c.Connection.Connection == nil {
		c.Connection.Connect()
	}

	for {
		c.Connection.WaitForConnection()

		channel, err := c.Connection.Connection.Channel()
		if err != nil {
			log.Println("error creating RabbitMQ channel in " + errorFileIdentification + ": " + err.Error())
			continue
		}

		err = channel.Confirm(false)
		if err != nil {
			log.Println("error configuring channel with Confirm() protocol in " + errorFileIdentification + ": " + err.Error())
			continue
		}

		time.Sleep(2 * time.Second)

		c.updateChannel(channel)
		log.Println("Successful RabbitMQ channel with id " + strconv.FormatUint(c.ChannelId, 10) + " at server '" + c.Connection.ServerAddress + "'")
		c.isOpen = true

		go c.keepChannel()

		return c
	}
}

/*
Refresh the closureNotificationChannel for helthyness.

Reference the newly created amqp.Channel, assuring assincronus concurrent access to multiple objects.

Refresh the channel id for controll of references.
*/
func (c *ChannelData) updateChannel(channel *amqp.Channel) {
	c.closureNotificationChannel = channel.NotifyClose(make(chan *amqp.Error))

	c.Channel = channel
	c.ChannelId++
}

/*
Method for maintance of a channel.
*/
func (c *ChannelData) keepChannel() {
	errorFileIdentification := "RabbitMQ at keepChannel()"

	select {
	case <-c.Connection.Context.Done():
		c.CloseChannel()
		break

	case <-c.Context.Done():
		c.Channel.Close()
		break

	case closeNotification := <-c.closureNotificationChannel:
		c.isOpen = false
		c.Channel.Close()

		var closureReason string = ""
		if closeNotification != nil {
			c.lastChannelError = closeNotification
			closureReason = closeNotification.Reason
		}
		log.Println("***ERROR*** in " + errorFileIdentification + ": a channel with RabbitMQ server '" + c.Connection.ServerAddress + "' have closed with reason: '" + closureReason + "'")

		c.CreateChannel()
	}

	runtime.Goexit()
}

/*
Method for closing the channel via context, sending  signal for all objects sharring channel to terminate its process.
*/
func (c *ChannelData) CloseChannel() {
	c.isOpen = false

	c.CancelContext()
}

/*
Check the channel, returning true if its down and unavailble.
*/
func (c *ChannelData) IsChannelDown() bool {
	return !c.isOpen
}

/*
Block the process until the channel is open.
*/
func (c *ChannelData) WaitForChannel() {
	for {
		if c.isOpen {
			return
		}

		log.Println("waiting for rabbitmq channel")
		time.Sleep(500 * time.Millisecond)
	}
}
