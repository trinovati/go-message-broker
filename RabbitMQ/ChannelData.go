package rabbitmq

import (
	"context"
	"log"
	"runtime"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

/*
Object used to reference a amqp.Channel address.

Since the connection have a intimate relation with the address of amqp.Channel, it could not be moved to another memory position for
shared channel purposes, so all shared channels points toward this object, and in case of channel remake, this object will point towards it.
*/
type ChannelData struct {
	Connection                 *ConnectionData
	Channel                    *amqp.Channel
	isOpen                     bool
	closureNotificationChannel chan *amqp.Error
	lastChannelError           *amqp.Error
	Context                    context.Context
	CancelContext              context.CancelFunc
}

/*
Build an object used to reference a amqp.Channel and store all the data needed to keep track of its health.
*/
func newChannelData() *ChannelData {
	channelContext, cancelContext := context.WithCancel(context.Background())

	return &ChannelData{
		Connection:                 &ConnectionData{},
		Channel:                    &amqp.Channel{},
		isOpen:                     false,
		closureNotificationChannel: nil,
		lastChannelError:           nil,
		Context:                    channelContext,
		CancelContext:              cancelContext,
	}
}

/*
Keep and prepare a channel linked to RabbitMQ connection.

It puts the channel in confirm mode, so any publishing done will have a response from the server.
*/
func (c *ChannelData) CreateChannel(connection *ConnectionData) {
	errorFileIdentification := "RabbitMQ.go at CreateChannel()"

	for {
		connection.WaitForConnection()

		channel, err := connection.Connection.Channel()
		if err != nil {
			log.Println("error creating RabbitMQ channel in " + errorFileIdentification + ": " + err.Error())
			continue
		}

		err = channel.Confirm(false)
		if err != nil {
			log.Println("error configuring channel with Confirm() protocol in " + errorFileIdentification + ": " + err.Error())
			continue
		}

		c.updateChannel(channel, connection)

		go c.keepChannel()

		return
	}
}

/*
Refresh the closureNotificationChannel for helthyness.

Reference the newly created amqp.Connection, assuring assincronus concurrent access to multiple objects.

Refresh the connection id for controll of references.
*/
func (c *ChannelData) updateChannel(channel *amqp.Channel, connection *ConnectionData) {
	c.closureNotificationChannel = channel.NotifyClose(make(chan *amqp.Error))

	c.Connection = connection

	c.Channel = channel

	c.isOpen = true
}

/*
Method for maintance of channel.
*/
func (c *ChannelData) keepChannel() {
	errorFileIdentification := "RabbitMQ.go at keepChannel()"

	serverAddress := strings.Split(strings.Split(c.Connection.serverAddress, "@")[1], ":")[0]

	select {
	case <-c.Connection.Context.Done():
		c.Channel.Close()
		break

	case <-c.Context.Done():
		break

	case closeNotification := <-c.closureNotificationChannel:
		c.isOpen = false
		c.lastChannelError = closeNotification
		log.Println("***ERROR*** in " + errorFileIdentification + ": a channel with RabbitMQ server '" + serverAddress + "' have closed with reason: '" + closeNotification.Reason + "'")

		err := c.Channel.Close()
		if err != nil {
			completeError := "***ERROR*** error closing RabbitMQ channel in " + errorFileIdentification + ": " + err.Error()
			log.Println(completeError)
		}

		*c = ChannelData{
			Connection:                 c.Connection,
			Channel:                    &amqp.Channel{},
			isOpen:                     false,
			closureNotificationChannel: nil,
			lastChannelError:           c.lastChannelError,
			Context:                    c.Context,
		}

		c.CreateChannel(c.Connection)
	}

	runtime.Goexit()

}

func (c *ChannelData) CloseChannel() {
	c.CancelContext()

	c.Channel.Close()
}

/*
Check the channel, returning true if its down and unavailble.
*/
func (c *ChannelData) isChannelDown() bool {
	return !c.isOpen
}

/*
Wait for the channel to be open.
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
