package channel

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"time"

	"gitlab.com/aplicacao/trinovati-connector-message-brokers/RabbitMQ/config"
	"gitlab.com/aplicacao/trinovati-connector-message-brokers/RabbitMQ/connection"
	"gitlab.com/aplicacao/trinovati-connector-message-brokers/RabbitMQ/interfaces"

	amqp "github.com/rabbitmq/amqp091-go"
)

/*
Object used to reference a amqp.Channel address.

Since the connection have a intimate relation with the address of amqp.Channel, it could not be moved to another memory position for
shared channel purposes, so all shared channels Channel objects points toward one object, and in case of channel remake, Channel object will point towards it.
*/
type Channel struct {
	connection                 *connection.Connection
	Channel                    *amqp.Channel
	isOpen                     bool
	closureNotificationChannel chan *amqp.Error
	lastChannelError           *amqp.Error
	cancelContext              context.CancelFunc
	ctxConnection              context.Context
	ChannelId                  uint64
}

/*
Build an object used to reference a amqp.Channel and store all the data needed to keep track of its health.
*/
func NewChannel() *Channel {
	return &Channel{
		connection:                 connection.NewConnection(),
		Channel:                    nil,
		isOpen:                     false,
		closureNotificationChannel: nil,
		lastChannelError:           nil,
		cancelContext:              nil,
		ctxConnection:              nil,
		ChannelId:                  0,
	}
}

func (c Channel) Access() *amqp.Channel {
	return c.Channel
}

func (c *Channel) SetConnection(conn interfaces.Connection) interfaces.Channel {
	var ok bool
	c.connection, ok = conn.(*connection.Connection)
	if !ok {
		log.Panic(config.Error.New("%T cannot be accepted as connection").String())
	}

	return c
}

func (c Channel) Connection() interfaces.Connection {
	return c.connection
}

func (c *Channel) WithConnectionData(host string, port string, username string, password string) interfaces.Channel {
	c.connection.WithConnectionData(host, port, username, password)

	return c
}

/*
Create and keep a channel linked to RabbitMQ connection.

If channel is dropped for any reason it will try remake the channel.
To terminante the channel, use CloseChannel() method, it will close the channel via context.Done().

It puts the channel in confirm mode, so any publishing done will have a response from the server.
*/
func (c *Channel) Connect() (channel interfaces.Channel) {
	if c.connection.Connection == nil {
		_, c.ctxConnection = c.connection.Connect()
	} else if c.isOpen {
		return c
	}

	for {
		c.connection.WaitForConnection()

		channel, err := c.connection.Connection.Channel()
		if err != nil {
			config.Error.Wrap(err, "error creating RabbitMQ channel").Print()
			time.Sleep(time.Second)
			continue
		}

		err = channel.Confirm(false)
		if err != nil {
			config.Error.Wrap(err, "error configuring channel with Confirm() protocol").Print()
			continue
		}

		c.updateChannel(channel)
		log.Printf("Successfully opened channel with id '%d' with connection id '%d' at server '%s'", c.ChannelId, c.connection.ConnectionId, c.connection.ServerAddress)
		c.isOpen = true

		ctx, cancel := context.WithCancel(context.Background())
		c.cancelContext = cancel

		go c.keepChannel(c.ctxConnection, ctx)

		return c
	}
}

/*
Refresh the closureNotificationChannel for helthyness.

Reference the newly created amqp.Channel, assuring assincronus concurrent access to multiple objects.

Refresh the channel id for controll of references.
*/
func (c *Channel) updateChannel(channel *amqp.Channel) {
	c.closureNotificationChannel = channel.NotifyClose(make(chan *amqp.Error))

	c.Channel = channel
	c.ChannelId++
}

/*
Method for maintance of a channel.
*/
func (c *Channel) keepChannel(ctxConnection context.Context, ctxChannel context.Context) {
	for {
		select {
		case <-ctxConnection.Done():
			log.Printf("connection context of channel id '%d' with connection id '%d' at server '%s' have been closed", c.ChannelId, c.connection.ConnectionId, c.connection.ServerAddress)
			c.CloseChannel()

		case <-ctxChannel.Done():
			log.Printf("channel context of channel id '%d' with connection id '%d' at server '%s' have been closed", c.ChannelId, c.connection.ConnectionId, c.connection.ServerAddress)
			c.Channel.Close()

		case closeNotification := <-c.closureNotificationChannel:
			c.isOpen = false
			c.Channel.Close()

			if closeNotification != nil {
				c.lastChannelError = closeNotification
				config.Error.New(fmt.Sprintf("channel of channel id '%d' with connection id '%d' at server '%s' have closed with\nreason: '%s'\nerror: '%s'\nstatus code: '%d'", c.ChannelId, c.connection.ConnectionId, c.connection.ServerAddress, closeNotification.Reason, closeNotification.Error(), closeNotification.Code)).Print()

			} else {
				config.Error.New(fmt.Sprintf("connection of channel id '%d' with connection id '%d' at server '%s' have closed with no specified reason", c.ChannelId, c.connection.ConnectionId, c.connection.ServerAddress)).Print()
			}

			c.Connect()
		}

		runtime.Goexit()
	}
}

/*
Method for closing the channel via context, sending  signal for all objects sharring channel to terminate its process.
*/
func (c *Channel) CloseChannel() {
	c.isOpen = false

	if c.cancelContext != nil {
		c.cancelContext()
	}
}

func (c *Channel) CloseConnection() {
	c.CloseChannel()
	c.connection.CloseConnection()
}

/*
Check the channel, returning true if its down and unavailble.
*/
func (c *Channel) IsChannelDown() bool {
	return !c.isOpen
}

/*
Block the process until the channel is open.
*/
func (c *Channel) WaitForChannel() {
	for {
		if c.isOpen {
			return
		}

		log.Println("waiting for rabbitmq channel")
		time.Sleep(500 * time.Millisecond)
	}
}
