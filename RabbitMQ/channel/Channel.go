package channel

import (
	"context"
	"log"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/trinovati/go-message-broker/v3/RabbitMQ/config"
	"github.com/trinovati/go-message-broker/v3/RabbitMQ/connection"
)

/*
Object used to reference a amqp.Channel address.

Since the connections have intimate relation with the address of amqp.Channel, it could not be moved to another memory position for
shared channel purposes, so all shared channels point to a single RabbitMQChannel object.

The purpose of this abstraction is minimize the quantity of open amqp.Channel to RabbitMQ and at the same time make use of a keep alive
and reconnect technique.
*/
type RabbitMQChannel struct {
	ChannelCount               uint64
	ChannelId                  uuid.UUID
	connection                 *connection.RabbitMQConnection
	Channel                    *amqp.Channel
	isOpen                     bool
	closureNotificationChannel chan *amqp.Error
	lastChannelError           *amqp.Error
	Context                    context.Context
	CancelContext              context.CancelFunc
}

/*
Builder of RabbitMQChannel object.
*/
func NewRabbitMQChannel(env config.RABBITMQ_CONFIG) *RabbitMQChannel {
	return &RabbitMQChannel{
		ChannelId:                  uuid.New(),
		connection:                 connection.NewRabbitMQConnection(env),
		Channel:                    nil,
		isOpen:                     false,
		closureNotificationChannel: nil,
		lastChannelError:           nil,
		CancelContext:              nil,
		ChannelCount:               0,
	}
}

/*
Setter with the purpose of share RabbitMQConnection between multiple RabbitMQChannel.
*/
func (c *RabbitMQChannel) SetConnection(conn *connection.RabbitMQConnection) *RabbitMQChannel {
	c.connection = conn

	return c
}

/*
Getter of the RabbitMQConnection.
*/
func (c RabbitMQChannel) Connection() *connection.RabbitMQConnection {
	return c.connection
}

/*
Create and keep alive a amqp.Channel linked to RabbitMQ connection.

If channel is dropped for any reason it will try to remake the channel.
To terminate the channel, use CloseChannel() method, it will close the channel via context.Done().

It puts the channel in confirm mode, so any publishing done will have a response from the server.
*/
func (c *RabbitMQChannel) Connect() *RabbitMQChannel {
	var err error
	var channel *amqp.Channel

	c.connection.Mutex.Lock()
	defer c.connection.Mutex.Unlock()

	if c.connection.Connection == nil {
		c.connection.Connect()
	} else if c.isOpen {
		return c
	}

	for {
		c.connection.WaitForConnection()

		channel, err = c.connection.Connection.Channel()
		if err != nil {
			log.Printf("error creating RabbitMQ channel %s: %s\n", c.ChannelId, err.Error())
			time.Sleep(time.Second)
			continue
		}

		err = channel.Confirm(false)
		if err != nil {
			log.Printf("error configuring channel %s with Confirm() protocol: %s\n", c.ChannelId, err.Error())
			continue
		}

		c.updateChannel(channel)
		log.Printf("opened channel id %s with connection id %s at server %s\n", c.ChannelId.String(), c.connection.ConnectionId.String(), c.connection.Env().RABBITMQ_HOST)
		c.isOpen = true

		c.Context, c.CancelContext = context.WithCancel(context.Background())

		go c.keepChannel()

		return c
	}
}

/*
Refresh the closureNotificationChannel for healthiness.

Reference the newly created amqp.Channel, assuring assincronus concurrent access to multiple objects.

Refresh the channel count for control of references.
*/
func (c *RabbitMQChannel) updateChannel(channel *amqp.Channel) {
	c.closureNotificationChannel = channel.NotifyClose(make(chan *amqp.Error))

	c.Channel = channel
	c.ChannelCount++
}

/*
Method for maintenance of a amqp.Channel.

It will close the channel if the RabbitMQConnection signal its closure.

It will stop to maintain the amqp.Channel if the RabbitMQChannel signal its closure.

It will reconnect if receive a signal of dropped connection.
*/
func (c *RabbitMQChannel) keepChannel() {
	select {
	case <-c.connection.Context.Done():
		log.Printf("connection context of channel id %s with connection id %s at server %s have been closed\n", c.ChannelId.String(), c.connection.ConnectionId.String(), c.connection.Env().RABBITMQ_HOST)
		c.CloseChannel()

	case <-c.Context.Done():
		log.Printf("channel context of channel id %s with connection id %s at server %s have been closed\n", c.ChannelId.String(), c.connection.ConnectionId.String(), c.connection.Env().RABBITMQ_HOST)
		c.Channel.Close()

	case closeNotification := <-c.closureNotificationChannel:
		c.isOpen = false
		c.Channel.Close()

		if closeNotification != nil {
			c.lastChannelError = closeNotification
			log.Printf("channel id %s with connection id %s at server %s have closed with\nreason: %s\nerror: %s\nstatus code: %d\n", c.ChannelId.String(), c.connection.ConnectionId.String(), c.connection.Env().RABBITMQ_HOST, closeNotification.Reason, closeNotification.Error(), closeNotification.Code)

		} else {
			log.Printf("connection of channel id %s with connection id %s at server %s have closed with no specified reason\n", c.ChannelId.String(), c.connection.ConnectionId.String(), c.connection.Env().RABBITMQ_HOST)
		}

		c.Connect()
	}
}

/*
Method for closing the channel via context.

Keep in mind that this will affect all objects that shares channel with this one.
*/
func (c *RabbitMQChannel) CloseChannel() {
	c.isOpen = false

	if c.CancelContext != nil {
		c.CancelContext()
	}
}

/*
Method for closing the channel and connection via context.

Keep in mind that this will affect all objects that shares channel or connection with this one, including closing
other non-shared channels that shares this connection.
*/
func (c *RabbitMQChannel) CloseConnection() {
	c.CloseChannel()
	c.connection.CloseConnection()
}

/*
Check the RabbitMQChannel availability.
*/
func (c *RabbitMQChannel) IsChannelDown() bool {
	return !c.isOpen
}

/*
Block the process until the channel is open.
*/
func (c *RabbitMQChannel) WaitForChannel() {
	for {
		if c.isOpen {
			return
		}

		log.Printf("waiting for rabbitmq channel %s\n", c.ChannelId.String())
		time.Sleep(500 * time.Millisecond)
	}
}
