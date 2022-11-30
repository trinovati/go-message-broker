package rabbitmq

/*
Object that holds all information needed for publishing into a RabbitMQ queue.
*/
type RMQPublish struct {
	Channel           *ChannelData
	notifyFlowChannel *chan bool
	ExchangeName      string
	ExchangeType      string
	QueueName         string
	AccessKey         string
}

/*
Builds a new object that holds all information needed for publishing into a RabbitMQ queue.
*/
func newRMQPublish() *RMQPublish {
	return &RMQPublish{
		notifyFlowChannel: nil,
	}
}

/*
Insert data into the object used for RabbitMQ queue publish.
*/
func (p *RMQPublish) populate(exchangeName string, exchangeType string, QueueName string, AccessKey string) {
	p.ExchangeName = exchangeName
	p.ExchangeType = exchangeType
	p.QueueName = QueueName
	p.AccessKey = AccessKey
}
