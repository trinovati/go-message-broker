package messagebroker

/*
Generic object that message broker use to transport data to other parts of the system.

MessageData is the interface that represents a how a protocol of transmition should be treated.

MessageId is the control that the message broker use to administrate its messages.
*/
type MessageBrokerConsumedMessage struct {
	MessageData interface{}
	MessageId   string
}

/*
Create a generic object that message broker use to transport data to other parts of the system.

MessageData is the interface that represents a how a protocol of transmition should be treated.

MessageId is the control that the message broker use to administrate its messages.
*/
func NewMessageBrokerConsumedMessage() *MessageBrokerConsumedMessage {
	return &MessageBrokerConsumedMessage{}
}

func (m *MessageBrokerConsumedMessage) GetMessageData() (messageData interface{}) {
	return m.MessageData
}

func (m *MessageBrokerConsumedMessage) GetMessageId() (messageId string) {
	return m.MessageId
}
