package messagebroker

/*
Generic object that message broker use to transport data to other parts of the system.

TransmissionData is the interface that represents a how a protocol of transmition should be treated.

MessageId is the control that the message broker use to administrate its messages.
*/
type MessageBrokerConsumedMessage struct {
	TransmissionData interface{}
	MessageId        string
}

/*
Create a generic object that message broker use to transport data to other parts of the system.

TransmissionData is the interface that represents a how a protocol of transmition should be treated.

MessageId is the control that the message broker use to administrate its messages.
*/
func NewMessageBrokerConsumedMessage() *MessageBrokerConsumedMessage {
	return &MessageBrokerConsumedMessage{}
}

func (m *MessageBrokerConsumedMessage) GetTransmissionData() (transmissionData interface{}) {
	return m.TransmissionData
}

func (m *MessageBrokerConsumedMessage) GetMessageId() (messageId string) {
	return m.MessageId
}
