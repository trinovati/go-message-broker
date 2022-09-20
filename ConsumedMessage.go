package messagebroker

/*
Generic object that message consume manager use to transport data to other parts of the system.

TransmissionData is the interface that represents a how a protocol of transmition should be treated.

TransmissionType is the identificator for which type of transmission the service recieved.

MessageId is the control that the message consume manager use to administrate its messages.
*/
type ConsumedMessage struct {
	TransmissionData interface{}
	MessageId        string
}

/*
Create a generic object that message consume manager use to transport data to other parts of the system.

TransmissionData is the interface that represents a how a protocol of transmition should be treated.

TransmissionType is the identificator for which type of transmission the service recieved.

MessageId is the control that the message consume manager use to administrate its messages.
*/
func NewConsumedMessage() *ConsumedMessage {
	return &ConsumedMessage{}
}
