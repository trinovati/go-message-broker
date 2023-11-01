package dto

/*
Generic object that message broker use to transport data to other parts of the system.

MessageData is the interface that represents a how a protocol of transmition should be treated.

MessageId is the control that the message broker use to administrate its messages.
*/
type Message struct {
	Data []byte
	Id   string
}
