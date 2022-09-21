package messagebroker

/*
Generic object that message broker use to recieve the status response of a sended message.
Some message broker expects to acknowledge every message.

Success is the marker that the system could or couldn't handle the message.

MessageId is the control that the broker use to administrate its messages.

OptionalRoute is a string flag, path or destiny that the message broker will redirect the message.

Comment is a commentary that can be anexed to the object as a sinalizer of errors or success.
*/
type MessageBrokerAcknowledger struct {
	Success       bool
	MessageId     string
	OptionalRoute string
	Comment       string
}

/*
Create a generic object that message broker use to recieve the status response of a sended message.
Some message broker expects to acknowledge every message.

Success is the marker that the system could or couldn't handle the message.

MessageId is the control that the broker use to administrate its messages.

OptionalRoute is a string flag, path or destiny that the message broker will redirect the message.

Comment is a commentary that can be anexed to the object as a sinalizer of errors or success.
*/
func NewMessageBrokerAcknowledger(success bool, comment string, messageId string, optionalRoute string) *MessageBrokerAcknowledger {
	return &MessageBrokerAcknowledger{
		Success:       success,
		MessageId:     messageId,
		OptionalRoute: optionalRoute,
		Comment:       comment,
	}
}
