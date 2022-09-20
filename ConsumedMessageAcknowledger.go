package messagebroker

/*
Generic object that message consume manager use to recieve the status response of a sended message.
Some message consume manager expects to acknowledge every message.

Success is the marker that the system could or couldn't handle the message.

MessageId is the control that the broker use to administrate its messages.

OptionalRoute is a string flag, path or destiny that the message broker will redirect the message.

Comment is a commentary that can be anexed to the object as a sinalizer of errors or success.
*/
type ConsumedMessageAcknowledger struct {
	Success       bool
	MessageId     string
	OptionalRoute string
	Comment       string
}

/*
Create a generic object that message consume manager use to recieve the status response of a sended message.
Some message consume manager expects to acknowledge every message.

Success is the marker that the system could or couldn't handle the message.

MessageId is the control that the broker use to administrate its messages.

OptionalRoute is a string flag, path or destiny that the message broker will redirect the message.

Comment is a commentary that can be anexed to the object as a sinalizer of errors or success.
*/
func NewConsumedMessageAcknowledger(success bool, comment string, messageId string, optionalRoute string) *ConsumedMessageAcknowledger {
	return &ConsumedMessageAcknowledger{
		Success:       success,
		MessageId:     messageId,
		OptionalRoute: optionalRoute,
		Comment:       comment,
	}
}
