package dto_broker

/*
Carry the data of a delivery from the broker.

Id is given by the broker for acknowledge purposes.

Header is the field that comes at the header of a delivery.

Body is the payload of the delivery.

# ConsumerDetail is the detail about the consumer queue and

Acknowledger is a channel that the adapter will give its reference, so its acknowledger worker can
receive acknowledges with no complexity to the service.
*/
type BrokerDelivery struct {
	Id             string
	Header         map[string]any
	Body           []byte
	ConsumerDetail map[string]any
	Acknowledger   chan BrokerAcknowledge
}
