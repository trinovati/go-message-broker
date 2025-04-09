// Package contains data transfer objects (DTOs) used for processing and handling broker deliveries and publishing.
// It includes structures for create publishing, receiving deliveries and acknowledging deliveries.
package dto_broker

/*
Carry the data of a delivery from the broker.

Id is given by the broker for acknowledge purposes.

Header is the field that comes at the header of a delivery.

Body is the payload of the delivery.

Acknowledger is a channel that the adapter will give its reference, so its acknowledger worker can
receive acknowledges with no complexity to the service.
*/
type BrokerDelivery struct {
	Id           string
	Header       map[string]any
	Body         []byte
	Acknowledger chan BrokerAcknowledge
}
