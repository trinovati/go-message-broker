// The dto_pkg package contains data transfer objects (DTOs) used for processing broker messages and handling document errors. It includes structures for acknowledging deliveries and reporting failed document processing.
package dto_pkg

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
