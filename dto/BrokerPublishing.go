// The dto_pkg package contains data transfer objects (DTOs) used for processing broker messages and handling document errors. It includes structures for acknowledging deliveries and reporting failed document processing.
package dto_pkg

/*
Carry the data of a publishing to the broker.

Header is the field that goes at the header of a publishing.

Body is the payload of the publishing.
*/
type BrokerPublishing struct {
	Header map[string]any
	Body   []byte
}
