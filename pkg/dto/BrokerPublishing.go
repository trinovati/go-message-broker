package dto_broker

/*
Carry the data of a publishing to the broker.

Header is the field that goes at the header of a publishing.

Body is the payload of the publishing.
*/
type BrokerPublishing struct {
	Header map[string]any
	Body   []byte
}
