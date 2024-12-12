package interfaces

type Publisher interface {
	Behaviour

	Persist(message []byte, gobTarget []byte) error
	Publish(message []byte, header map[string]interface{}, gobTarget []byte) error
}
