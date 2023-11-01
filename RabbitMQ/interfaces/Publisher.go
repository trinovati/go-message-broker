package interfaces

type Publisher interface {
	Behaviour

	Persist(message []byte, gobTarget []byte) error
}
