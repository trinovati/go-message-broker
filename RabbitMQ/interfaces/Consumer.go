package interfaces

type Consumer interface {
	Behaviour

	SetPublisher(publisher Publisher)

	Deliveries() (gobMessageChannel chan []byte)

	ConsumeForever()
	Acknowledge(gobAcknowledge []byte) error
}
