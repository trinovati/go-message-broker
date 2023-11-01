package interfaces

type Consumer interface {
	Behaviour

	SetPublisher(publisher Publisher)

	GetDeliveryChannel() (gobMessageChannel chan []byte, err error)

	ConsumeForever()
	Acknowledge(gobAcknowledge []byte) error
}
