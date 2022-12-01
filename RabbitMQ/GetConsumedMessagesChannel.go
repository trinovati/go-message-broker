package rabbitmq

/*
Return the channel that will be used to send messages into the service.
*/
func (r *RabbitMQ) GetConsumedMessagesChannel() (consumedMessageChannel chan interface{}) {
	return r.ConsumeData.OutgoingDeliveryChannel
}
