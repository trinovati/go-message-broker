package rabbitmq

/*
Return the channel that will be used to send messages into the service.
*/
func (c *Consumer) GetConsumedMessagesChannel() (consumedMessageChannel chan interface{}) {
	return c.OutgoingDeliveryChannel
}
