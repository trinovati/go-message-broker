package rabbitmq

import "context"

/*
Signal to ConsumeForever() to stop its process.
*/
func (consumer *RabbitMQConsumer) BreakConsume(ctx context.Context) {
	consumer.mutex.Lock()

	if consumer.consumerCancel != nil {
		consumer.consumerCancel()
		consumer.consumerCancel = nil
	}

	consumer.isConsuming = false
	consumer.isRunning = false

	consumer.channel.UnregisterConsumer(consumer.Id)

	consumer.mutex.Unlock()
}
