package rabbitmq

import "context"

/*
Signal to ConsumeForever() to stop its process.
*/
func (consumer *RabbitMQConsumer) BreakConsume(ctx context.Context) {
	if consumer.consumerCtx != nil {
		consumer.consumerCancel()
	}
}
