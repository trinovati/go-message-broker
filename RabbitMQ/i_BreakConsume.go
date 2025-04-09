// this rabbitmq package is adapting the amqp091-go lib
package rabbitmq

/*
Signal to ConsumeForever() to stop its process.
*/
func (consumer *RabbitMQConsumer) BreakConsume() {
	if consumer.consumerCtx != nil {
		consumer.consumerCancel()
	}
}
