/*
Package constants defines static values and configuration constants used across
the application. This includes the different actions that can be taken when acknowledging
a message.

The `AcknowledgeAction` type and constants help define the possible actions for acknowledging
a message, such as marking it as successful, requeuing it, or moving it to the dead-letter queue.

Constants defined in this package:
  - ACKNOWLEDGE_SUCCESS: Indicates that the message was processed successfully.
  - ACKNOWLEDGE_REQUEUE: Indicates that the message should be requeued for later processing.
  - ACKNOWLEDGE_DEADLETTER: Indicates that the message should be moved to the dead-letter queue.
*/
package constant_broker

/*
Selector for which action the acknowledger should take for a particular acknowledge.
*/
type AcknowledgeAction string

const (
	ACKNOWLEDGE_SUCCESS    AcknowledgeAction = "success"    // indicates that the message was processed successfully.
	ACKNOWLEDGE_REQUEUE    AcknowledgeAction = "requeue"    // indicates that the message should be requeued for later processing.
	ACKNOWLEDGE_DEADLETTER AcknowledgeAction = "deadletter" // indicates that the message should be moved to the dead-letter queue.
)
