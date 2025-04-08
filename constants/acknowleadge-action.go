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
package constants

/*
AcknowledgeAction represents the action to be taken when acknowledging a message.
It is used to categorize how the acknowledgment should be processed by the system.
*/
type AcknowledgeAction string

const (
	// ACKNOWLEDGE_SUCCESS indicates that the message was processed successfully.
	ACKNOWLEDGE_SUCCESS AcknowledgeAction = "success"
	// ACKNOWLEDGE_REQUEUE indicates that the message should be requeued for later processing.
	ACKNOWLEDGE_REQUEUE AcknowledgeAction = "requeue"
	// ACKNOWLEDGE_DEADLETTER indicates that the message should be moved to the dead-letter queue.
	ACKNOWLEDGE_DEADLETTER AcknowledgeAction = "deadletter"
)
