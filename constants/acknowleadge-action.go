package constants

/*
Selector for which action the acknowledger should take for a particular acknowledge.
*/
type AcknowledgeAction string

const (
	ACKNOWLEDGE_SUCCESS    AcknowledgeAction = "success"
	ACKNOWLEDGE_REQUEUE    AcknowledgeAction = "requeue"
	ACKNOWLEDGE_DEADLETTER AcknowledgeAction = "deadletter"
)
