package config

/*
Flag indicating the environment the pod is running.
*/
const (
	ENVIRONMENT_PRODUCTION = "production"
	ENVIRONMENT_STAGING    = "staging"
	ENVIRONMENT_DEVELOPING = "developer"
)

const (
	PUBLISHER = "publisher"
	CONSUMER  = "consumer"
)

const (
	NOT_FOUND    int = 404
	CLOSED_BATCH int = 466

	INTERNAL_ERROR int = 500
	RETRY_POSSILBE int = 576

	RECALCULATE         int = 600
	WARNING             int = 601
	INVALID_METHOD      int = 688
	DROP_CALCULATION    int = 698
	ABORT_RECALCULATION int = 699
)
