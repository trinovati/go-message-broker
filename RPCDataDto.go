package messagebroker

/*
DTO struct used by RPC service for storring useful information for control and response to correct service.
*/
type RPCDataDto struct {
	Data          []byte
	CorrelationId string
	ResponseRoute string
}

/*
Created a struct used by RPC service for storring useful information for control and response to correct service.
*/
func NewRPCDataDto() *RPCDataDto {
	return &RPCDataDto{}
}
