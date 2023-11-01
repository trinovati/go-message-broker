package dto

/*
Generic object that message broker use to recieve acknowledge.
*/
type Acknowledge struct {
	Data    []byte
	Id      string
	Success bool
	Requeue bool
	Enqueue bool
	Target  Target
}
