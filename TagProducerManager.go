package messagebroker

/*
Interface that defines the behaviour of a tag producer, which will produce unique hash.
*/
type TagProducerManager interface {
	/*
		Produce a tag that is a unique hash.
	*/
	MakeUniqueTag() string
}
