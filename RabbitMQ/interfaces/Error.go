package interfaces

type Error interface {
	SetStatus(status int) Error
	SetBody(body string) Error

	GetStatus() int
	GetBody() string
	GetStack() error

	New(message string) Error
	Merge(err error) Error
	Wrap(err error, message string) Error
	Unwrap() error
	Error() string
	String() string
	Print()
}
