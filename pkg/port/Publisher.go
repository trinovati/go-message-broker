package port

import (
	"context"
)

/*
Enforce a generic form of publish for any simple publisher service.
All adapters of this library must support this methods to keep concise behavior.

Publish is a simple message publishing action.
*/
type Publisher interface {
	Publish(ctx context.Context, header map[string]any, body []byte) error
}
