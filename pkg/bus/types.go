package bus

import "context"

type Producer interface {
	Publish(ctx context.Context, key string, value []byte) error
	Close() error
}
