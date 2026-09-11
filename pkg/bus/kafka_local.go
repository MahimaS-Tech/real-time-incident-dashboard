//go:build !production

package bus

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"sync"
	"time"
)

// LocalProducer is a dependency-free fallback used by unit tests and local builds
// without Kafka libraries. Production Docker builds use kafka_production.go.
type LocalProducer struct {
	mu   sync.Mutex
	file *os.File
}

func NewKafkaProducer(_ []string, _ string, _ int, _ time.Duration) *LocalProducer {
	path := os.Getenv("LOCAL_EVENT_LOG")
	if path == "" {
		path = "incidents.raw.log"
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return &LocalProducer{}
	}
	return &LocalProducer{file: file}
}

func (p *LocalProducer) Publish(_ context.Context, key string, value []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.file == nil {
		return nil
	}
	_, err := fmt.Fprintf(p.file, "%s %s\n", key, base64.StdEncoding.EncodeToString(value))
	return err
}

func (p *LocalProducer) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.file == nil {
		return nil
	}
	return p.file.Close()
}
