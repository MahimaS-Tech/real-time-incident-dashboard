//go:build !production

package main

import "log/slog"

func main() {
	slog.Info("processor production build requires Kafka dependencies; run with: go run -tags production ./cmd/processor")
}
