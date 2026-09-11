//go:build production

package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"incident-dashboard/pkg/config"
	incidentprocessor "incident-dashboard/pkg/processor"
	"incident-dashboard/pkg/store"

	"github.com/segmentio/kafka-go"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	brokers := config.CSV("KAFKA_BROKERS", "localhost:9092")
	topic := config.Env("RAW_TOPIC", "incidents.raw")
	groupID := config.Env("CONSUMER_GROUP", "incident-processors")
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       config.EnvInt("KAFKA_MIN_BYTES", 1),
		MaxBytes:       config.EnvInt("KAFKA_MAX_BYTES", 10<<20),
		QueueCapacity:  config.EnvInt("KAFKA_QUEUE_CAPACITY", 10000),
		CommitInterval: 0,
		StartOffset:    kafka.FirstOffset,
	})
	defer reader.Close()

	redisStore := store.NewRedisStore(
		config.Env("REDIS_ADDR", "localhost:6379"),
		config.Env("REDIS_PASSWORD", ""),
		config.EnvInt("REDIS_DB", 0),
		config.EnvDuration("DEDUPE_TTL", 7*24*time.Hour),
	)
	defer redisStore.Close()
	if err := redisStore.Ping(ctx); err != nil {
		slog.Error("redis unavailable", "error", err)
		os.Exit(1)
	}

	processor := incidentprocessor.New(redisStore)
	slog.Info("processor started", "brokers", brokers, "topic", topic, "group", groupID)
	for {
		message, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Error("fetch failed", "error", err)
			time.Sleep(250 * time.Millisecond)
			continue
		}
		if err := processor.Process(ctx, message.Value); err != nil {
			slog.Error("process failed; message will be retried", "topic", message.Topic, "partition", message.Partition, "offset", message.Offset, "error", err)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if err := reader.CommitMessages(ctx, message); err != nil {
			slog.Error("commit failed", "error", err)
		}
	}
}
