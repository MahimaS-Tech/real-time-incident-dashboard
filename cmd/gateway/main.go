package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"incident-dashboard/pkg/bus"
	"incident-dashboard/pkg/config"
	"incident-dashboard/pkg/gateway"
)

func main() {
	brokers := config.CSV("KAFKA_BROKERS", "localhost:9092")
	topic := config.Env("RAW_TOPIC", "incidents.raw")
	producer := bus.NewKafkaProducer(
		brokers,
		topic,
		config.EnvInt("KAFKA_BATCH_SIZE", 1000),
		config.EnvDuration("KAFKA_BATCH_TIMEOUT", 5*time.Millisecond),
	)
	defer producer.Close()

	app := gateway.NewServer(producer)
	server := &http.Server{
		Addr:              ":" + config.Env("PORT", "8080"),
		Handler:           app.Routes(),
		ReadHeaderTimeout: 2 * time.Second,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    8 << 10,
	}

	go func() {
		slog.Info("gateway listening", "addr", server.Addr, "brokers", brokers, "topic", topic)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("gateway failed", "error", err)
			os.Exit(1)
		}
	}()

	shutdown(server)
}

func shutdown(server *http.Server) {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_ = server.Shutdown(ctx)
}
