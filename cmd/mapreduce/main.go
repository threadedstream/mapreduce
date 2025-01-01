package main

import (
	"context"
	"flag"

	"github.com/threadedstream/mapreduce/coordinator"
	"go.uber.org/zap"
)

var (
	isCoordinator = flag.Bool("coordinator", false, "Whether to run as coordinator")
)

func main() {
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := setupLogger()

	if *isCoordinator {
		if err := coordinator.Main(ctx, logger); err != nil {
			logger.Fatal("failed to run coordinator")
		}
	}
}

func setupLogger() *zap.Logger {
	cfg := zap.NewDevelopmentConfig()
	logger, err := cfg.Build()
	if err != nil {
		panic(err)
	}
	return logger
}
