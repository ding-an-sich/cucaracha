package main

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/segmentio/kafka-go"
)

func Consume(ctx context.Context, cfg Config) (Result, error) {
	dedup := NewDeduplicator()

	partitions, err := getPartitions(cfg)
	if err != nil {
		return Result{}, fmt.Errorf("failed to get partitions: %w", err)
	}

	count := 0
	for _, partition := range partitions {
		if cfg.Limit > 0 && count >= cfg.Limit {
			break
		}

		n, err := consumePartition(ctx, cfg, partition, dedup, cfg.Limit-count)
		if err != nil {
			return Result{}, fmt.Errorf("failed to consume partition %d: %w", partition, err)
		}
		count += n
	}

	return dedup.Results(cfg), nil
}

func getPartitions(cfg Config) ([]int, error) {
	conn, err := kafka.Dial("tcp", cfg.Brokers[0])
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	partitionList, err := conn.ReadPartitions(cfg.Topic)
	if err != nil {
		return nil, err
	}

	partitions := make([]int, len(partitionList))
	for i, p := range partitionList {
		partitions[i] = p.ID
	}
	return partitions, nil
}

func consumePartition(ctx context.Context, cfg Config, partition int, dedup *Deduplicator, limit int) (int, error) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   cfg.Brokers,
		Topic:     cfg.Topic,
		Partition: partition,
	})
	defer reader.Close()

	if err := reader.SetOffsetAt(ctx, cfg.StartTime); err != nil {
		return 0, fmt.Errorf("failed to set offset at start time: %w", err)
	}

	count := 0

	fmt.Printf("Consuming partition %d from offset %d", partition, reader.Offset())

	for {
		if limit > 0 && count >= limit {
			break
		}

		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return count, err
		}

		if msg.Time.After(cfg.EndTime) {
			break
		}

		dedup.Add(msg.Value, msg.Partition, msg.Offset, msg.Time)
		count++
	}

	return count, nil
}
