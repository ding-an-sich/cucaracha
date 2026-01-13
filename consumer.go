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

	conn, err := kafka.Dial("tcp", cfg.Brokers[0])
	if err != nil {
		return Result{}, fmt.Errorf("failed to dial broker: %w", err)
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions(cfg.Topic)
	if err != nil {
		return Result{}, fmt.Errorf("failed to get partitions: %w", err)
	}

	count := 0
	for _, p := range partitions {
		if cfg.Limit > 0 && count >= cfg.Limit {
			break
		}

		n, err := consumePartition(ctx, cfg, p, dedup, cfg.Limit-count)
		if err != nil {
			return Result{}, fmt.Errorf("failed to consume partition %d: %w", p.ID, err)
		}
		count += n
	}

	return dedup.Results(cfg), nil
}

func consumePartition(ctx context.Context, cfg Config, p kafka.Partition, dedup *Deduplicator, limit int) (int, error) {
	addr := fmt.Sprintf("%s:%d", p.Leader.Host, p.Leader.Port)
	conn, err := kafka.DialLeader(ctx, "tcp", addr, cfg.Topic, p.ID)
	if err != nil {
		return 0, fmt.Errorf("failed to dial partition leader: %w", err)
	}
	defer conn.Close()

	startOffset, err := conn.ReadOffset(cfg.StartTime)
	if err != nil {
		return 0, fmt.Errorf("failed to read offset at start time: %w", err)
	}

	endOffset, err := conn.ReadLastOffset()
	if err != nil {
		return 0, fmt.Errorf("failed to read last offset: %w", err)
	}

	if startOffset >= endOffset {
		return 0, nil
	}

	_, err = conn.Seek(startOffset, kafka.SeekAbsolute)
	if err != nil {
		return 0, fmt.Errorf("failed to seek to start offset: %w", err)
	}

	fmt.Printf("Consuming partition %d from offset %d\n", p.ID, startOffset)

	count := 0
	for {
		batch := conn.ReadBatch(1, 1e6) // min 1 byte, max 1MB
		done := false
		var readErr error

		for {
			msg, err := batch.ReadMessage()
			if err != nil {
				if !errors.Is(err, io.EOF) {
					readErr = err
				}
				break
			}

			if msg.Offset >= endOffset || msg.Time.After(cfg.EndTime) || (limit > 0 && count >= limit) {
				done = true
				break
			}

			dedup.Add(msg.Value, msg.Partition, msg.Offset, msg.Time)
			count++
		}

		batch.Close()

		if readErr != nil {
			return count, readErr
		}
		if done {
			break
		}

		currentOffset, _ := conn.Seek(0, kafka.SeekCurrent)
		if currentOffset >= endOffset {
			break
		}
	}

	return count, nil
}
