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
		n, err := consumePartition(ctx, cfg, p, dedup)
		if err != nil {
			return Result{}, fmt.Errorf("failed to consume partition %d: %w", p.ID, err)
		}
		count += n
	}

	return dedup.Results(cfg), nil
}

func consumePartition(ctx context.Context, cfg Config, p kafka.Partition, dedup *Deduplicator) (int, error) {
	addr := fmt.Sprintf("%s:%d", p.Leader.Host, p.Leader.Port)
	conn, err := kafka.DialLeader(ctx, "tcp", addr, cfg.Topic, p.ID)
	if err != nil {
		return 0, fmt.Errorf("failed to dial partition leader: %w", err)
	}
	defer conn.Close()

	startingOffset := cfg.StartingOffset
	endingOffset := cfg.EndOffset
	lastOffset, err := conn.ReadLastOffset()

	if err != nil {
		return 0, fmt.Errorf("failed to read last offset: %w", err)
	}

	if startingOffset >= lastOffset {
		return 0, nil
	}

	_, err = conn.Seek(cfg.StartingOffset, kafka.SeekAbsolute)
	if err != nil {
		return 0, fmt.Errorf("failed to seek to start offset: %w", err)
	}

	fmt.Printf("Consuming partition %d from offset %d until offset %d\n", p.ID, startingOffset, endingOffset)
	fmt.Printf("Last offset is %d\n", lastOffset)

	count := 0
	for {
		batch := conn.ReadBatch(1, 5e6) // min 1 byte, max 5MB
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

			if msg.Offset >= endingOffset {
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
		if currentOffset >= endingOffset || currentOffset >= (lastOffset-1) {
			break
		}
	}

	return count, nil
}
