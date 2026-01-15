package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
)

func Consume(ctx context.Context, cfg Config) (Result, error) {
	dedup := NewDeduplicator()

	mechanism, err := scram.Mechanism(scram.SHA256, os.Getenv("CUCARACHA_SASL_USERNAME"), os.Getenv("CUCARACHA_SASL_PASS"))
	if err != nil {
		panic(err)
	}

	dialer := &kafka.Dialer{
		Timeout:       10 * time.Second,
		DualStack:     true,
		SASLMechanism: mechanism,
		TLS:           &tls.Config{},
	}

	conn, err := dialer.Dial("tcp", cfg.Brokers[0])
	if err != nil {
		return Result{}, fmt.Errorf("failed to dial broker: %w", err)
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions(cfg.Topic)
	if err != nil {
		return Result{}, fmt.Errorf("failed to get partitions: %w", err)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, len(partitions))

	for _, p := range partitions {
		wg.Add(1)
		go func(p kafka.Partition) {
			defer wg.Done()
			_, err := consumePartition(dialer, ctx, cfg, p, dedup)
			if err != nil {
				errCh <- fmt.Errorf("failed to consume partition %d: %w", p.ID, err)
			}
		}(p)
	}

	wg.Wait()
	close(errCh)

	if err := <-errCh; err != nil {
		return Result{}, err
	}

	return dedup.Results(cfg), nil
}

func consumePartition(dialer *kafka.Dialer, ctx context.Context, cfg Config, p kafka.Partition, dedup *Deduplicator) (int, error) {
	conn, err := dialer.DialLeader(ctx, "tcp", cfg.Brokers[0], cfg.Topic, p.ID)
	if err != nil {
		return 0, fmt.Errorf("failed to dial partition leader: %w", err)
	}
	defer conn.Close()

	firstOffset, err := conn.ReadFirstOffset()
	lastOffset, err := conn.ReadLastOffset()

	if err != nil {
		return 0, fmt.Errorf("failed to read last offset: %w", err)
	}

	_, err = conn.Seek(firstOffset, kafka.SeekAbsolute)
	if err != nil {
		return 0, fmt.Errorf("failed to seek to start offset: %w", err)
	}

	fmt.Printf("Consuming partition %d from offset %d until offset %d\n", p.ID, firstOffset, lastOffset)

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

			if msg.Offset >= lastOffset {
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
		if currentOffset >= (lastOffset - 1) {
			break
		}
	}

	return count, nil
}
