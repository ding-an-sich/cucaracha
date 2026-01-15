package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
)

type Deduplicator struct {
	mu   sync.Mutex
	seen map[uint64][]Occurrence
}

func NewDeduplicator() *Deduplicator {
	return &Deduplicator{
		seen: make(map[uint64][]Occurrence),
	}
}

func (d *Deduplicator) Add(value []byte, partition int, offset int64, timestamp time.Time) {
	hash := xxhash.Sum64(value)
	d.mu.Lock()
	d.seen[hash] = append(d.seen[hash], Occurrence{
		Partition: partition,
		Offset:    offset,
		Timestamp: timestamp,
	})
	d.mu.Unlock()
}

func (d *Deduplicator) Results(cfg Config) Result {
	var duplicates []Duplicate
	uniqueCount := 0
	totalMessages := 0

	for hash, occurrences := range d.seen {
		totalMessages += len(occurrences)
		if len(occurrences) > 1 {
			duplicates = append(duplicates, Duplicate{
				Hash:        fmt.Sprintf("%016x", hash),
				Count:       len(occurrences),
				Occurrences: occurrences,
			})
		} else {
			uniqueCount++
		}
	}

	return Result{
		Topic:           cfg.Topic,
		MessagesRead:    totalMessages,
		UniqueMessages:  uniqueCount,
		DuplicateGroups: len(duplicates),
		Duplicates:      duplicates,
	}
}
