package main

import "time"

type Config struct {
	Brokers        []string
	Topic          string
	StartingOffset int64
	EndOffset      int64
}

type Occurrence struct {
	Partition int       `json:"partition"`
	Offset    int64     `json:"offset"`
	Timestamp time.Time `json:"timestamp"`
}

type Duplicate struct {
	Hash        string       `json:"hash"`
	Count       int          `json:"count"`
	Occurrences []Occurrence `json:"occurrences"`
}

type TimeRange struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
}

type Result struct {
	Topic           string      `json:"topic"`
	MessagesRead    int         `json:"messages_read"`
	UniqueMessages  int         `json:"unique_messages"`
	DuplicateGroups int         `json:"duplicate_groups"`
	Duplicates      []Duplicate `json:"duplicates"`
}
