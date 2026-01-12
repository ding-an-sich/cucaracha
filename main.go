package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"
)

func main() {
	var (
		brokers   string
		topic     string
		startStr  string
		endStr    string
		limit     int
	)

	flag.StringVar(&brokers, "brokers", "", "Comma-separated Kafka broker addresses (required)")
	flag.StringVar(&topic, "topic", "", "Topic to read from (required)")
	flag.StringVar(&startStr, "start", "", "Start time in RFC3339 format (required)")
	flag.StringVar(&endStr, "end", "", "End time in RFC3339 format (required)")
	flag.IntVar(&limit, "limit", 0, "Max messages to read (0 = unlimited)")
	flag.Parse()

	if brokers == "" || topic == "" || startStr == "" || endStr == "" {
		fmt.Fprintln(os.Stderr, "Error: --brokers, --topic, --start, and --end are required")
		flag.Usage()
		os.Exit(1)
	}

	startTime, err := time.Parse(time.RFC3339, startStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: invalid start time: %v\n", err)
		os.Exit(1)
	}

	endTime, err := time.Parse(time.RFC3339, endStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: invalid end time: %v\n", err)
		os.Exit(1)
	}

	if endTime.Before(startTime) {
		fmt.Fprintln(os.Stderr, "Error: end time must be after start time")
		os.Exit(1)
	}

	if startTime.Unix() < 0 {
		fmt.Fprintln(os.Stderr, "Error: start time must be after 1970-01-01")
		os.Exit(1)
	}

	cfg := Config{
		Brokers:   strings.Split(brokers, ","),
		Topic:     topic,
		StartTime: startTime,
		EndTime:   endTime,
		Limit:     limit,
	}

	result, err := Consume(context.Background(), cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(result); err != nil {
		fmt.Fprintf(os.Stderr, "Error encoding JSON: %v\n", err)
		os.Exit(1)
	}
}
