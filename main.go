package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
)

func main() {
	var (
		brokers string
		topic   string
		start   int64
		end     int64
	)

	flag.StringVar(&brokers, "brokers", "", "Comma-separated Kafka broker addresses (required)")
	flag.StringVar(&topic, "topic", "", "Topic to read from (required)")
	flag.Int64Var(&start, "start", -1, "Start reading from this offset (required)")
	flag.Int64Var(&end, "end", 0, "Stop reading at this offset (required)")
	flag.Parse()

	if brokers == "" || topic == "" || start == -1 || end == 0 {
		fmt.Fprintln(os.Stderr, "Error: --brokers, --topic, --start, and --end are required")
		flag.Usage()
		os.Exit(1)
	}

	cfg := Config{
		Brokers:        strings.Split(brokers, ","),
		Topic:          topic,
		StartingOffset: start,
		EndOffset:      end,
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
