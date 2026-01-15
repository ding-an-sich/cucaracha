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
	)

	flag.StringVar(&brokers, "brokers", "", "Comma-separated Kafka broker addresses (required)")
	flag.StringVar(&topic, "topic", "", "Topic to read from (required)")
	flag.Parse()

	if brokers == "" || topic == "" {
		fmt.Fprintln(os.Stderr, "Error: --brokers, --topic, are required")
		flag.Usage()
		os.Exit(1)
	}

	cfg := Config{
		Brokers: strings.Split(brokers, ","),
		Topic:   topic,
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
