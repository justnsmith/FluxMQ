package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"unicode/utf8"

	"fluxmq/sdk/fluxmq"

	"github.com/spf13/cobra"
)

var (
	consumeGroup       string
	consumeFromBeginning bool
	consumeMaxMessages int
	consumeFormat      string
)

var consumeCmd = &cobra.Command{
	Use:   "consume TOPIC",
	Short: "Consume messages from a topic",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		topic := args[0]

		cfg := fluxmq.ConsumerConfig{
			GroupID:    consumeGroup,
			AutoCommit: true,
			MaxWaitMs:  500,
			MaxBytes:   1 * 1024 * 1024,
		}

		consumer, err := fluxmq.NewConsumer(brokerAddr, cfg)
		if err != nil {
			return fmt.Errorf("connect: %w", err)
		}
		defer consumer.Close()

		if err := consumer.Subscribe(topic); err != nil {
			return fmt.Errorf("subscribe: %w", err)
		}

		// Handle Ctrl+C gracefully.
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

		count := 0
		for {
			select {
			case sig := <-sigCh:
				fmt.Fprintf(os.Stderr, "\nReceived %v, shutting down...\n", sig)
				return nil

			case msg, ok := <-consumer.Messages():
				if !ok {
					return nil
				}
				if err := printMessage(msg, consumeFormat); err != nil {
					fmt.Fprintf(os.Stderr, "error formatting message: %v\n", err)
				}
				count++
				if consumeMaxMessages > 0 && count >= consumeMaxMessages {
					return nil
				}
			}
		}
	},
}

func printMessage(msg *fluxmq.Message, format string) error {
	switch format {
	case "json":
		var valueStr string
		if utf8.Valid(msg.Value) {
			valueStr = string(msg.Value)
		} else {
			valueStr = base64.StdEncoding.EncodeToString(msg.Value)
		}
		obj := map[string]interface{}{
			"partition": msg.Partition,
			"offset":    msg.Offset,
			"value":     valueStr,
		}
		b, err := json.Marshal(obj)
		if err != nil {
			return err
		}
		fmt.Println(string(b))
	default: // "text"
		fmt.Printf("[partition=%d offset=%d] %s\n", msg.Partition, msg.Offset, msg.Value)
	}
	return nil
}

func init() {
	consumeCmd.Flags().StringVar(&consumeGroup, "group", "fluxmq-cli", "consumer group ID")
	consumeCmd.Flags().BoolVar(&consumeFromBeginning, "from-beginning", false, "start consuming from the earliest offset")
	consumeCmd.Flags().IntVar(&consumeMaxMessages, "max-messages", 0, "maximum number of messages to consume (0 = unlimited)")
	consumeCmd.Flags().StringVar(&consumeFormat, "format", "text", "output format: text or json")
}
