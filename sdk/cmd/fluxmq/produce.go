package main

import (
	"bufio"
	"fmt"
	"os"
	"time"

	"fluxmq/sdk/fluxmq"

	"github.com/spf13/cobra"
)

var (
	produceKey       string
	produceValue     string
	producePartition int32
	produceCount     int
	produceInterval  time.Duration
)

var produceCmd = &cobra.Command{
	Use:   "produce TOPIC",
	Short: "Produce messages to a topic",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		topic := args[0]

		client, err := fluxmq.NewClient(brokerAddr)
		if err != nil {
			return fmt.Errorf("connect: %w", err)
		}
		defer client.Close()

		var keyBytes []byte
		if produceKey != "" {
			keyBytes = []byte(produceKey)
		}

		send := func(value []byte) error {
			part, offset, err := client.Produce(topic, producePartition, keyBytes, value)
			if err != nil {
				return err
			}
			fmt.Printf("partition=%d offset=%d\n", part, offset)
			return nil
		}

		if produceValue != "" {
			// Send --value N times.
			for i := 0; i < produceCount; i++ {
				if err := send([]byte(produceValue)); err != nil {
					fmt.Fprintf(os.Stderr, "error: %v\n", err)
				}
				if produceInterval > 0 && i < produceCount-1 {
					time.Sleep(produceInterval)
				}
			}
			return nil
		}

		// Read from stdin, one message per line.
		scanner := bufio.NewScanner(os.Stdin)
		sent := 0
		for scanner.Scan() {
			if produceCount > 0 && sent >= produceCount {
				break
			}
			line := scanner.Text()
			if err := send([]byte(line)); err != nil {
				fmt.Fprintf(os.Stderr, "error: %v\n", err)
			}
			sent++
			if produceInterval > 0 {
				time.Sleep(produceInterval)
			}
		}
		return scanner.Err()
	},
}

func init() {
	produceCmd.Flags().StringVar(&produceKey, "key", "", "message key")
	produceCmd.Flags().StringVar(&produceValue, "value", "", "message value (reads from stdin if omitted)")
	produceCmd.Flags().Int32Var(&producePartition, "partition", -1, "target partition (-1 = auto)")
	produceCmd.Flags().IntVar(&produceCount, "count", 1, "number of messages to send")
	produceCmd.Flags().DurationVar(&produceInterval, "interval", 0, "interval between messages")
}
