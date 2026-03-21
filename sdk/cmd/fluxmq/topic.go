package main

import (
	"fmt"
	"os"

	"fluxmq/sdk/fluxmq"

	"github.com/spf13/cobra"
)

var topicCmd = &cobra.Command{
	Use:   "topic",
	Short: "Manage topics",
}

// ─── topic create ─────────────────────────────────────────────────────────────

var (
	topicCreateName       string
	topicCreatePartitions int32
)

var topicCreateCmd = &cobra.Command{
	Use:   "create",
	Short: "Create a topic",
	RunE: func(cmd *cobra.Command, args []string) error {
		if topicCreateName == "" {
			return fmt.Errorf("--name is required")
		}
		client, err := fluxmq.NewClient(brokerAddr)
		if err != nil {
			return fmt.Errorf("connect: %w", err)
		}
		defer client.Close()

		if err := client.CreateTopic(topicCreateName, topicCreatePartitions); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Created topic %q with %d partition(s).\n", topicCreateName, topicCreatePartitions)
		return nil
	},
}

// ─── topic list ───────────────────────────────────────────────────────────────

var topicListCmd = &cobra.Command{
	Use:   "list",
	Short: "List all topics",
	RunE: func(cmd *cobra.Command, args []string) error {
		client, err := fluxmq.NewClient(brokerAddr)
		if err != nil {
			return fmt.Errorf("connect: %w", err)
		}
		defer client.Close()

		topics, err := client.Metadata()
		if err != nil {
			return fmt.Errorf("metadata: %w", err)
		}
		if len(topics) == 0 {
			fmt.Println("No topics found.")
			return nil
		}
		fmt.Printf("%-40s  %s\n", "TOPIC", "PARTITIONS")
		fmt.Printf("%-40s  %s\n", "-----", "----------")
		for _, t := range topics {
			fmt.Printf("%-40s  %d\n", t.Name, t.NumPartitions)
		}
		return nil
	},
}

// ─── topic describe ───────────────────────────────────────────────────────────

var topicDescribeCmd = &cobra.Command{
	Use:   "describe NAME",
	Short: "Describe a topic",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		name := args[0]
		client, err := fluxmq.NewClient(brokerAddr)
		if err != nil {
			return fmt.Errorf("connect: %w", err)
		}
		defer client.Close()

		topics, err := client.Metadata()
		if err != nil {
			return fmt.Errorf("metadata: %w", err)
		}
		for _, t := range topics {
			if t.Name == name {
				fmt.Printf("Topic:      %s\n", t.Name)
				fmt.Printf("Partitions: %d\n", t.NumPartitions)
				return nil
			}
		}
		return fmt.Errorf("topic %q not found", name)
	},
}

func init() {
	topicCreateCmd.Flags().StringVar(&topicCreateName, "name", "", "topic name (required)")
	topicCreateCmd.Flags().Int32Var(&topicCreatePartitions, "partitions", 1, "number of partitions")

	topicCmd.AddCommand(topicCreateCmd)
	topicCmd.AddCommand(topicListCmd)
	topicCmd.AddCommand(topicDescribeCmd)
}
