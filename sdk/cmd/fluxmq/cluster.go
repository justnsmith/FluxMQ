package main

import (
	"fmt"

	"fluxmq/sdk/fluxmq"

	"github.com/spf13/cobra"
)

var clusterCmd = &cobra.Command{
	Use:   "cluster",
	Short: "Cluster information commands",
}

var clusterInfoCmd = &cobra.Command{
	Use:   "info",
	Short: "Display cluster metadata (topics and partitions)",
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

		fmt.Printf("Broker: %s\n\n", brokerAddr)
		if len(topics) == 0 {
			fmt.Println("No topics found.")
			return nil
		}
		fmt.Printf("%-40s  %s\n", "TOPIC", "PARTITIONS")
		fmt.Printf("%-40s  %s\n", "-----", "----------")
		for _, t := range topics {
			fmt.Printf("%-40s  %d\n", t.Name, t.NumPartitions)
		}
		fmt.Printf("\nTotal topics: %d\n", len(topics))
		return nil
	},
}

func init() {
	clusterCmd.AddCommand(clusterInfoCmd)
}
