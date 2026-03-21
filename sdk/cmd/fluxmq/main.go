package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var brokerAddr string

var rootCmd = &cobra.Command{
	Use:   "fluxmq",
	Short: "FluxMQ command-line interface",
	Long:  "fluxmq is the CLI for interacting with a FluxMQ broker.",
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().StringVar(&brokerAddr, "broker", "localhost:9092", "broker address (host:port)")

	rootCmd.AddCommand(topicCmd)
	rootCmd.AddCommand(produceCmd)
	rootCmd.AddCommand(consumeCmd)
	rootCmd.AddCommand(clusterCmd)
}
