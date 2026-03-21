package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var brokerAddr string

var rootCmd = &cobra.Command{
	Use:   "fluxmq-bench",
	Short: "FluxMQ benchmarking harness",
	Long:  "fluxmq-bench measures produce/consume throughput and end-to-end latency.",
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().StringVar(&brokerAddr, "broker", "localhost:9092", "broker address (host:port)")

	rootCmd.AddCommand(produceCmd)
	rootCmd.AddCommand(consumeCmd)
	rootCmd.AddCommand(e2eCmd)
}
