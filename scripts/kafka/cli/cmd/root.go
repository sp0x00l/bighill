package cmd

import (
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	brokers string

	rootCmd = &cobra.Command{
		Use:   "kafka-cli",
		Short: "A CLI tool for Kafka debugging",
		Long:  `A command-line tool to check Kafka cluster state, list topics, and inspect messages.`,
	}
)

func Execute() {
	cobra.CheckErr(rootCmd.Execute())
}

func init() {
	log.SetFormatter(&log.TextFormatter{FullTimestamp: true})
	rootCmd.PersistentFlags().StringVarP(&brokers, "brokers", "b", "localhost:9092", "Comma-separated list of Kafka brokers")
	rootCmd.AddCommand(clusterStateCmd)
	rootCmd.AddCommand(topicsListCmd)
	rootCmd.AddCommand(consumerLagCmd)
	rootCmd.AddCommand(producerFlushCmd)
	rootCmd.AddCommand(brokerConnectivityCmd)
	rootCmd.AddCommand(topicPartitionsCmd)
	rootCmd.AddCommand(producerMetricsCmd)
}
