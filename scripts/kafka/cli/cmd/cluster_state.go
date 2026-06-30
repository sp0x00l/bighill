package cmd

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var clusterStateCmd = &cobra.Command{
	Use:   "cluster-state",
	Short: "Check the Kafka cluster state. Print the controller ID and broker list.",
	Run: func(cmd *cobra.Command, args []string) {
		clusterState()
	},
}

func clusterState() {
	conf := &kafka.ConfigMap{"bootstrap.servers": brokers}

	adminClient, err := kafka.NewAdminClient(conf)
	if err != nil {
		log.Fatalf("Failed to create Admin client: %s", err)
	}
	defer adminClient.Close()

	metadata, err := adminClient.GetMetadata(nil, true, 5000)
	if err != nil {
		log.Fatalf("Failed to get cluster metadata: %s", err)
	}

	log.Info("====================================================")
	log.Info("Kafka Cluster State:")
	log.Info("====================================================")
	log.Infof(" - Controller ID: %d", metadata.OriginatingBroker.ID)
	log.Infof(" - Brokers:")
	for _, broker := range metadata.Brokers {
		log.Infof("   - ID: %d, Host: %s, Port: %d", broker.ID, broker.Host, broker.Port)
	}
}
