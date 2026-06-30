package cmd

import (
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var brokerConnectivityCmd = &cobra.Command{
	Use:   "broker-connectivity",
	Short: "Check network connectivity to Kafka brokers",
	Run: func(cmd *cobra.Command, args []string) {
		brokerConnectivity()
	},
}

func brokerConnectivity() {
	var brokerList []string
	if brokers == "" {
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
		for _, broker := range metadata.Brokers {
			brokerList = append(brokerList, broker.Host+":"+fmt.Sprint(broker.Port))
		}
	} else {
		brokerList = strings.Split(brokers, ",")
	}
	timeout := 5 * time.Second

	log.Info("====================================================")
	log.Info("Kafka Broker State:")
	log.Info("====================================================")

	for _, broker := range brokerList {
		address := strings.TrimSpace(broker)
		if !strings.Contains(address, ":") {
			address += ":9092"
		}
		conn, err := net.DialTimeout("tcp", address, timeout)
		if err != nil {
			log.Errorf("- %s: Failed to connect to broker. %s", address, err)
			continue
		}
		log.Infof("- %s: Successfully connected to broker", address)
		conn.Close()
	}
}
