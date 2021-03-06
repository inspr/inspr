package kafkaop

import (
	"strconv"
	"strings"

	"go.uber.org/zap"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"inspr.dev/inspr/pkg/ierrors"
	"inspr.dev/inspr/pkg/meta"
)

type kafkaConfiguration struct {
	numberOfPartitions int
	replicationFactor  int
}

func configFromChannel(ch *meta.Channel) (kafkaConfiguration, error) {
	logger.Debug("trying to get Kafka configs from Channel annotations",
		zap.String("channel", ch.Meta.Name),
		zap.Any("annotations", ch.Meta.Annotations))

	config := kafkaConfiguration{
		numberOfPartitions: 1,
		replicationFactor:  1,
	}
	if nPart, ok := ch.Meta.Annotations["kafka.partition.number"]; ok {
		var err error
		config.numberOfPartitions, err = strconv.Atoi(nPart)
		if err != nil {
			config.numberOfPartitions = 1
			logger.Error("invalid 'kafka.partition.number' in Channels annotations")
			return config, ierrors.New(
				"invalid partition configuration %s",
				ch.Meta.Annotations["kafka.partition.number"],
			).InvalidChannel()
		}
	}

	if nPart, ok := ch.Meta.Annotations["kafka.replication.factor"]; ok {
		var err error
		config.replicationFactor, err = strconv.Atoi(nPart)
		if err != nil {
			config.replicationFactor = 1
			logger.Error("invalid 'kafka.replication.factor' in Channels annotations")
			return config, ierrors.New(
				"invalid replication configuration %s",
				ch.Meta.Annotations["kafka.replication.factor"],
			).InvalidChannel()
		}
	}

	return config, nil
}

func toTopic(ch *meta.Channel) string {
	logger.Debug("getting Kafka Topic name given a Channel name and context",
		zap.String("context", ch.Meta.Parent),
		zap.String("channel", ch.Meta.Name))

	return "INSPR_" + ch.Meta.UUID
}

func fromTopic(name string, meta *kafka.Metadata) (ch *meta.Channel) {
	logger.Debug("getting Channel given a Kafka Topic name",
		zap.String("topic", name))

	ch.Meta.Annotations["kafka.partition.number"] = strconv.Itoa(len(meta.Topics[name].Partitions))
	splitName := strings.Split(name, "-")
	if len(splitName) == 4 {
		ch.Meta.Name = splitName[3]
		ch.Meta.Parent = splitName[2]
	} else if len(splitName) == 3 {
		ch.Meta.Name = splitName[2]
		ch.Meta.Parent = splitName[1]
	}
	return
}
