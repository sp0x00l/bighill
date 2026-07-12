package messaging

import log "github.com/sirupsen/logrus"

type TrainingTopics struct {
	Inference     string
	ModelRegistry string
	Training      string
}

func (t TrainingTopics) List() []string {
	log.Trace("TrainingTopics List")

	topics := []string{}
	if t.Inference != "" {
		topics = append(topics, t.Inference)
	}
	if t.ModelRegistry != "" {
		topics = append(topics, t.ModelRegistry)
	}
	if t.Training != "" {
		topics = append(topics, t.Training)
	}
	return topics
}
