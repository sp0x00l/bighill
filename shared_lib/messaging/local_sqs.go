package messaging

import (
	"context"
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqsTypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	log "github.com/sirupsen/logrus"
)

type LocalSQS struct {
	messages []string
	mu       sync.Mutex
}

func (m *LocalSQS) ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	log.Trace("LocalSQS: ReceiveMessage")

	m.mu.Lock()
	defer m.mu.Unlock()
	messages := make([]sqsTypes.Message, 0, len(m.messages))
	for _, msg := range m.messages {
		messages = append(messages, sqsTypes.Message{
			Body:          aws.String(msg),
			ReceiptHandle: aws.String(msg),
		})
	}
	return &sqs.ReceiveMessageOutput{
		Messages: messages,
	}, nil
}

func (m *LocalSQS) SendMessage(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	log.Trace("LocalSQS SendMessage")

	m.mu.Lock()
	defer m.mu.Unlock()
	if params.MessageBody != nil {
		m.messages = append(m.messages, *params.MessageBody)
	}
	return &sqs.SendMessageOutput{
		// Local emulator value for AWS SQS' service-generated message ID.
		MessageId: aws.String(fmt.Sprintf("local-sqs-%d", len(m.messages))),
	}, nil
}
