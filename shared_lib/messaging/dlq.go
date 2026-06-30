package messaging

import (
	"context"
	"fmt"
	"strings"

	log "github.com/sirupsen/logrus"
	metrics "lib/shared_lib/metrics"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

type DLQ interface {
	WriteMessage(ctx context.Context, message []byte) error
	ReadMessages(ctx context.Context, maxMessages int32) ([][]byte, error)
}

type SQSAPI interface {
	SendMessage(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
	ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
}

type Dlq struct {
	SQS    SQSAPI
	DlqURL string
}

func NewDLQ(ctx context.Context, dlqURL string) *Dlq {
	log.Trace("NewDLQ")

	if strings.HasPrefix(dlqURL, "https://sqs") {
		log.Infof("Using AWS SQS %s", dlqURL)
		cfg, err := config.LoadDefaultConfig(ctx)
		if err != nil {
			log.WithContext(ctx).WithError(err).Fatal("failed to load AWS config")
		}

		sqsClient := sqs.NewFromConfig(cfg)
		return &Dlq{
			SQS:    sqsClient,
			DlqURL: dlqURL,
		}
	}

	return &Dlq{
		SQS:    &LocalSQS{},
		DlqURL: dlqURL,
	}
}

func (d *Dlq) WriteMessage(ctx context.Context, message []byte) error {
	log.Trace("DLQ WriteMessage")

	_, err := d.SQS.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    &d.DlqURL,
		MessageBody: aws.String(string(message)),
	})
	if err != nil {
		metrics.Default().RecordError(ctx, metrics.BoundaryExternal, "dlq_write", metrics.ErrorClassNetwork, "")
		log.WithContext(ctx).Errorf("failed to send message (%s) to DLQ: %s", string(message), d.DlqURL)
		return fmt.Errorf("failed to send message to DLQ: %w", err)
	}
	return nil
}

func (d *Dlq) ReadMessages(ctx context.Context, maxMessages int32) ([][]byte, error) {
	log.Trace("DLQ ReadMessages")
	output, err := d.SQS.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            &d.DlqURL,
		MaxNumberOfMessages: maxMessages,
	})
	if err != nil {
		metrics.Default().RecordError(ctx, metrics.BoundaryExternal, "dlq_read", metrics.ErrorClassNetwork, "")
		log.WithContext(ctx).Errorf("failed to read messages from DLQ: %s", d.DlqURL)
		return nil, fmt.Errorf("failed to read messages from DLQ: %w", err)
	}

	messages := make([][]byte, 0, len(output.Messages))
	for _, message := range output.Messages {
		if message.Body != nil {
			messages = append(messages, []byte(*message.Body))
		}
	}
	return messages, nil
}
