package messaging_test

import (
	"context"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqsTypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"lib/shared_lib/messaging"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type MockSQSClient struct {
	Messages []sqsTypes.Message
	mu       sync.Mutex
}

func NewMockSQSClient() *MockSQSClient {
	return &MockSQSClient{}
}

func (m *MockSQSClient) SendMessage(ctx context.Context, input *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	message := sqsTypes.Message{
		MessageId:     aws.String("test-message-id"),
		ReceiptHandle: aws.String("test-receipt-handle"),
		Body:          input.MessageBody,
	}

	m.Messages = append(m.Messages, message)

	return &sqs.SendMessageOutput{
		MessageId: message.MessageId,
	}, nil
}

func (m *MockSQSClient) ReceiveMessage(ctx context.Context, input *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	output := &sqs.ReceiveMessageOutput{
		Messages: m.Messages,
	}
	// Optionally clear after reading
	m.Messages = nil
	return output, nil
}

var _ = Describe("dlq infrastructure", func() {
	var message = []byte("test message")
	var mockSQS *MockSQSClient
	var dlqURL string
	var dlq messaging.Dlq

	BeforeEach(func() {
		mockSQS = NewMockSQSClient()
		dlqURL = "https://sqs.eu-west-1.amazonaws.com/123456789012/bighill-dlq"
		dlq = messaging.Dlq{
			SQS:    mockSQS,
			DlqURL: dlqURL,
		}
	})

	Context("when writing a message to the DLQ", func() {
		It("should return no error and the DLQ should contain the message", func() {
			err := dlq.WriteMessage(context.Background(), message)
			Expect(err).To(BeNil())
			Expect(len(mockSQS.Messages)).To(Equal(1))
			Expect(*mockSQS.Messages[0].Body).To(Equal(string(message)))
			Expect(mockSQS.Messages[0].MessageId).ToNot(BeNil())
			Expect(mockSQS.Messages[0].MessageId).To(Equal(aws.String("test-message-id")))
			Expect(mockSQS.Messages[0].ReceiptHandle).ToNot(BeNil())
			Expect(mockSQS.Messages[0].ReceiptHandle).To(Equal(aws.String("test-receipt-handle")))
		})
	})

	Context("when reading messages from the DLQ", func() {
		It("should return the messages", func() {
			messages := []sqsTypes.Message{
				{
					MessageId:     aws.String("test-message-id-1"),
					ReceiptHandle: aws.String("test-receipt-handle-1"),
					Body:          aws.String("test message 1"),
				},
				{
					MessageId:     aws.String("test-message-id-2"),
					ReceiptHandle: aws.String("test-receipt-handle-2"),
					Body:          aws.String("test message 2"),
				},
			}
			mockSQS.Messages = messages

			ctx := context.Background()
			receivedMessages, err := dlq.ReadMessages(ctx, int32(10))
			Expect(err).To(BeNil())
			Expect(len(receivedMessages)).To(Equal(len(messages)))
			for i := range receivedMessages {
				Expect(string(receivedMessages[i])).To(Equal(*messages[i].Body))
			}
		})
	})
})
