package kafka

import (
	"context"
	"encoding/json"

	"github.com/IBM/sarama"
)

type Producer struct { p sarama.SyncProducer; topic string }

func New(brokers []string, topic string) (*Producer, error) {
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	pr, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil { return nil, err }
	return &Producer{p: pr, topic: topic}, nil
}

func (k *Producer) PublishDriverLocation(ctx context.Context, key string, payload any) error {
	b, _ := json.Marshal(payload)
	msg := &sarama.ProducerMessage{ Topic: k.topic, Key: sarama.StringEncoder(key), Value: sarama.ByteEncoder(b) }
	_, _, err := k.p.SendMessage(msg)
	return err
}
