package kafkus

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"github.com/x3a-tech/configo"
	"github.com/x3a-tech/logit-go"
	"time"
)

type Producer struct {
	writer *kafka.Writer
	logger logit.Logger
}

// NewProducer создает нового Kafka продюсера
func NewProducer(cfg *configo.KafkaProducer, logger logit.Logger) *Producer {
	const op = "transport.kafka.NewProducer"
	ctx := logger.NewOpCtx(context.Background(), op)

	acks := kafka.RequiredAcks(cfg.RequiredAcks)
	if !(acks == kafka.RequireNone || acks == kafka.RequireOne || acks == kafka.RequireAll) {
		logger.Warn(ctx, fmt.Sprintf("Недопустимое значение для producer.requiredAcks: %d. Используется значение по умолчанию 1 (Leader).", cfg.RequiredAcks))
		acks = kafka.RequireOne
	}

	debugLogger := kafka.LoggerFunc(func(msg string, args ...interface{}) {
		logger.Debug(ctx, fmt.Sprintf(msg, args...))
	})
	errorLogger := kafka.LoggerFunc(func(msg string, args ...interface{}) {
		logger.Error(ctx, fmt.Errorf(msg, args...))
	})

	writer := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Brokers...),
		RequiredAcks: acks,
		Async:        cfg.Async,
		BatchSize:    cfg.BatchSize,
		BatchTimeout: cfg.BatchTimeout,
		WriteTimeout: cfg.WriteTimeout,
		MaxAttempts:  cfg.MaxAttempts,
		Logger:       debugLogger,
		ErrorLogger:  errorLogger,
	}

	logger.Info(ctx, fmt.Sprintf("Kafka producer сконфигурирован для брокеров: %v с RequiredAcks=%d", cfg.Brokers, acks))

	return &Producer{
		writer: writer,
		logger: logger,
	}
}

func (p *Producer) SendMessage(ctx context.Context, topic string, key []byte, value []byte) error {
	const op = "transport.kafka.Producer.SendMessage"
	ctx = p.logger.NewOpCtx(ctx, op)
	ctx = p.logger.NewOpCtx(ctx, "transport.kafka.Producer.MessageSend")
	if key != nil {
		ctx = p.logger.NewOpCtx(ctx, "transport.kafka.Producer.MessageKey")
	}

	msg := kafka.Message{
		Topic: topic,
		Key:   key,
		Value: value,
		Time:  time.Now(),
	}

	// Используем контекст для WriteMessages
	err := p.writer.WriteMessages(ctx, msg)
	if err != nil {
		p.logger.Error(ctx, fmt.Errorf("ошибка отправки сообщения в Kafka: %v", err))
		return fmt.Errorf("ошибка отправки сообщения в Kafka: %w", err)
	}

	p.logger.Debug(ctx, "сообщение успешно отправлено в Kafka")
	return nil
}

func (p *Producer) Close() error {
	const op = "transport.kafka.Producer.Close"
	ctx := p.logger.NewOpCtx(context.Background(), op)
	p.logger.Info(ctx, "Закрытие Kafka producer...")

	err := p.writer.Close()
	if err != nil {
		p.logger.Error(ctx, fmt.Errorf("ошибка закрытия Kafka writer: %v", err))
		return fmt.Errorf("ошибка закрытия Kafka writer: %w", err)
	}

	p.logger.Info(ctx, "Kafka producer успешно закрыт.")
	return nil
}
