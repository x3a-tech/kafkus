package kafkus

import (
	"context"
	"errors"
	"fmt"
	"github.com/segmentio/kafka-go"
	"github.com/x3a-tech/configo"
	"github.com/x3a-tech/logit-go"
	"net"
	"strconv"
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
		logger.Info(ctx, fmt.Sprintf(msg, args...))
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

func (p *Producer) TryCreateTopics(ctx context.Context, topics *configo.KafkaTopics) error {
	const op = "transport.kafka.Producer.TryCreateTopics"
	ctx = p.logger.NewOpCtx(ctx, op)

	// Создаем подключение к одному из брокеров
	conn, err := kafka.DialContext(ctx, "tcp", p.writer.Addr.String())
	if err != nil {
		return fmt.Errorf("ошибка подключения к Kafka: %w", err)
	}

	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("ошибка получения контроллера Kafka: %w", err)
	}
	defer conn.Close()

	controllerConn, err := kafka.DialContext(ctx, "tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return fmt.Errorf("ошибка подключения к контроллеру Kafka: %w", err)
	}
	defer controllerConn.Close()

	for _, topic := range topics.List {
		topicConfigs := []kafka.TopicConfig{
			{
				Topic:             topic,
				NumPartitions:     topics.NumPartitions,
				ReplicationFactor: topics.ReplicationFactor,
			},
		}

		err := controllerConn.CreateTopics(topicConfigs...)
		if err != nil {
			// Если топик уже существует, это не считается ошибкой
			if !errors.Is(err, kafka.TopicAlreadyExists) {
				p.logger.Warn(ctx, fmt.Sprintf("Не удалось создать топик %s: %v", topic, err))
			} else {
				p.logger.Info(ctx, fmt.Sprintf("Топик %s уже существует", topic))
			}
		} else {
			p.logger.Info(ctx, fmt.Sprintf("Топик %s успешно создан", topic))
		}
	}

	return nil
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

	err := p.writer.WriteMessages(ctx, msg)
	if err != nil {
		p.logger.Error(ctx, fmt.Errorf("ошибка отправки сообщения в Kafka: %v", err))
		return fmt.Errorf("ошибка отправки сообщения в Kafka: %w", err)
	}

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
