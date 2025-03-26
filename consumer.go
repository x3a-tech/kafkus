package kafkus

import (
	"context"
	"errors"
	"fmt"
	"github.com/segmentio/kafka-go"
	"github.com/x3a-tech/configo"
	"github.com/x3a-tech/logit-go"
	"sync"
	"time"
)

// MessageHandler - тип функции для обработки сообщений из Kafka
type MessageHandler func(ctx context.Context, message []byte, topic string, partition int, offset int64) error

type Consumer struct {
	reader  *kafka.Reader
	handler MessageHandler
	logger  logit.Logger
	wg      sync.WaitGroup
	cancel  context.CancelFunc
}

func NewConsumer(ctx context.Context, cfg *configo.KafkaConsumer, handler MessageHandler, logger logit.Logger) (*Consumer, error) {
	const op = "kafkaus.NewConsumer"
	ctx = logger.NewOpCtx(ctx, op)

	startOffsetKafka := kafka.LastOffset
	if cfg.StartOffset == "earliest" {
		startOffsetKafka = kafka.FirstOffset
	}

	debugLogger := kafka.LoggerFunc(func(msg string, args ...interface{}) {
		logger.Info(ctx, fmt.Sprintf(msg, args...))
	})
	errorLogger := kafka.LoggerFunc(func(msg string, args ...interface{}) {
		logger.Error(ctx, fmt.Errorf(msg, args...))
	})

	// Настраиваем Reader, используя поля из cfg *config.KafkaConsumer
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:           cfg.Brokers,
		GroupID:           cfg.GroupID,
		GroupTopics:       cfg.Topics,
		MinBytes:          cfg.MinBytes,
		MaxBytes:          cfg.MaxBytes,
		MaxWait:           cfg.MaxWait,
		CommitInterval:    cfg.CommitInterval,
		StartOffset:       startOffsetKafka,
		Logger:            debugLogger,
		ErrorLogger:       errorLogger,
		HeartbeatInterval: cfg.HeartbeatInterval,
		SessionTimeout:    cfg.SessionTimeout,
		RebalanceTimeout:  cfg.RebalanceTimeout,
		MaxAttempts:       cfg.MaxAttempts,
	})

	logger.Info(ctx, fmt.Sprintf("kafka consumer сконфигурирован для брокеров: %v, группы: %s, топиков: %v", cfg.Brokers, cfg.GroupID, cfg.Topics))

	return &Consumer{
		reader:  r,
		handler: handler,
		logger:  logger,
	}, nil
}

// Run запускает цикл чтения сообщений из Kafka в отдельной горутине
func (c *Consumer) Run(ctx context.Context) {
	const op = "transport.kafka.Consumer.Run"
	ctx = c.logger.NewOpCtx(ctx, op)

	runCtx, cancel := context.WithCancel(ctx)
	c.cancel = cancel

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.logger.Info(runCtx, fmt.Sprintf("Запуск Kafka consumer для топиков: %v", c.reader.Config().GroupTopics))

		for {
			m, err := c.reader.FetchMessage(runCtx)
			if err != nil {
				select {
				case <-runCtx.Done():
					c.logger.Info(runCtx, "Контекст отменен, Kafka consumer останавливается.")
					return
				default:
					if errors.Is(err, context.Canceled) {
						c.logger.Info(runCtx, "Reader остановлен из-за отмены контекста.")
						return
					}
					if errors.Is(err, kafka.ErrGroupClosed) {
						c.logger.Warn(runCtx, "Группа Kafka закрыта. Возможно, произошла ребалансировка или таймаут сессии.")
						return // или реализуйте логику переподключения
					}
					if errors.Is(err, kafka.RequestTimedOut) {
						c.logger.Warn(runCtx, "Тайм-аут при чтении сообщения из Kafka. Повторная попытка...")
						time.Sleep(5 * time.Second) // Увеличенная задержка перед повторной попыткой
						continue
					}
					c.logger.Error(runCtx, fmt.Errorf("ошибка чтения сообщения из Kafka: %w", err))
					time.Sleep(1 * time.Second)
					continue
				}
			}

			msgCtx := c.logger.NewOpCtx(runCtx, "kafka.Consumer.Run.HandleMessage")

			err = c.handler(msgCtx, m.Value, m.Topic, m.Partition, m.Offset)
			if err != nil {
				c.logger.Error(msgCtx, fmt.Errorf("ошибка обработки сообщения: %w", err))
				// TODO: Добавить логику Dead Letter Queue (DLQ) или ретраев, если нужно
				continue // Переходим к следующему сообщению, не коммитя текущее
			}

			// Коммитим сообщение после успешной обработки
			if err := c.reader.CommitMessages(runCtx, m); err != nil {
				c.logger.Error(msgCtx, fmt.Errorf("ошибка коммита сообщения: %w", err))
				// Если коммит не удался, рискуем обработать сообщение повторно
			}
		}
	}()
}
