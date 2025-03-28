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

func (c *Consumer) Run(ctx context.Context) {
	const op = "transport.kafka.Consumer.Run"
	ctx = c.logger.NewOpCtx(ctx, op)

	runCtx, cancel := context.WithCancel(context.Background())
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
					c.logger.Info(runCtx, "Контекст отменен, Kafka consumer останавливает чтение.")
					return
				default:
					if errors.Is(err, context.Canceled) {
						c.logger.Info(runCtx, "Чтение отменено.")
						return
					}
					if errors.Is(err, kafka.ErrGroupClosed) {
						c.logger.Warn(runCtx, "Группа Kafka закрыта. Consumer останавливается.")
						return
					}
					if errors.Is(err, kafka.RequestTimedOut) {
						c.logger.Warn(runCtx, "Тайм-аут при чтении сообщения из Kafka. Повторная попытка...")
						select {
						case <-time.After(1 * time.Second):
							continue
						case <-runCtx.Done():
							c.logger.Info(runCtx, "Контекст отменен во время паузы после таймаута.")
							return
						}
					}
					c.logger.Error(runCtx, fmt.Errorf("ошибка чтения сообщения из Kafka: %w", err))
					select {
					case <-time.After(1 * time.Second):
						continue
					case <-runCtx.Done():
						c.logger.Info(runCtx, "Контекст отменен во время паузы после ошибки.")
						return
					}
				}
			}

			msgCtx := c.logger.NewOpCtx(runCtx, "kafka.Consumer.Run.HandleMessage")

			processErr := c.handler(msgCtx, m.Value, m.Topic, m.Partition, m.Offset)

			if processErr != nil {
				c.logger.Error(msgCtx, fmt.Errorf("ошибка обработки сообщения: %w", processErr))
				continue
			}

			if err := c.reader.CommitMessages(runCtx, m); err != nil {
				select {
				case <-runCtx.Done():
					c.logger.Info(msgCtx, "Контекст отменен во время попытки коммита.")
					return
				default:
					c.logger.Error(msgCtx, fmt.Errorf("ошибка коммита сообщения: %w", err))
				}
			}
		}
	}()
}

func (c *Consumer) Close() error {
	const op = "transport.kafka.Consumer.Close"
	ctx := c.logger.NewOpCtx(context.Background(), op)

	c.logger.Info(ctx, "Начало закрытия Kafka consumer...")

	if c.cancel != nil {
		c.logger.Info(ctx, "Отправка сигнала отмены в горутину Run...")
		c.cancel()
	} else {
		c.logger.Warn(ctx, "Функция cancel не инициализирована, возможно Run() не был вызван.")
	}

	c.logger.Info(ctx, "Ожидание завершения горутины Run...")
	c.wg.Wait()
	c.logger.Info(ctx, "Горутина Run завершена.")

	c.logger.Info(ctx, "Закрытие Kafka reader...")
	err := c.reader.Close()
	if err != nil {
		c.logger.Error(ctx, fmt.Errorf("ошибка при закрытии Kafka reader: %w", err))
		c.logger.Warn(ctx, "Kafka consumer закрыт с ошибкой.")
		return fmt.Errorf("ошибка при закрытии Kafka reader: %w", err)
	}

	c.logger.Info(ctx, "Kafka consumer успешно закрыт.")
	return nil
}
