package queue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// JobsQueue is the queue workers consume from.
const JobsQueue = "jobs"

const (
	maxBackoff     = 5 * time.Second
	publishRetries = 5
)

var retryDelays = []time.Duration{
	5 * time.Second,
	30 * time.Second,
	2 * time.Minute,
	10 * time.Minute,
	30 * time.Minute,
}

func RetryDelay(attempt int) time.Duration {
	i := attempt - 1
	if i < 0 {
		i = 0
	}
	if i >= len(retryDelays) {
		i = len(retryDelays) - 1
	}
	return retryDelays[i]
}

func retryQueueName(d time.Duration) string {
	return fmt.Sprintf("%s.retry.%ds", JobsQueue, int(d.Seconds()))
}

// Publisher owns an AMQP connection + channel for sending job IDs.
type Publisher struct {
	url string
	log *slog.Logger
	mu   sync.RWMutex
	conn *amqp.Connection
	ch   *amqp.Channel
	closed    chan struct{}
	closeOnce sync.Once
}

// NewPublisher connects, opens a channel, declares the jobs queue, and starts watching the connection so it can recover on its own
func NewPublisher(url string, log *slog.Logger) (*Publisher, error) {
	p := &Publisher{
		url:    url,
		log:    log,
		closed: make(chan struct{}),
	}
	if err := p.connect(); err != nil {
		return nil, err
	}
	go p.watch()
	return p, nil
}

func (p *Publisher) connect() error {
	conn, err := amqp.Dial(p.url)
	if err != nil {
		return fmt.Errorf("dial rabbitmq: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return fmt.Errorf("open channel: %w", err)
	}
	// durable=true: the queue definition survives a broker restart.
	if _, err := ch.QueueDeclare(JobsQueue, true, false, false, false, nil); err != nil {
		ch.Close()
		conn.Close()
		return fmt.Errorf("declare queue: %w", err)
	}

	if err := ch.Confirm(false); err != nil {
		ch.Close()
		conn.Close()
		return fmt.Errorf("enable publisher confirms: %w", err)
	}
	if err := declareRetryQueues(ch); err != nil {
		ch.Close()
		conn.Close()
		return err
	}

	p.mu.Lock()
	p.conn, p.ch = conn, ch
	p.mu.Unlock()
	return nil
}

func declareRetryQueues(ch *amqp.Channel) error {
	for _, d := range retryDelays {
		args := amqp.Table{
			"x-message-ttl":             int64(d / time.Millisecond),
			"x-dead-letter-exchange":    "",
			"x-dead-letter-routing-key": JobsQueue,
		}
		if _, err := ch.QueueDeclare(retryQueueName(d), true, false, false, false, args); err != nil {
			return fmt.Errorf("declare retry queue %s: %w", retryQueueName(d), err)
		}
	}
	return nil
}

func (p *Publisher) watch() {
	for {
		p.mu.RLock()
		conn := p.conn
		p.mu.RUnlock()

		notify := conn.NotifyClose(make(chan *amqp.Error, 1))

		select {
		case <-p.closed:
			return
		case reason := <-notify:
			if reason == nil {
				return
			}
			p.log.Warn("amqp connection lost, reconnecting", "err", reason)
		}

		for attempt := 1; ; attempt++ {
			select {
			case <-p.closed:
				return
			case <-time.After(backoff(attempt)):
			}

			if err := p.connect(); err != nil {
				p.log.Warn("amqp reconnect failed", "attempt", attempt, "err", err)
				continue
			}
			p.log.Info("amqp reconnected", "attempts", attempt)
			break
		}
	}
}

func backoff(attempt int) time.Duration {
	if attempt > 5 {
		attempt = 5
	}
	d := min(250 * time.Millisecond * time.Duration(1<<uint(attempt-1)), maxBackoff)
	return d
}

func (p *Publisher) Close() {
	p.closeOnce.Do(func() { close(p.closed) })

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ch != nil {
		p.ch.Close()
	}
	if p.conn != nil {
		p.conn.Close()
	}
}

func (p *Publisher) channel() *amqp.Channel {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.ch
}

func (p *Publisher) Consume(ctx context.Context) (<-chan amqp.Delivery, error) {
	deliveries, err := p.subscribe()
	if err != nil {
		return nil, err
	}
	out := make(chan amqp.Delivery)
	go p.forward(ctx, out, deliveries)
	return out, nil
}

func (p *Publisher) subscribe() (<-chan amqp.Delivery, error) {
	ch := p.channel()
	if ch == nil {
		return nil, errors.New("consume: no amqp channel")
	}
	if err := ch.Qos(1, 0, false); err != nil {
		return nil, fmt.Errorf("set qos: %w", err)
	}
	deliveries, err := ch.Consume(JobsQueue, "", false, false, false, false, nil)
	if err != nil {
		return nil, fmt.Errorf("consume: %w", err)
	}
	return deliveries, nil
}

func (p *Publisher) forward(ctx context.Context, out chan<- amqp.Delivery, deliveries <-chan amqp.Delivery) {
	defer close(out)

	for {
		select {
		case <-ctx.Done():
			return
		case <-p.closed:
			return
		case d, ok := <-deliveries:
			if !ok {
				p.log.Warn("delivery channel closed, resubscribing")
				next, err := p.resubscribe(ctx)
				if err != nil {
					p.log.Info("stopped consuming", "reason", err)
					return
				}
				deliveries = next
				p.log.Info("resubscribed to jobs queue")
				continue
			}
			select {
			case out <- d:
			case <-ctx.Done():
				return
			}
		}
	}
}

func (p *Publisher) resubscribe(ctx context.Context) (<-chan amqp.Delivery, error) {
	for attempt := 1; ; attempt++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-p.closed:
			return nil, errors.New("publisher closed")
		case <-time.After(backoff(attempt)):
		}

		deliveries, err := p.subscribe()
		if err != nil {
			p.log.Warn("resubscribe failed", "attempt", attempt, "err", err)
			continue
		}
		return deliveries, nil
	}
}

func (p *Publisher) PublishJobID(ctx context.Context, jobID string) error {
	return p.publish(ctx, JobsQueue, jobID)
}

func (p *Publisher) PublishRetry(ctx context.Context, jobID string, attempt int) (time.Duration, error) {
	d := RetryDelay(attempt)
	return d, p.publish(ctx, retryQueueName(d), jobID)
}

func (p *Publisher) publish(ctx context.Context, routingKey, jobID string) error {
	var lastErr error

	for attempt := 1; attempt <= publishRetries; attempt++ {
		ch := p.channel()
		if ch == nil {
			lastErr = errors.New("no amqp channel")
		} else if err := publishConfirmed(ctx, ch, routingKey, jobID); err != nil {
			lastErr = err
		} else {
			if attempt > 1 {
				p.log.Info("publish succeeded after retry", "attempts", attempt)
			}
			return nil
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("publish to %s: %w", routingKey, ctx.Err())
		case <-time.After(backoff(attempt)):
		}
	}

	return fmt.Errorf("publish to %s after %d attempts: %w", routingKey, publishRetries, lastErr)
}

func publishConfirmed(ctx context.Context, ch *amqp.Channel, routingKey, jobID string) error {
	conf, err := ch.PublishWithDeferredConfirmWithContext(ctx,
		"",
		routingKey,
		false, false,
		amqp.Publishing{
			ContentType:  "text/plain",
			Body:         []byte(jobID),
			DeliveryMode: amqp.Persistent, // message survives a broker restart (with a durable queue)
		})
	if err != nil {
		return err
	}

	acked, err := conf.WaitContext(ctx)
	if err != nil {
		return fmt.Errorf("await confirm: %w", err)
	}
	if !acked {
		return errors.New("broker nacked the message")
	}
	return nil
}
