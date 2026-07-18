package queue

import (
	"context"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// JobsQueue is the queue workers consume from.
const JobsQueue = "jobs"

// Publisher owns an AMQP connection + channel for sending job IDs.
type Publisher struct {
	conn *amqp.Connection
	ch   *amqp.Channel
}

// NewPublisher connects, opens a channel, and declares the jobs queue.
func NewPublisher(url string) (*Publisher, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("dial rabbitmq: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("open channel: %w", err)
	}
	// durable=true: the queue definition survives a broker restart.
	if _, err := ch.QueueDeclare(JobsQueue, true, false, false, false, nil); err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("declare queue: %w", err)
	}
	return &Publisher{conn: conn, ch: ch}, nil
}

func (p *Publisher) Close() {
	p.ch.Close()
	p.conn.Close()
}

// Consume starts delivering messages from the jobs queue on the returned channel.
func (p *Publisher) Consume() (<-chan amqp.Delivery, error) {
	if err := p.ch.Qos(1, 0, false); err != nil {
		return nil, fmt.Errorf("set qos: %w", err)
	}
	deliveries, err := p.ch.Consume(JobsQueue, "", false, false, false, false, nil)
	if err != nil {
		return nil, fmt.Errorf("consume: %w", err)
	}
	return deliveries, nil
}

// PublishJobID sends a job ID to the jobs queue.
func (p *Publisher) PublishJobID(ctx context.Context, jobID string) error {
	err := p.ch.PublishWithContext(ctx,
		"",        // default exchange: routes directly to the queue named by the routing key
		JobsQueue, // routing key
		false, false,
		amqp.Publishing{
			ContentType:  "text/plain",
			Body:         []byte(jobID),
			DeliveryMode: amqp.Persistent, // message survives a broker restart (with a durable queue)
		})
	if err != nil {
		return fmt.Errorf("publish job id: %w", err)
	}
	return nil
}
