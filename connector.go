package amqp_connector

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"
)
import "github.com/sandsiv/logging"
import amqp "github.com/rabbitmq/amqp091-go"

var logger = logging.NewDefault("RabbitMQ Connector").UnsetFlags(logging.ShortCaller)

func (conn *Connection) establishConnection(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return logging.Trace(ctx.Err())
		default:
			connection, err := amqp.Dial(conn.addr)
			if err == nil {
				logger.Debug("Connection successfully established")
				conn.Connection = connection
				return nil
			}
			logger.LogError(logging.Trace(fmt.Errorf("failed to establish connection: %v, retry after %d sec", err, conn.reconnectionDelaySec)))
			conn.delay()
		}
	}
}

func (conn *Connection) establishChannel(ctx context.Context, channel *Channel, prefetchCount int) error {
	for {
		select {
		case <-ctx.Done():
			return logging.Trace(ctx.Err())
		default:
			ch, err := conn.Connection.Channel()
			if err == nil {
				channel.Channel = ch
				var qosErr error
				if prefetchCount != UnlimitedPrefetchCount {
					qosErr = ch.Qos(prefetchCount, 0, false)
					if qosErr != nil {
						logger.LogError(logging.Trace(errors.New("failed to set QoS")))
					}
				}
				if qosErr == nil {
					logger.Debug("Channel successfully established")
					return nil
				}
			}
			logger.LogError(logging.Trace(fmt.Errorf("failed to establish channel: %v, retry after %d sec", err, conn.reconnectionDelaySec)))
			conn.delay()
		}
	}
}

func (conn *Connection) delay() {
	time.Sleep(time.Duration(conn.reconnectionDelaySec) * time.Second)
}

func (ch *Channel) isClosed() bool {
	return atomic.LoadInt32(&ch.closed) == 1
}

func (ch *Channel) isCanceled(consumer string) bool {
	if v, ok := ch.activeConsumers.Load(consumer); !ok || !v.(bool) {
		return true
	}
	return false
}
