package amqp_connector

import (
	"context"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sandsiv/logging"
	"sync"
	"sync/atomic"
	"time"
)

const UnlimitedPrefetchCount = -1

type Connection struct {
	*amqp.Connection
	reconnectionDelaySec int
	heartbeatSec         int
	readWriteDeadlineSec int
	qosPrefetchCount     int
	addr                 string
}

type Channel struct {
	Connection *Connection
	*amqp.Channel
	activeConsumers sync.Map
	closed          int32
}

func Dial(ctx context.Context, addr string, reconnectionDelaySec, heartbeatSec, readWriteDeadlineSec int) (*Connection, error) {
	connection := &Connection{
		addr:                 addr,
		reconnectionDelaySec: reconnectionDelaySec,
		heartbeatSec:         heartbeatSec,
		readWriteDeadlineSec: readWriteDeadlineSec,
	}

	err := connection.establishConnection(ctx)
	if err == nil {
		go func() {
			for {
				select {
				case <-ctx.Done():
					logger.Warning("Graceful shutdown. Exited from reconnection loop")
					return
				case reason := <-connection.NotifyClose(make(chan *amqp.Error)):
					if reason != nil && reason.Recover {
						logger.Warning(fmt.Sprintf("Connection was closed by recoverable reason: '%s' -- '%s'", addr, reason.Error()))
						continue
					}
					reasonMsg := ""
					if reason != nil {
						reasonMsg = reason.Reason
					}
					logger.LogError(logging.Trace(fmt.Errorf("pool lost connection by reason: %s. Do reconnection", reasonMsg)))
					_ = connection.establishConnection(ctx)
				}
			}
		}()
	}
	return connection, logging.Trace(err)
}

func (conn *Connection) Channel(ctx context.Context, prefetchCount int) (*Channel, error) {
	channel := &Channel{Connection: conn}
	err := conn.establishChannel(ctx, channel, prefetchCount)
	if err == nil {
		go func() {
			for {
				select {
				case <-ctx.Done():
					logger.Warning("Graceful shutdown. Exited from reconnection loop")
					return
				case reason := <-channel.NotifyClose(make(chan *amqp.Error)):
					if reason != nil && reason.Recover {
						logger.Warning(fmt.Sprintf("Connection was closed by recoverable reason: '%s' -- '%s'", conn.addr, reason.Error()))
						continue
					}
					reasonMsg := ""
					if reason != nil {
						reasonMsg = reason.Reason
					}
					logger.LogError(logging.Trace(fmt.Errorf("pool lost channel by reason: %v. Do reconnection", reasonMsg)))
					_ = conn.establishChannel(ctx, channel, prefetchCount)
				}
			}
		}()
	}
	return channel, logging.Trace(err)
}

func (conn *Connection) CloseConnection() error {
	if conn != nil && conn.Connection != nil {
		return logging.Trace(conn.Connection.Close())
	}
	return nil
}

func (ch *Channel) Consume(ctx context.Context, queue string, consumer string, autoAck bool, exclusive bool, noLocal bool, noWait bool, args amqp.Table) <-chan amqp.Delivery {
	deliveries := make(chan amqp.Delivery)
	ch.activeConsumers.Store(consumer, true)
	go func() {
		for {
			select {
			case <-ctx.Done():
				close(deliveries)
				return
			default:
				d, err := ch.Channel.Consume(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
				if ch.isCanceled(consumer) || ch.isClosed() {
					logger.Debug(fmt.Sprintf("Consuming for '%s' queue was manually closed", queue))
					close(deliveries)
					return
				}
				if err != nil {
					logger.LogError(logging.Trace(fmt.Errorf("failed to consume from '%s' queue: %v, retry after 4 sec", queue, err)))
					time.Sleep(4 * time.Second)
					continue
				}
				for msg := range d {
					deliveries <- msg
				}
			}
		}
	}()
	return deliveries
}

func (ch *Channel) CloseChannel() error {
	if ch != nil && ch.Channel != nil {
		if ch.isClosed() {
			return logging.Trace(amqp.ErrClosed)
		}
		err := ch.Channel.Close()
		if err != nil {
			return logging.Trace(err)
		}
		atomic.StoreInt32(&ch.closed, 1)
	}
	return nil
}

func (ch *Channel) CancelChannel(consumer string, noWait bool) error {
	if ch != nil && ch.Channel != nil {
		if ch.isCanceled(consumer) {
			return logging.Trace(fmt.Errorf("consumer '%s' already canceled", consumer))
		}
		err := ch.Channel.Cancel(consumer, noWait)
		if err != nil {
			return logging.Trace(err)
		}
		ch.activeConsumers.Store(consumer, false)
	}
	return nil
}
