package portal

import (
	"context"
	"time"

	"github.com/rs/zerolog/log"
	"golang.org/x/time/rate"
)

const (
	tenantTLSQueueSize = 128
	tenantTLSQueueWait = 750 * time.Millisecond
	tenantTLSBurst     = 16
)

type tenantTLSRequest struct {
	ctx   context.Context
	ready chan bool
}

type tenantTLSQueue struct {
	pending chan tenantTLSRequest
	limiter *rate.Limiter
	warn    rate.Sometimes
}

func newTenantTLSQueue(perSecond int) *tenantTLSQueue {
	return &tenantTLSQueue{
		// 디스패처가 처리 중인 항목 하나도 대기열 용량에 포함한다.
		pending: make(chan tenantTLSRequest, tenantTLSQueueSize-1),
		limiter: rate.NewLimiter(rate.Limit(perSecond), min(perSecond, tenantTLSBurst)),
		warn:    rate.Sometimes{Interval: time.Minute},
	}
}

func (q *tenantTLSQueue) wait(ctx context.Context) bool {
	waitCtx, cancel := context.WithTimeout(ctx, tenantTLSQueueWait)
	defer cancel()
	request := tenantTLSRequest{ctx: waitCtx, ready: make(chan bool, 1)}
	reason := "full"
	select {
	case q.pending <- request:
		select {
		case admitted := <-request.ready:
			if admitted && waitCtx.Err() == nil {
				return true
			}
		case <-waitCtx.Done():
		}
		reason = "wait limit"
	default:
	}
	if ctx.Err() == nil {
		q.warn.Do(func() {
			log.Warn().Str("reason", reason).Msg("tenant TLS queue rejected a connection")
		})
	}
	return false
}

func (q *tenantTLSQueue) run(ctx context.Context) {
	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
			return
		case request := <-q.pending:
			request.ready <- (q.limiter.Wait(request.ctx) == nil)
		}
	}
}
