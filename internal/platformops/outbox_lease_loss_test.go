package platformops

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

type leaseLossStore struct {
	*deliveryTestStore
	lost chan struct{}
}

func (s *leaseLossStore) RenewDeliveryLease(context.Context, DeliveryMessage, string, time.Time, time.Duration) (time.Time, error) {
	select {
	case <-s.lost:
	default:
		close(s.lost)
	}
	return time.Time{}, ErrStaleLease
}

type cancellableSideEffect struct {
	active  atomic.Int64
	started chan struct{}
	stopped chan struct{}
}

func (e *cancellableSideEffect) Deliver(ctx context.Context, _ DeliveryMessage) error {
	if e.active.Add(1) != 1 {
		return errors.New("concurrent stale side effect")
	}
	defer e.active.Add(-1)
	close(e.started)
	<-ctx.Done()
	close(e.stopped)
	return ctx.Err()
}

// 验证 outbox 续租失败会立即取消可中断副作用并等待其退出，下一 owner 不会与旧调用并发。
func TestOutboxDispatcherCancelsDeliveryImmediatelyAfterLeaseLoss(t *testing.T) {
	now := time.Now().UTC()
	base := &deliveryTestStore{message: DeliveryMessage{
		ID: "out_lease_loss", OrganizationID: "organization_a", Attempts: 1, MaxAttempts: 2,
	}}
	store := &leaseLossStore{deliveryTestStore: base, lost: make(chan struct{})}
	effect := &cancellableSideEffect{started: make(chan struct{}), stopped: make(chan struct{})}
	dispatcher, err := NewOutboxDispatcher(store, effect, "dispatcher_old", 15*time.Millisecond, time.Second, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	dispatcher.now = func() time.Time { return time.Now().UTC() }
	result, err := dispatcher.DispatchOne(context.Background(), "organization_a", now)
	if !errors.Is(err, ErrStaleLease) || result.MessageID != "out_lease_loss" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	select {
	case <-effect.stopped:
	default:
		t.Fatal("dispatcher returned before the stale side effect exited")
	}
	if effect.active.Load() != 0 {
		t.Fatalf("stale side effect remained active: %d", effect.active.Load())
	}
}
