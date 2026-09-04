package platformops

import "context"

// DeliveryRouter assigns internal trigger messages and external runtime events to distinct consumers.
type DeliveryRouter struct {
	trigger  DeliveryClient
	webhooks DeliveryClient
}

// NewDeliveryRouter creates a total outbox topic router.
func NewDeliveryRouter(trigger, webhooks DeliveryClient) (*DeliveryRouter, error) {
	if trigger == nil || webhooks == nil {
		return nil, ErrInvalidArgument
	}
	return &DeliveryRouter{trigger: trigger, webhooks: webhooks}, nil
}

// Deliver dispatches trigger.fired internally and all durable runtime topics externally.
func (r *DeliveryRouter) Deliver(ctx context.Context, message DeliveryMessage) error {
	if message.Topic == "trigger.fired" {
		return r.trigger.Deliver(ctx, message)
	}
	return r.webhooks.Deliver(ctx, message)
}
