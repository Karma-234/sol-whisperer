package types

type HeliusWebhookEvent struct {
	EventType string `json:"eventType"`
	Slot      uint64 `json:"slot"`
}
