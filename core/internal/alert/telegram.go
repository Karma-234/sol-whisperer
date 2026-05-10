package alert

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/karma-234/sol-whisperer/core/internal/processor"
)

type TelegramSink struct {
	token  string
	chatID string
	client *http.Client
	apiURL string
}

func NewTelegramSink(token, chatID string) *TelegramSink {
	if token == "" || chatID == "" {
		return nil
	}

	return &TelegramSink{
		token:  token,
		chatID: chatID,
		client: &http.Client{Timeout: 3 * time.Second},
		apiURL: "https://api.telegram.org/bot" + token + "/sendMessage",
	}
}

func (s *TelegramSink) Send(ctx context.Context, a processor.Alert) error {
	if s == nil {
		return nil
	}

	text := buildMessage(a)
	payload := map[string]string{
		"chat_id":                  s.chatID,
		"text":                     text,
		"parse_mode":               "HTML",
		"disable_web_page_preview": "true",
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.apiURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("telegram send failed: %s", resp.Status)
	}

	return nil
}

func buildMessage(a processor.Alert) string {
	return strings.Join([]string{
		"<b>Volume spike detected</b>",
		"",
		"<b>Mint:</b> " + escapeHTML(a.Mint),
		"<b>Swapper:</b> " + escapeHTML(a.Swapper),
		"<b>Source:</b> " + escapeHTML(a.Source),
		"<b>Window:</b> " + strconv.FormatInt(a.WindowSec, 10) + "s",
		"<b>Trades:</b> " + strconv.FormatUint(uint64(a.TradeCount), 10),
		"<b>Volume raw:</b> " + strconv.FormatUint(a.VolumeRaw, 10),
		"<b>Baseline EWMA:</b> " + trimFloat(a.BaselineEWMA),
		"<b>Spike ratio:</b> " + trimFloat(a.SpikeRatio),
		"<b>Signature:</b> " + escapeHTML(a.Signature),
	}, "\n")
}

func escapeHTML(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '&':
			b.WriteString("&amp;")
		case '<':
			b.WriteString("&lt;")
		case '>':
			b.WriteString("&gt;")
		case '"':
			b.WriteString("&quot;")
		case '\'':
			b.WriteString("&#39;")
		default:
			b.WriteByte(s[i])
		}
	}
	return b.String()
}

func trimFloat(v float64) string {
	return strings.TrimRight(strings.TrimRight(fmt.Sprintf("%.4f", v), "0"), ".")
}

var _ url.URL
