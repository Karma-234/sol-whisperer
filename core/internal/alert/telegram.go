package alert

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
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
		slog.Warn("alert_send_failed", slog.String("signature", a.Signature), slog.String("reason", "marshal_error"), slog.String("error", err.Error()))
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.apiURL, bytes.NewReader(body))
	if err != nil {
		slog.Warn("alert_send_failed", slog.String("signature", a.Signature), slog.String("reason", "request_create_error"), slog.String("error", err.Error()))
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		slog.Warn("alert_send_failed", slog.String("signature", a.Signature), slog.String("reason", "http_error"), slog.String("error", err.Error()))
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		err := fmt.Errorf("telegram send failed: %s", resp.Status)
		slog.Warn("alert_send_failed", slog.String("signature", a.Signature), slog.String("reason", "non_2xx_response"), slog.String("status", resp.Status))
		return err
	}

	slog.Info("alert_sent_success", slog.String("signature", a.Signature), slog.String("token_name", a.TokenName))
	return nil
}

func buildMessage(a processor.Alert) string {
	var lines []string
	lines = append(lines, "<b>Volume spike detected</b>")
	lines = append(lines, "")

	if a.TokenName != "" {
		lines = append(lines, "<b>Token:</b> "+escapeHTML(a.TokenName))
	}
	if a.AmountInSOL > 0 {
		solAmount := float64(a.AmountInSOL) / 1e9 // Convert lamports to SOL
		lines = append(lines, "<b>Amount:</b> "+trimFloat(solAmount)+" SOL")
	}
	lines = append(lines, "<b>Mint:</b> "+escapeHTML(a.Mint))
	lines = append(lines, "<b>Swapper:</b> "+escapeHTML(a.Swapper))
	lines = append(lines, "<b>Source:</b> "+escapeHTML(a.Source))
	lines = append(lines, "<b>Window:</b> "+strconv.FormatInt(a.WindowSec, 10)+"s")
	lines = append(lines, "<b>Trades:</b> "+strconv.FormatUint(uint64(a.TradeCount), 10))
	lines = append(lines, "<b>Volume raw:</b> "+strconv.FormatUint(a.VolumeRaw, 10))
	lines = append(lines, "<b>Baseline EWMA:</b> "+trimFloat(a.BaselineEWMA))
	lines = append(lines, "<b>Spike ratio:</b> "+trimFloat(a.SpikeRatio))
	lines = append(lines, "<b>Signature:</b> "+escapeHTML(a.Signature))

	return strings.Join(lines, "\n")
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
