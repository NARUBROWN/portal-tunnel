package portal

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"golang.org/x/time/rate"

	"github.com/gosuda/portal-tunnel/v2/utils"
)

const (
	tenantWaitPageMaxBytes = 32 << 10
	tenantWaitPageTimeout  = 3 * time.Second
	tenantWaitPageRetry    = 5
	tenantWaitPageToken    = "{{retry_seconds}}"
)

type tenantWaitPage struct {
	pages   [4]string
	active  chan struct{}
	limiter *rate.Limiter
}

func loadTenantWaitPage(path string) (*tenantWaitPage, error) {
	if path == "" {
		return nil, nil
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("stat tenant waiting page: %w", err)
	}
	if !info.Mode().IsRegular() || info.Size() > tenantWaitPageMaxBytes {
		return nil, errors.New("tenant waiting page must be a regular file up to 32 KiB")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open tenant waiting page: %w", err)
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, tenantWaitPageMaxBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read tenant waiting page: %w", err)
	}
	if len(data) > tenantWaitPageMaxBytes || !strings.Contains(string(data), tenantWaitPageToken) {
		return nil, errors.New("tenant waiting page must be up to 32 KiB and contain {{retry_seconds}}")
	}

	page := &tenantWaitPage{
		active:  make(chan struct{}, 32),
		limiter: rate.NewLimiter(120, 16),
	}
	for i := range page.pages {
		page.pages[i] = strings.ReplaceAll(string(data), tenantWaitPageToken, strconv.Itoa(tenantWaitPageRetry+i))
	}
	return page, nil
}

func (p *tenantWaitPage) serve(ctx context.Context, conn net.Conn, config *tls.Config, serverName string) {
	select {
	case p.active <- struct{}{}:
		defer func() { <-p.active }()
	default:
		return
	}
	if !p.limiter.Allow() {
		return
	}

	// 브라우저에 응답할 한 연결만 처리한다. 앱이나 제어 API로 전달하지 않는다.
	if err := conn.SetDeadline(time.Now().Add(tenantWaitPageTimeout)); err != nil {
		return
	}
	tlsConn := tls.Server(conn, config)
	if err := tlsConn.HandshakeContext(ctx); err != nil {
		return
	}
	req, err := http.ReadRequest(bufio.NewReader(io.LimitReader(tlsConn, 16<<10)))
	if err != nil {
		return
	}
	variant := rand.IntN(len(p.pages))
	body := "Service temporarily unavailable. Try again shortly.\n"
	contentType := "text/plain; charset=utf-8"
	if req.Method == http.MethodGet && utils.NormalizeHostWithoutPort(req.Host) == serverName &&
		(req.Header.Get("Sec-Fetch-Mode") == "navigate" ||
			(req.Header.Get("Sec-Fetch-Mode") == "" && strings.Contains(req.Header.Get("Accept"), "text/html"))) {
		body = p.pages[variant]
		contentType = "text/html; charset=utf-8"
	}
	header := http.Header{
		"Cache-Control":           {"no-store"},
		"Content-Type":            {contentType},
		"Content-Security-Policy": {"default-src 'none'; style-src 'unsafe-inline'; img-src data:; base-uri 'none'; frame-ancestors 'none'; form-action 'none'"},
		"Referrer-Policy":         {"no-referrer"},
		"Retry-After":             {strconv.Itoa(tenantWaitPageRetry + variant)},
		"X-Content-Type-Options":  {"nosniff"},
		"X-Robots-Tag":            {"noindex"},
	}
	response := http.Response{
		StatusCode:    http.StatusServiceUnavailable,
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        header,
		Body:          io.NopCloser(strings.NewReader(body)),
		ContentLength: int64(len(body)),
		Request:       req,
		Close:         true,
	}
	_ = response.Write(tlsConn)
}
