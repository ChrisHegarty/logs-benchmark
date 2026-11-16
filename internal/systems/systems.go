// Package systems contains the per-system clients that send the benchmark
// workload to each log database over its native HTTP API. All systems receive
// the same information, encoded in the format each expects.
package systems

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/VictoriaMetrics/logs-benchmark/internal/source/synthetic"
)

// Batch is one ready-to-send ingestion request body.
type Batch struct {
	Body  []byte
	Lines int
}

// LogEntry is one generated record with its timestamp.
type LogEntry struct {
	Rec synthetic.Record
	Ts  time.Time
}

// groupByStream splits entries into one group per (host, app) stream, keeping
// the groups in order of first appearance and the entries in input order.
func groupByStream(entries []LogEntry) [][]LogEntry {
	type streamKey struct {
		host *synthetic.HostInfo
		app  *synthetic.AppInfo
	}
	idx := make(map[streamKey]int)
	var groups [][]LogEntry
	for _, e := range entries {
		k := streamKey{e.Rec.Host, e.Rec.App}
		i, ok := idx[k]
		if !ok {
			i = len(groups)
			idx[k] = i
			groups = append(groups, nil)
		}
		groups[i] = append(groups[i], e)
	}
	return groups
}

// WriteClient sends log lines to one system under test over its native bulk
// API. Write clients must be safe for concurrent Send calls.
type WriteClient interface {
	// WaitReady polls until the system accepts requests or a deadline passes.
	WaitReady() error
	// PrepareBatch encodes the entries into the system's native format as one
	// request body ready to send. The caller controls how many entries per batch.
	PrepareBatch(entries []LogEntry) (Batch, error)
	// Send ingests one prepared batch, retrying on backpressure (429/502/503/504);
	// returns the retry count. Cancelling ctx aborts an in-flight send.
	Send(ctx context.Context, b Batch) (retries int, err error)
}

// splitSystemType separates the engine type from its variant options. A system
// type may carry options after semicolons, e.g.
// "elasticsearch;probe=seqno;set.index.refresh_interval=30s". Only the
// elasticsearch adapter accepts options; the others reject them, so a typo
// cannot silently run a different experiment.
func splitSystemType(sysType string) (base string, opts []string) {
	parts := strings.Split(sysType, ";")
	return parts[0], parts[1:]
}

func rejectOptions(base string, opts []string) error {
	if len(opts) > 0 {
		return fmt.Errorf("system type %q does not take options, got %q", base, opts)
	}
	return nil
}

// newElasticsearch builds the adapter, the one engine that takes options. Both
// the write and the query client are the same value.
func newElasticsearch(addr string, opts []string) (*elasticsearch, error) {
	o, err := parseESOptions(opts)
	if err != nil {
		return nil, err
	}
	return &elasticsearch{addr: addr, opts: o}, nil
}

// NewWriteClient returns the write client for the given engine type at addr.
func NewWriteClient(sysType, addr string) (WriteClient, error) {
	sysType, opts := splitSystemType(sysType)
	if sysType == "elasticsearch" {
		es, err := newElasticsearch(addr, opts)
		if err != nil {
			return nil, err // not `return es, err`: that would be a non-nil interface
		}
		return es, nil
	}
	if err := rejectOptions(sysType, opts); err != nil {
		return nil, err
	}
	switch sysType {
	case "victorialogs":
		return &victoriaLogs{addr: addr}, nil
	case "loki":
		return &loki{addr: addr}, nil
	case "loki-json":
		return &lokiJSON{loki{addr: addr}}, nil
	case "victorialogs-loki":
		return &victoriaLogsLoki{victoriaLogs{addr: addr}}, nil
	case "signoz":
		return &signoz{addr: addr}, nil
	case "opensearch":
		return &opensearch{addr: addr}, nil
	case "clickstack":
		return &clickstack{addr: addr}, nil
	}
	return nil, fmt.Errorf("unknown system type %q", sysType)
}

// jsonInt parses a JSON number or a quoted number (123 or "123") to int64.
func jsonInt(raw json.RawMessage) (int64, error) {
	s := strings.Trim(strings.TrimSpace(string(raw)), `"`)
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, fmt.Errorf("not a number: %s", truncate(string(raw), 40))
	}
	return int64(f), nil
}

// Query is one benchmark query against one system.
type Query struct {
	Name string
	// resolved to [now-Window, now] at run time, like dashboards and alerts do
	Window time.Duration
	// Do runs the query over [start, end] and returns the body, fully read.
	// The call is timed, so it must do nothing besides the request and the read.
	Do func(ctx context.Context, start, end time.Time) ([]byte, error)
	// Check validates the body after the measurement; a failure discards the
	// latency sample and counts as an error.
	Check func(body []byte) error
}

// QueryParams holds the query inputs that depend on the generated data;
// they come from the data source, so the targets exist in every system.
type QueryParams struct {
	App string // the service the stream-scoped queries target
}

// storedRowsWindow is the look-back window the time-filtered StoredRows queries count
// over ([now-storedRowsWindow, now]); it must cover all ingested data, and a run starts
// empty and lasts far less.
const storedRowsWindow = 7 * 24 * time.Hour

// QueryClient runs the benchmark query set against one system under test.
type QueryClient interface {
	// WaitReady polls until the system accepts requests or a deadline passes.
	WaitReady() error
	// Queries returns the benchmark query set; same meaning in every system.
	Queries(p QueryParams) []Query
	// StoredRows returns the count of durably stored records, or nil if the system
	// has no exact count to report. A non-nil error is a transient read failure.
	StoredRows(ctx context.Context) (*int64, error)
}

// NewQueryClient returns the query client for the given engine type at addr.
func NewQueryClient(sysType, addr string) (QueryClient, error) {
	sysType, opts := splitSystemType(sysType)
	if sysType == "elasticsearch" {
		es, err := newElasticsearch(addr, opts)
		if err != nil {
			return nil, err // not `return es, err`: that would be a non-nil interface
		}
		return es, nil
	}
	if err := rejectOptions(sysType, opts); err != nil {
		return nil, err
	}
	switch sysType {
	case "victorialogs":
		return &victoriaLogs{addr: addr}, nil
	case "loki":
		return &loki{addr: addr}, nil
	case "loki-json":
		return &lokiJSON{loki{addr: addr}}, nil
	case "victorialogs-loki":
		return &victoriaLogsLoki{victoriaLogs{addr: addr}}, nil
	case "signoz":
		return &signoz{addr: addr}, nil
	case "opensearch":
		return &opensearch{addr: addr}, nil
	case "clickstack":
		return &clickstack{addr: addr}, nil
	}
	return nil, fmt.Errorf("unknown system type %q", sysType)
}
