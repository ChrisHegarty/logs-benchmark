package systems

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/logs-benchmark/internal/source/synthetic"
)

func TestSplitSystemType(t *testing.T) {
	base, opts := splitSystemType("elasticsearch;probe=seqno;sort=app+host.name+@timestamp")
	if base != "elasticsearch" || !reflect.DeepEqual(opts, []string{"probe=seqno", "sort=app+host.name+@timestamp"}) {
		t.Fatalf("got %q %q", base, opts)
	}
	base, opts = splitSystemType("victorialogs")
	if base != "victorialogs" || len(opts) != 0 {
		t.Fatalf("got %q %q", base, opts)
	}
}

func TestOtherSystemsRejectOptions(t *testing.T) {
	if _, err := NewWriteClient("opensearch;probe=seqno", "http://x"); err == nil {
		t.Fatal("opensearch must reject options")
	}
	if _, err := NewQueryClient("loki;x=y", "http://x"); err == nil {
		t.Fatal("loki must reject options")
	}
}

func TestPlainElasticsearchIsUnchanged(t *testing.T) {
	c, err := NewWriteClient("elasticsearch", "http://x")
	if err != nil {
		t.Fatal(err)
	}
	es := c.(*elasticsearch)
	if es.opts.target != esTarget || es.opts.probe != esProbeCount || es.opts.needsTemplate() || es.opts.license != "" {
		t.Fatalf("plain elasticsearch must keep the published defaults, got %+v", es.opts)
	}
}

// probe=none must report no count and must not contact the cluster at all, so it
// cannot keep a shard out of search-idle.
func TestProbeNoneSendsNothing(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("probe=none sent %s %s", r.Method, r.URL)
	}))
	defer srv.Close()
	c, err := NewQueryClient("elasticsearch;probe=none", srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	n, err := c.StoredRows(context.Background())
	if err != nil || n != nil {
		t.Fatalf("probe=none must return (nil, nil), got (%v, %v)", n, err)
	}
	for _, probe := range []string{"count", "seqno"} {
		c, err := NewQueryClient("elasticsearch;probe="+probe, "http://127.0.0.1:1")
		if err != nil {
			t.Fatal(err)
		}
		if n, err := c.StoredRows(context.Background()); err == nil {
			t.Fatalf("probe=%s must try the cluster and fail on an unroutable address, got %v", probe, n)
		}
	}
}

func TestParseESOptionsErrors(t *testing.T) {
	for _, bad := range []string{"probe=nope", "license=gold", "message=binary", "bogus=1", "noval", "set.x="} {
		if _, err := parseESOptions([]string{bad}); err == nil {
			t.Errorf("option %q must fail", bad)
		}
	}
}

func TestParseESOptionsRejectsDuplicates(t *testing.T) {
	if _, err := parseESOptions([]string{"message=text", "message=keyword"}); err == nil {
		t.Fatal("a repeated key must fail")
	}
}

func TestTemplateJSON(t *testing.T) {
	o, err := parseESOptions([]string{
		"target=logs-bench-sort",
		"sort=app+host.name+@timestamp",
		"message=text",
		"noindex=request_id+ip+duration_ms",
		"set.index.refresh_interval=30s",
		"set.index.number_of_shards=2",
	})
	if err != nil {
		t.Fatal(err)
	}
	b, err := o.templateJSON()
	if err != nil {
		t.Fatal(err)
	}
	var tpl struct {
		IndexPatterns []string `json:"index_patterns"`
		Priority      int      `json:"priority"`
		ComposedOf    []string `json:"composed_of"`
		Template      struct {
			Settings map[string]any `json:"settings"`
			Mappings struct {
				Properties map[string]map[string]any `json:"properties"`
			} `json:"mappings"`
		} `json:"template"`
	}
	if err := json.Unmarshal(b, &tpl); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(tpl.IndexPatterns, []string{"logs-bench-sort"}) || tpl.Priority <= 100 {
		t.Fatalf("bad pattern/priority: %s", b)
	}
	if !reflect.DeepEqual(tpl.ComposedOf, []string{"logs@mappings", "logs@settings", "logs@custom", "ecs@mappings"}) {
		t.Fatalf("must compose the built-in logs components: %s", b)
	}
	if !reflect.DeepEqual(tpl.Template.Settings["index.sort.field"], []any{"app", "host.name", "@timestamp"}) ||
		!reflect.DeepEqual(tpl.Template.Settings["index.sort.order"], []any{"asc", "asc", "desc"}) ||
		tpl.Template.Settings["index.refresh_interval"] != "30s" ||
		tpl.Template.Settings["index.number_of_shards"] != "2" {
		t.Fatalf("bad settings: %s", b)
	}
	props := tpl.Template.Mappings.Properties
	if props["app"]["type"] != "keyword" || props["message"]["type"] != "text" ||
		props["request_id"]["index"] != false || props["ip"]["type"] != "ip" || props["duration_ms"]["type"] != "long" {
		t.Fatalf("bad mappings: %s", b)
	}
	host, ok := props["host"]["properties"].(map[string]any)
	if !ok || host["name"] == nil {
		t.Fatalf("host.name must be mapped for the index sort: %s", b)
	}
}

func TestNoIndexRejectsDottedNames(t *testing.T) {
	o, _ := parseESOptions([]string{"noindex=host.name"})
	if _, err := o.templateJSON(); err == nil || !strings.Contains(err.Error(), "dotted") {
		t.Fatalf("want dotted-name error, got %v", err)
	}
}

func TestParseSeqNoTotal(t *testing.T) {
	// Real shape from Elasticsearch 9.5.5 on the baseline run: two backing
	// indices of a data stream, one primary each, plus an unassigned-style replica
	// entry that must not be counted.
	body := []byte(`{"indices":{
	  ".ds-a":{"shards":{"0":[{"routing":{"primary":true},"seq_no":{"max_seq_no":213307999}}]}},
	  ".ds-b":{"shards":{"0":[{"routing":{"primary":true},"seq_no":{"max_seq_no":49637807}},
	                          {"routing":{"primary":false},"seq_no":{"max_seq_no":49637807}}]}}}}`)
	n, err := parseSeqNoTotal(body)
	if err != nil {
		t.Fatal(err)
	}
	if want := int64(213308000 + 49637808); n != want {
		t.Fatalf("got %d, want %d", n, want)
	}
	if n, err := parseSeqNoTotal([]byte(`{"indices":{".ds-c":{"shards":{"0":[{"routing":{"primary":true},"seq_no":{"max_seq_no":-1}}]}}}}`)); err != nil || n != 0 {
		t.Fatalf("empty shard: got %d, %v", n, err)
	}
}

// Captured from a 9.6.0-SNAPSHOT node: GET /<stream>/_stats/docs?level=shards with
// the filter_path of esStoredRowsSeqNo, after 3000 documents were bulk-indexed.
func TestParseSeqNoTotalRealPayload(t *testing.T) {
	body := []byte(`{"indices":{".ds-logs-live-col-2026.11.16-000001":{"shards":{"0":[{"routing":{"primary":true},"seq_no":{"max_seq_no":2999}}]}}}}`)
	if n, err := parseSeqNoTotal(body); err != nil || n != 3000 {
		t.Fatalf("got %d, %v, want 3000", n, err)
	}
}

func TestMessageKeywordAndNoIndexAll(t *testing.T) {
	o, err := parseESOptions([]string{"message=keyword", "noindex=all", "license=trial"})
	if err != nil {
		t.Fatal(err)
	}
	b, err := o.templateJSON()
	if err != nil {
		t.Fatal(err)
	}
	var tpl struct {
		Template struct {
			Mappings struct {
				Properties map[string]map[string]any `json:"properties"`
			} `json:"mappings"`
		} `json:"template"`
	}
	if err := json.Unmarshal(b, &tpl); err != nil {
		t.Fatal(err)
	}
	props := tpl.Template.Mappings.Properties
	if props["message"]["type"] != "keyword" || props["message"]["index"] != nil {
		t.Fatalf("message must be a plain keyword: %s", b)
	}
	if props["request_id"]["type"] != "keyword" || props["request_id"]["index"] != false ||
		props["ip"]["type"] != "ip" || props["status"]["type"] != "long" || props["status"]["index"] != false ||
		props["namespace"]["index"] != false || props["zone"]["index"] != false {
		t.Fatalf("pool fields must be doc-values only with their types: %s", b)
	}
	for _, f := range []string{"app", "level", "host"} {
		if _, ok := props[f]; ok {
			t.Fatalf("%s must stay indexed (query and sort field): %s", f, b)
		}
	}
	if _, err := parseESOptions([]string{"message=binary"}); err == nil {
		t.Fatal("want an error for message=binary")
	}
}

// fakeES serves a license that starts as basic. raceTrial makes the POST lose a
// race: another caller starts the trial first, so the POST gets a 403.
func fakeES(t *testing.T, raceTrial bool) (*httptest.Server, *[]string) {
	t.Helper()
	var mu sync.Mutex
	var calls []string
	license := "basic"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		calls = append(calls, r.Method+" "+r.URL.Path)
		switch {
		case r.URL.Path == "/_license" && r.Method == "GET":
			fmt.Fprintf(w, `{"license":{"type":%q}}`, license)
		case r.URL.Path == "/_license/start_trial":
			if raceTrial {
				license = "trial"
				w.WriteHeader(http.StatusForbidden)
				fmt.Fprint(w, `{"trial_was_started":false}`)
				return
			}
			license = "trial"
			fmt.Fprint(w, `{"trial_was_started":true}`)
		case r.URL.Path == "/_index_template/logs" && r.Method == "GET":
			// Shape seen on 9.7.0-SNAPSHOT with the filter_path of esBuiltinLogsTemplate.
			fmt.Fprint(w, `{"index_templates":[{"index_template":{"composed_of":["logs@mappings","logs@settings","logs@custom","ecs@mappings"],"ignore_missing_component_templates":["logs@custom"]}}]}`)
		case strings.HasPrefix(r.URL.Path, "/_index_template/") && r.Method == "PUT":
			fmt.Fprint(w, `{"acknowledged":true}`)
		case strings.HasSuffix(r.URL.Path, "/_mapping"):
			fmt.Fprint(w, `{".ds-x-000001":{"mappings":{"properties":{"message":{"type":"keyword"},"request_id":{"type":"keyword","index":false},"host":{"properties":{"name":{"type":"keyword","ignore_above":1024}}}}}}}`)
		case r.URL.Path == "/":
			fmt.Fprint(w, `{"version":{"number":"9.5.5","build_hash":"deadbeefcafe","build_snapshot":false}}`)
		case strings.HasSuffix(r.URL.Path, "/_settings"):
			fmt.Fprint(w, `{".ds-x-000001":{"settings":{"index.mode":"logsdb","index.refresh_interval":"30s","index.default_pipeline":"_none"}}}`)
		default:
			fmt.Fprint(w, `{}`)
		}
	}))
	t.Cleanup(srv.Close)
	return srv, &calls
}

func TestEnsureTrialStartsTrial(t *testing.T) {
	srv, _ := fakeES(t, false)
	if err := esEnsureTrial(srv.URL); err != nil {
		t.Fatal(err)
	}
}

// The writer and the querier both call WaitReady: losing the race for the one
// allowed trial must not fail, because the license is a trial afterwards.
func TestEnsureTrialToleratesLostRace(t *testing.T) {
	srv, _ := fakeES(t, true)
	if err := esEnsureTrial(srv.URL); err != nil {
		t.Fatalf("lost race must succeed: %v", err)
	}
}

func TestEnsureTrialFailsWhenStillNotTrial(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/_license/start_trial" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		fmt.Fprint(w, `{"license":{"type":"basic"}}`)
	}))
	defer srv.Close()
	if err := esEnsureTrial(srv.URL); err == nil {
		t.Fatal("want an error when the license stays basic")
	}
}

func TestApplyOnlyTouchesWhatTheOptionsNeed(t *testing.T) {
	srv, calls := fakeES(t, false)
	o, _ := parseESOptions([]string{"probe=seqno"})
	if err := o.apply(srv.URL); err != nil {
		t.Fatal(err)
	}
	if len(*calls) != 0 {
		t.Fatalf("probe alone must not contact the cluster, got %v", *calls)
	}
	o, _ = parseESOptions([]string{"license=trial", "set.index.refresh_interval=30s"})
	if err := o.apply(srv.URL); err != nil {
		t.Fatal(err)
	}
	var sawTemplate bool
	for _, c := range *calls {
		if strings.HasPrefix(c, "PUT /_index_template/logs-bench-variant-") {
			sawTemplate = true
		}
	}
	if !sawTemplate {
		t.Fatalf("want a template PUT, got %v", *calls)
	}
}

func TestNoIndexUnknownFieldIsAnError(t *testing.T) {
	o, _ := parseESOptions([]string{"noindex=reqest_id"})
	if _, err := o.templateJSON(); err == nil {
		t.Fatal("a misspelled field must fail, not silently stay indexed")
	}
}

func TestNoIndexExplicitMatchesAll(t *testing.T) {
	for _, f := range []string{"status", "ip", "bytes_sent", "request_id", "zone"} {
		got, err := noIndexMapping(f)
		if err != nil {
			t.Fatal(err)
		}
		var want map[string]any
		for _, pf := range synthetic.AllPoolFields() {
			if pf.Name == f {
				want = noIndexPoolMapping(pf)
			}
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s: explicit %v, all %v", f, got, want)
		}
	}
}

// noindex=all must cover every field the Elasticsearch encoder emits. A field
// that is emitted but not mapped goes through dynamic mapping and stays
// indexed, which would silently weaken the arm.
func TestNoIndexAllCoversEveryEmittedField(t *testing.T) {
	o, err := parseESOptions([]string{"noindex=all"})
	if err != nil {
		t.Fatal(err)
	}
	b, err := o.templateJSON()
	if err != nil {
		t.Fatal(err)
	}
	var tpl struct {
		Template struct {
			Mappings struct {
				Properties map[string]any `json:"properties"`
			} `json:"mappings"`
		} `json:"template"`
	}
	if err := json.Unmarshal(b, &tpl); err != nil {
		t.Fatal(err)
	}
	exempt := map[string]bool{"@timestamp": true, "host": true, "app": true, "level": true, "message": true}
	fleet := synthetic.NewFleet(1, 125, 40)
	gen := synthetic.New(fleet, 1)
	seen := map[string]bool{}
	var buf []byte
	ts := time.Unix(1_700_000_000, 0).UTC()
	for i := 0; i < 50000; i++ {
		buf = appendECS(buf[:0], gen.Next(), ts)
		var doc map[string]any
		if err := json.Unmarshal(buf, &doc); err != nil {
			t.Fatal(err)
		}
		for k := range doc {
			seen[k] = true
		}
	}
	for k := range seen {
		if exempt[k] {
			continue
		}
		if _, ok := tpl.Template.Mappings.Properties[k]; !ok {
			t.Errorf("emitted field %q is not covered by noindex=all", k)
		}
	}
}

func TestCheckIndexSettings(t *testing.T) {
	body := []byte(`{".ds-a":{"settings":{"index.mode":"logsdb","index.refresh_interval":"30s","index.number_of_shards":"2"}}}`)
	ok := map[string]string{"index.mode": "logsdb", "index.refresh_interval": "30s", "index.number_of_shards": "2"}
	if err := checkIndexSettings(body, ok); err != nil {
		t.Fatal(err)
	}
	// The lost-mode case: the variant template replaced the built-in one.
	err := checkIndexSettings(body, map[string]string{"index.mode": "logsdb_columnar"})
	if err == nil || !strings.Contains(err.Error(), "index.mode is logsdb, want") {
		t.Fatalf("want a mode mismatch, got %v", err)
	}
	if err := checkIndexSettings(body, map[string]string{"index.default_pipeline": "_none"}); err == nil {
		t.Fatal("a setting missing from the index must fail")
	}
	if err := checkIndexSettings([]byte(`{}`), ok); err == nil {
		t.Fatal("no backing index must fail")
	}
}

func TestWantIndexSettings(t *testing.T) {
	o, err := parseESOptions([]string{"mode=logsdb_columnar", "set.index.refresh_interval=30s", "set.default_pipeline=_none"})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"index.mode": "logsdb_columnar", "index.refresh_interval": "30s", "index.default_pipeline": "_none"}
	if got := o.wantIndexSettings(); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	if m, _ := parseESOptions([]string{"mode=logsdb"}); m.needsTemplate() || !m.needsVerify() {
		t.Fatal("mode alone must verify but not install a template")
	}
}

func TestApplyFailsWhenModeIsLost(t *testing.T) {
	srv, _ := fakeES(t, false) // the fake backing index reports index.mode=logsdb
	o, _ := parseESOptions([]string{"mode=logsdb_columnar", "set.index.refresh_interval=30s"})
	err := o.apply(srv.URL)
	if err == nil || !strings.Contains(err.Error(), "index.mode") {
		t.Fatalf("want a mode mismatch, got %v", err)
	}
	o, _ = parseESOptions([]string{"mode=logsdb", "set.index.refresh_interval=30s"})
	if err := o.apply(srv.URL); err != nil {
		t.Fatal(err)
	}
}

func TestCheckBuild(t *testing.T) {
	release := []byte(`{"version":{"number":"9.5.5","build_hash":"deadbeefcafe","build_snapshot":false}}`)
	// A snapshot reports number "9.7.0-SNAPSHOT" and a full 40-digit hash (seen on 9.6.0-SNAPSHOT).
	snap := []byte(`{"version":{"number":"9.7.0-SNAPSHOT","build_hash":"a0c0b5a0123456789abcdef0123456789abcdef0","build_snapshot":true}}`)
	for _, c := range []struct {
		name           string
		body           []byte
		version, build string
		ok             bool
	}{
		{"release ok", release, "9.5.5", "release", true},
		{"snapshot ok", snap, "9.7.0-SNAPSHOT", "a0c0b5a0", true},
		{"snapshot number without the suffix", snap, "9.7.0", "a0c0b5a0", false},
		{"nothing asserted", snap, "", "", true},
		{"snapshot on a release arm", snap, "9.5.5", "release", false},
		{"release on a snapshot arm", release, "9.7.0-SNAPSHOT", "a0c0b5a0", false},
		{"wrong snapshot", snap, "9.7.0-SNAPSHOT", "1234567", false},
		{"wrong version only", release, "9.6.0", "release", false},
	} {
		if err := checkBuild(c.body, c.version, c.build); (err == nil) != c.ok {
			t.Errorf("%s: err = %v, want ok=%t", c.name, err, c.ok)
		}
	}
}

func TestBuildOptionParsing(t *testing.T) {
	for _, good := range []string{"build=release", "build=a0c0b5a0", "version=9.5.5"} {
		if _, err := parseESOptions([]string{good}); err != nil {
			t.Errorf("%s: %v", good, err)
		}
	}
	for _, bad := range []string{"build=snapshot", "build=xyz", "build=a0c"} {
		if _, err := parseESOptions([]string{bad}); err == nil {
			t.Errorf("%s must fail", bad)
		}
	}
}

func TestApplyFailsOnWrongBuild(t *testing.T) {
	srv, calls := fakeES(t, false) // a release 9.5.5
	o, _ := parseESOptions([]string{"version=9.5.5", "build=release"})
	if err := o.apply(srv.URL); err != nil {
		t.Fatal(err)
	}
	for _, c := range *calls {
		if c != "GET /" {
			t.Fatalf("identity checks must only read, got %v", *calls)
		}
	}
	o, _ = parseESOptions([]string{"version=9.7.0-SNAPSHOT", "build=a0c0b5a0", "license=trial"})
	*calls = nil
	if err := o.apply(srv.URL); err == nil {
		t.Fatal("a release node on a snapshot arm must fail")
	}
	if len(*calls) != 1 {
		t.Fatalf("the identity check must run before anything is changed, got %v", *calls)
	}
}

func TestCheckMappings(t *testing.T) {
	want := map[string]any{
		"message":    map[string]any{"type": "keyword"},
		"request_id": map[string]any{"type": "keyword", "index": false},
		"host":       map[string]any{"properties": map[string]any{"name": map[string]any{"type": "keyword", "ignore_above": 1024}}},
	}
	// Plain logsdb: nested host, index:false echoed (shape seen on 9.6.0-SNAPSHOT).
	nested := []byte(`{".ds-a":{"mappings":{"properties":{
	  "message":{"type":"keyword"},
	  "request_id":{"type":"keyword","index":false},
	  "host":{"properties":{"name":{"type":"keyword","ignore_above":1024,"extra":1}}}}}}}`)
	if err := checkMappings(nested, want, false); err != nil {
		t.Fatal(err)
	}
	// Columnar: flat "host.name" key, index:false not echoed (seen on 9.6.0-SNAPSHOT).
	flat := []byte(`{".ds-a":{"mappings":{"properties":{
	  "message":{"type":"keyword"},
	  "request_id":{"type":"keyword"},
	  "host.name":{"type":"keyword","ignore_above":1024}}}}}`)
	if err := checkMappings(flat, want, true); err != nil {
		t.Fatalf("columnar shape: %v", err)
	}
	// Outside columnar a missing index:false means the field is still indexed.
	if err := checkMappings(flat, want, false); err == nil || !strings.Contains(err.Error(), "request_id.index is not set") {
		t.Fatalf("a missing index:false must fail outside columnar, got %v", err)
	}
	// message lost its keyword mapping (the built-in match_only_text is back)
	bad := []byte(`{".ds-a":{"mappings":{"properties":{"message":{"type":"match_only_text"},"request_id":{"type":"keyword","index":true}}}}}`)
	err := checkMappings(bad, want, false)
	if err == nil {
		t.Fatal("want mismatches")
	}
	for _, s := range []string{"message.type is match_only_text", "request_id.index is true", "host.name is not mapped"} {
		if !strings.Contains(err.Error(), s) {
			t.Errorf("error %q must mention %q", err, s)
		}
	}
	if err := checkMappings([]byte(`{}`), want, false); err == nil {
		t.Fatal("no backing index must fail")
	}
}

func TestApplyFailsWhenMappingIsLost(t *testing.T) {
	srv, _ := fakeES(t, false) // the fake index maps message as keyword and request_id as index:false
	o, _ := parseESOptions([]string{"message=keyword", "noindex=request_id", "set.index.refresh_interval=30s"})
	if err := o.apply(srv.URL); err != nil {
		t.Fatal(err)
	}
	o, _ = parseESOptions([]string{"message=text", "set.index.refresh_interval=30s"})
	if err := o.apply(srv.URL); err == nil || !strings.Contains(err.Error(), "message.type") {
		t.Fatalf("want a message mapping mismatch, got %v", err)
	}
}

func TestModeAndSetModeConflict(t *testing.T) {
	for _, set := range []string{"set.index.mode=logsdb", "set.mode=logsdb"} {
		if _, err := parseESOptions([]string{"mode=logsdb", set}); err == nil {
			t.Errorf("mode= with %s must fail", set)
		}
	}
	if _, err := parseESOptions([]string{"set.index.mode=logsdb"}); err != nil {
		t.Errorf("set.index.mode alone is fine: %v", err)
	}
}

func TestEnsureDataStream(t *testing.T) {
	status, body := 200, `{"acknowledged":true}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(status)
		fmt.Fprint(w, body)
	}))
	defer srv.Close()
	if err := esEnsureDataStream(srv.URL, "t"); err != nil {
		t.Fatal(err)
	}
	status, body = 400, `{"error":{"type":"resource_already_exists_exception"}}`
	if err := esEnsureDataStream(srv.URL, "t"); err != nil {
		t.Fatalf("already exists must succeed: %v", err)
	}
	status, body = 403, `{"error":{"type":"security_exception"}}`
	if err := esEnsureDataStream(srv.URL, "t"); err == nil {
		t.Fatal("a 403 must not be swallowed")
	}
	status, body = 400, `{"error":{"type":"illegal_argument_exception","reason":"no matching index template found"}}`
	if err := esEnsureDataStream(srv.URL, "t"); err == nil {
		t.Fatal("a template that does not resolve must fail")
	}
}

// The components come from the cluster's built-in logs template, not from a
// list in the harness, so the arm is built on what the baseline runs on.
func TestParseBuiltinLogsTemplate(t *testing.T) {
	// Shape seen on 9.7.0-SNAPSHOT.
	body := []byte(`{"index_templates":[{"index_template":{"composed_of":["logs@mappings","logs@settings","logs@custom","ecs@mappings"],"ignore_missing_component_templates":["logs@custom"]}}]}`)
	c, i, err := parseBuiltinLogsTemplate(body)
	if err != nil || !reflect.DeepEqual(c, esBuiltinComposedOf) || !reflect.DeepEqual(i, esBuiltinIgnoreMissing) {
		t.Fatalf("got %v %v %v", c, i, err)
	}
	for _, bad := range []string{`{}`, `{"index_templates":[]}`, `{"index_templates":[{"index_template":{}}]}`, `nope`} {
		if _, _, err := parseBuiltinLogsTemplate([]byte(bad)); err == nil {
			t.Errorf("%s must fail", bad)
		}
	}
}

func TestTemplateUsesTheClustersComponents(t *testing.T) {
	o, _ := parseESOptions([]string{"set.index.refresh_interval=30s"})
	b, err := o.templateJSONWith([]string{"logs@mappings", "x@other"}, []string{"x@other"})
	if err != nil {
		t.Fatal(err)
	}
	var tpl struct {
		ComposedOf    []string `json:"composed_of"`
		IgnoreMissing []string `json:"ignore_missing_component_templates"`
	}
	if err := json.Unmarshal(b, &tpl); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(tpl.ComposedOf, []string{"logs@mappings", "x@other"}) || !reflect.DeepEqual(tpl.IgnoreMissing, []string{"x@other"}) {
		t.Fatalf("got %s", b)
	}
}

func TestApplyBuildsTheTemplateOnTheBuiltinComponents(t *testing.T) {
	var gotTemplate string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/_index_template/logs" && r.Method == "GET":
			fmt.Fprint(w, `{"index_templates":[{"index_template":{"composed_of":["logs@mappings","brand-new@component"]}}]}`)
		case strings.HasPrefix(r.URL.Path, "/_index_template/") && r.Method == "PUT":
			b, _ := io.ReadAll(r.Body)
			gotTemplate = string(b)
			fmt.Fprint(w, `{"acknowledged":true}`)
		default:
			fmt.Fprint(w, `{}`)
		}
	}))
	defer srv.Close()
	o, _ := parseESOptions([]string{"message=keyword"}) // template only: no data stream, no verification
	if err := o.apply(srv.URL); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(gotTemplate, `"composed_of":["logs@mappings","brand-new@component"]`) {
		t.Fatalf("the template must be built on the cluster's components, got %s", gotTemplate)
	}
}

func TestApplyFailsWithoutTheBuiltinTemplate(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/_index_template/logs" {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, `{"error":{"type":"resource_not_found_exception"}}`)
			return
		}
		t.Errorf("nothing may be installed without the built-in template, got %s %s", r.Method, r.URL)
	}))
	defer srv.Close()
	o, _ := parseESOptions([]string{"message=keyword"})
	if err := o.apply(srv.URL); err == nil || !strings.Contains(err.Error(), "built-in logs template") {
		t.Fatalf("want an error about the built-in template, got %v", err)
	}
}

func TestSortIsVerifiedOnTheBackingIndex(t *testing.T) {
	o, err := parseESOptions([]string{"sort=app+host.name+@timestamp"})
	if err != nil {
		t.Fatal(err)
	}
	if !o.needsVerify() {
		t.Fatal("sort alone must be verified")
	}
	want := o.wantIndexSettings()
	if want["index.sort.field"] != "app,host.name,@timestamp" || want["index.sort.order"] != "asc,asc,desc" {
		t.Fatalf("got %v", want)
	}
	// Shape seen on 9.7.0-SNAPSHOT with flat_settings: lists, not strings.
	ok := []byte(`{".ds-a":{"settings":{"index.sort.field":["app","host.name","@timestamp"],"index.sort.order":["asc","asc","desc"]}}}`)
	if err := checkIndexSettings(ok, want); err != nil {
		t.Fatal(err)
	}
	lost := []byte(`{".ds-a":{"settings":{"index.sort.field":["host.name","@timestamp"],"index.sort.order":["asc","desc"]}}}`)
	if err := checkIndexSettings(lost, want); err == nil || !strings.Contains(err.Error(), "index.sort.field is host.name,@timestamp") {
		t.Fatalf("want a sort mismatch, got %v", err)
	}
}
