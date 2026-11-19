package systems

// Variant options for the elasticsearch adapter.
//
// The published benchmark runs Elasticsearch at its defaults (the built-in
// logs-*-* data stream template). To measure how much each default costs, a
// system type can carry options after semicolons:
//
//	elasticsearch;probe=seqno;set.index.refresh_interval=30s;sort=app+host.name+@timestamp
//
// An option-less "elasticsearch" type behaves exactly as before. Option values
// must not contain commas (the type is passed through a comma-separated flag).
//
// Supported options:
//
//	target=<name>          data stream name (default logs-bench-default)
//	probe=count|seqno|none how StoredRows counts documents (default count).
//	                       "count" uses _count, which is a search and keeps the
//	                       shard out of search-idle, so it refreshes every
//	                       index.refresh_interval for the whole run. "seqno"
//	                       sums max_seq_no+1 of the primaries from _stats, which
//	                       does not touch a searcher. "none" sends nothing to
//	                       the cluster: StoredRows reports no count, exactly
//	                       like loki.go, so the querier's stored-rows loop stops
//	                       and the dashboard and reports fall back to the
//	                       lines the writer got a 2xx for
//	                       (logbench_lines_sent_total). A bulk is acknowledged
//	                       only after the translog is fsynced and every item
//	                       succeeded (esBulkSend), so that is a durable count.
//	                       Check it once after the run by comparing a _count
//	                       of the data stream with logbench_lines_sent_total.
//	license=trial          start the 30-day trial before the first write, so
//	                       LogsDB can use synthetic _source (the default Basic
//	                       license falls back to stored _source)
//	sort=<f>+<f>+...       index sort fields, e.g. app+host.name+@timestamp;
//	                       @timestamp sorts desc, every other field asc
//	message=text           map message as text (positions) instead of the
//	                       built-in match_only_text
//	message=keyword        map message as keyword (one term per distinct line;
//	                       no word search)
//	noindex=<f>+<f>+...    map these fields with index:false (doc values only).
//	                       Names must be fields of the workload pool (an unknown
//	                       name is an error); numeric pool fields are mapped
//	                       long, ip as ip, the others as keyword. "noindex=all"
//	                       means every field of the workload's pool (host and
//	                       app fields) except message, app, level and host, with
//	                       the same mappings
//	version=<number>       assert the Elasticsearch version of the node, for
//	                       example 9.5.5. A snapshot reports 9.7.0-SNAPSHOT, not
//	                       9.7.0. Read-only.
//	build=release|<hash>   assert the build: "release" means a published
//	                       release (not a snapshot); a hex prefix means a
//	                       snapshot whose build hash starts with it. Read-only.
//	                       Use both on every arm, so that an image set for one
//	                       arm (ES_IMAGE in the environment) cannot end up on
//	                       another without the run stopping.
//	mode=<index mode>      assert the index mode of the backing index, for example
//	                       logsdb or logsdb_columnar. Without it only the set.*
//	                       settings are asserted. See "Verification" below.
//	set.<setting>=<value>  any index setting, e.g. set.index.refresh_interval=30s
//	                       or set.index.number_of_shards=2
//
// Order of work in WaitReady, after the cluster is healthy:
//
//  1. version= and build=, if given: read GET / and fail on a different node.
//  2. license=trial, if given.
//  3. When any of sort, message, noindex or set.* is given, install a
//     composable template for the target (priority 200) on the components of
//     the cluster's own built-in logs template, so the arm differs from the
//     published setup only by its options. Like opensearch.go does, before the
//     first write.
//  4. When mode, sort or set.* is given, create the data stream explicitly and
//     read the settings of its backing index back: mode, the index sort and
//     every set.* value must match.
//  5. When a template was installed, read the mapping back: every explicit
//     mapping (message, noindex, sort fields) must be present.
//
// A variant template replaces the built-in logs template, so what the backing
// index really got can differ from what the options say. Any mismatch stops the
// run before the first write instead of measuring the wrong configuration. Steps
// 4 and 5 check the backing index that exists at startup; a later rollover
// creates its index from the same template, so it is not checked again.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/VictoriaMetrics/logs-benchmark/internal/source/synthetic"
)

const (
	esMsgText    = "text"
	esMsgKeyword = "keyword"
)

const (
	esProbeCount = "count"
	esProbeSeqNo = "seqno"
	esProbeNone  = "none"
)

var esBuildHash = regexp.MustCompile(`^[0-9a-f]{7,40}$`)

type esOptions struct {
	target   string
	probe    string
	license  string
	sort     []string
	version  string // expected node version, "" = not asserted
	build    string // "release" or a build hash prefix, "" = not asserted
	mode     string // expected index.mode of the backing index, "" = not asserted
	message  string // "", esMsgText or esMsgKeyword
	noIndex  []string
	indexed  []string
	settings map[string]string
}

func parseESOptions(raw []string) (esOptions, error) {
	o := esOptions{target: esTarget, probe: esProbeCount, settings: map[string]string{}}
	seen := map[string]bool{}
	for _, kv := range raw {
		k, v, ok := strings.Cut(kv, "=")
		if !ok || k == "" || v == "" {
			return o, fmt.Errorf("elasticsearch option %q: want key=value", kv)
		}
		// A repeated key would silently let the last one win.
		if seen[k] {
			return o, fmt.Errorf("elasticsearch option %q given twice", k)
		}
		seen[k] = true
		switch {
		case k == "target":
			o.target = v
		case k == "probe":
			if v != esProbeCount && v != esProbeSeqNo && v != esProbeNone {
				return o, fmt.Errorf("elasticsearch option probe=%q: want %s, %s or %s", v, esProbeCount, esProbeSeqNo, esProbeNone)
			}
			o.probe = v
		case k == "license":
			if v != "trial" {
				return o, fmt.Errorf("elasticsearch option license=%q: only trial is supported", v)
			}
			o.license = v
		case k == "version":
			o.version = v
		case k == "build":
			if v != "release" && !esBuildHash.MatchString(v) {
				return o, fmt.Errorf("elasticsearch option build=%q: want release or a hex build hash prefix of 7 to 40 digits", v)
			}
			o.build = v
		case k == "mode":
			o.mode = v
		case k == "sort":
			o.sort = strings.Split(v, "+")
		case k == "message":
			if v != esMsgText && v != esMsgKeyword {
				return o, fmt.Errorf("elasticsearch option message=%q: want %s or %s", v, esMsgText, esMsgKeyword)
			}
			o.message = v
		case k == "noindex":
			o.noIndex = strings.Split(v, "+")
		case k == "index":
			o.indexed = strings.Split(v, "+")
		case strings.HasPrefix(k, "set."):
			o.settings[strings.TrimPrefix(k, "set.")] = v
		default:
			return o, fmt.Errorf("unknown elasticsearch option %q", k)
		}
	}
	// mode= is an assertion and set.index.mode a setting: both on the same key
	// would let one silently win.
	for _, k := range []string{"index.mode", "mode"} {
		if _, ok := o.settings[k]; ok && o.mode != "" {
			return o, fmt.Errorf("elasticsearch options mode= and set.%s both name the index mode: use one", k)
		}
	}
	// A field in both index= and noindex= would let one silently win.
	for _, f := range o.indexed {
		for _, nf := range o.noIndex {
			if f == nf {
				return o, fmt.Errorf("field %q is in both index= and noindex=", f)
			}
		}
	}
	return o, nil
}

// sortOrder is the order of each sort field: @timestamp descending, every other
// field ascending.
func sortOrder(fields []string) []string {
	order := make([]string, len(fields))
	for i, f := range fields {
		order[i] = "asc"
		if f == "@timestamp" {
			order[i] = "desc"
		}
	}
	return order
}

// needsTemplate reports whether the options change the index template.
func (o esOptions) needsTemplate() bool {
	return len(o.sort) > 0 || o.message != "" || len(o.noIndex) > 0 || len(o.indexed) > 0 || len(o.settings) > 0
}

// needsIdentity reports whether the node's version or build is checked.
func (o esOptions) needsIdentity() bool {
	return o.version != "" || o.build != ""
}

// needsVerify reports whether the backing index settings are checked.
func (o esOptions) needsVerify() bool {
	return o.mode != "" || len(o.settings) > 0 || len(o.sort) > 0
}

// apply prepares the cluster before the first write, in the order described in
// the file comment: identity, license (first of the changes, since LogsDB picks
// stored or synthetic _source when the index is created), template, data stream,
// settings, mappings. Every step is idempotent, because the writer and the
// querier both call WaitReady.
func (o esOptions) apply(addr string) error {
	if o.needsIdentity() {
		if err := o.verifyBuild(addr); err != nil {
			return err
		}
	}
	if o.license == "trial" {
		if err := esEnsureTrial(addr); err != nil {
			return err
		}
	}
	if o.needsTemplate() {
		composedOf, ignoreMissing, err := esBuiltinLogsTemplate(addr)
		if err != nil {
			return err
		}
		body, err := o.templateJSONWith(composedOf, ignoreMissing)
		if err != nil {
			return err
		}
		if _, _, _, err := doReq(context.Background(), "PUT", addr+"/_index_template/logs-bench-variant-"+o.target, "application/json", body); err != nil {
			return fmt.Errorf("set index template: %w", err)
		}
	}
	if o.needsVerify() {
		if err := esEnsureDataStream(addr, o.target); err != nil {
			return err
		}
		if err := o.verifyBackingIndex(addr); err != nil {
			return err
		}
		return o.verifyMappings(addr)
	}
	return nil
}

// verifyBuild reads the node's version and build and compares them with the
// options. It only reads, so it is safe on the published baseline too.
func (o esOptions) verifyBuild(addr string) error {
	body, err := doQuery(context.Background(), "GET", addr+"/?filter_path=version.number,version.build_hash,version.build_snapshot", "", nil)
	if err != nil {
		return fmt.Errorf("read node version: %w", err)
	}
	return checkBuild(body, o.version, o.build)
}

func checkBuild(body []byte, version, build string) error {
	var r struct {
		Version struct {
			Number        string `json:"number"`
			BuildHash     string `json:"build_hash"`
			BuildSnapshot bool   `json:"build_snapshot"`
		} `json:"version"`
	}
	if err := json.Unmarshal(body, &r); err != nil {
		return fmt.Errorf("parse node info: %w: %s", err, truncate(string(body), 200))
	}
	v := r.Version
	var problems []string
	if version != "" && v.Number != version {
		problems = append(problems, fmt.Sprintf("version is %q, want %q", v.Number, version))
	}
	switch build {
	case "":
	case "release":
		if v.BuildSnapshot {
			problems = append(problems, fmt.Sprintf("build is a snapshot (%.8s), want a release", v.BuildHash))
		}
	default:
		if !v.BuildSnapshot || !strings.HasPrefix(v.BuildHash, build) {
			problems = append(problems, fmt.Sprintf("build is %.8s (snapshot=%t), want a snapshot starting with %s", v.BuildHash, v.BuildSnapshot, build))
		}
	}
	if len(problems) > 0 {
		return fmt.Errorf("this node is not the build the arm is meant to run (is ES_IMAGE set in the environment of the wrong install?): %s", strings.Join(problems, "; "))
	}
	return nil
}

// esEnsureDataStream creates the data stream (and so its first backing index)
// before the first write, so the settings can be verified. The writer and the
// querier both call it, so "already exists" is success. Any other failure,
// such as a 403 or a template that does not resolve, is not.
func esEnsureDataStream(addr, target string) error {
	_, _, _, err := doReq(context.Background(), "PUT", addr+"/_data_stream/"+target, "", nil)
	if err != nil && !strings.Contains(err.Error(), "resource_already_exists_exception") {
		return fmt.Errorf("create data stream %s: %w", target, err)
	}
	return nil
}

// wantIndexSettings is what the backing index must report, as flat settings.
func (o esOptions) wantIndexSettings() map[string]string {
	want := map[string]string{}
	if o.mode != "" {
		want["index.mode"] = o.mode
	}
	if len(o.sort) > 0 {
		// The index reports a list; checkIndexSettings joins it with commas.
		want["index.sort.field"] = strings.Join(o.sort, ",")
		want["index.sort.order"] = strings.Join(sortOrder(o.sort), ",")
	}
	for k, v := range o.settings {
		if !strings.HasPrefix(k, "index.") {
			k = "index." + k
		}
		want[k] = v
	}
	return want
}

// verifyBackingIndex reads the settings of the target's backing indices and
// fails if the mode or a set.* value is not what the options asked for.
func (o esOptions) verifyBackingIndex(addr string) error {
	body, err := doQuery(context.Background(), "GET", addr+"/"+o.target+"/_settings?flat_settings=true&filter_path=*.settings", "", nil)
	if err != nil {
		return fmt.Errorf("read backing index settings: %w", err)
	}
	return checkIndexSettings(body, o.wantIndexSettings())
}

// verifyMappings reads the mapping of the backing indices back and fails if a
// field the options map explicitly (message, noindex, sort fields) is not
// mapped that way. A lost or overridden mapping would otherwise leave a field
// indexed and the arm would measure something else, with no error.
func (o esOptions) verifyMappings(addr string) error {
	if !o.needsTemplate() {
		return nil
	}
	props, _, err := o.mappingProps()
	if err != nil {
		return err
	}
	if len(props) == 0 {
		return nil
	}
	body, err := doQuery(context.Background(), "GET", addr+"/"+o.target+"/_mapping?filter_path=*.mappings.properties", "", nil)
	if err != nil {
		return fmt.Errorf("read backing index mappings: %w", err)
	}
	return checkMappings(body, props, strings.Contains(o.mode, "columnar"))
}

// checkMappings checks that every expected mapping is a subset of the actual one.
//
// Elasticsearch reports the same mapping in different shapes: a plain logsdb
// index nests host as {"host":{"properties":{"name":...}}}, a columnar one
// reports the flat key "host.name". Both sides are flattened to dotted paths
// before they are compared. A columnar index does not echo index:false, which
// is its default, so a missing index attribute is accepted there only
// (columnar); in every other mode a missing index:false means the field is
// still indexed.
func checkMappings(body []byte, want map[string]any, columnar bool) error {
	var r map[string]struct {
		Mappings struct {
			Properties map[string]any `json:"properties"`
		} `json:"mappings"`
	}
	if err := json.Unmarshal(body, &r); err != nil {
		return fmt.Errorf("parse _mapping response: %w: %s", err, truncate(string(body), 200))
	}
	if len(r) == 0 {
		return fmt.Errorf("no backing index found to verify mappings")
	}
	wantFlat := map[string]map[string]any{}
	flattenMappings("", want, wantFlat)
	var problems []string
	for idx, v := range r {
		gotFlat := map[string]map[string]any{}
		flattenMappings("", v.Mappings.Properties, gotFlat)
		for path, attrs := range wantFlat {
			got, ok := gotFlat[path]
			if !ok {
				problems = append(problems, fmt.Sprintf("%s.%s is not mapped", idx, path))
				continue
			}
			for k, w := range attrs {
				g, ok := got[k]
				switch {
				case !ok && columnar && k == "index" && w == false:
				case !ok:
					problems = append(problems, fmt.Sprintf("%s.%s.%s is not set, want %v", idx, path, k, w))
				case fmt.Sprint(g) != fmt.Sprint(w):
					problems = append(problems, fmt.Sprintf("%s.%s.%s is %v, want %v", idx, path, k, g, w))
				}
			}
		}
	}
	if len(problems) > 0 {
		sort.Strings(problems)
		if len(problems) > 5 {
			problems = append(problems[:5], fmt.Sprintf("and %d more", len(problems)-5))
		}
		return fmt.Errorf("backing index mappings do not match the options: %s", strings.Join(problems, "; "))
	}
	return nil
}

// flattenMappings turns nested "properties" and dotted keys into one map from
// dotted path to the field's own attributes (type, index, ignore_above, ...).
func flattenMappings(prefix string, props map[string]any, out map[string]map[string]any) {
	for k, v := range props {
		m, ok := v.(map[string]any)
		if !ok {
			continue
		}
		path := k
		if prefix != "" {
			path = prefix + "." + k
		}
		attrs := map[string]any{}
		for ak, av := range m {
			if ak == "properties" {
				if sub, ok := av.(map[string]any); ok {
					flattenMappings(path, sub, out)
				}
				continue
			}
			attrs[ak] = av
		}
		if len(attrs) > 0 {
			out[path] = attrs
		}
	}
}

// settingString renders a flat setting value: a list (index.sort.field) as its
// items joined with commas, anything else as it prints.
func settingString(v any) string {
	list, ok := v.([]any)
	if !ok {
		return fmt.Sprint(v)
	}
	items := make([]string, len(list))
	for i, x := range list {
		items[i] = fmt.Sprint(x)
	}
	return strings.Join(items, ",")
}

// checkIndexSettings compares a flat _settings response with the wanted settings.
func checkIndexSettings(body []byte, want map[string]string) error {
	var r map[string]struct {
		Settings map[string]any `json:"settings"`
	}
	if err := json.Unmarshal(body, &r); err != nil {
		return fmt.Errorf("parse _settings response: %w: %s", err, truncate(string(body), 200))
	}
	if len(r) == 0 {
		return fmt.Errorf("no backing index found to verify")
	}
	var problems []string
	for idx, v := range r {
		for k, w := range want {
			got, ok := v.Settings[k]
			if !ok {
				problems = append(problems, fmt.Sprintf("%s: %s is not set, want %q", idx, k, w))
			} else if settingString(got) != w {
				problems = append(problems, fmt.Sprintf("%s: %s is %s, want %q", idx, k, settingString(got), w))
			}
		}
	}
	if len(problems) > 0 {
		sort.Strings(problems)
		return fmt.Errorf("backing index does not match the options (the variant template replaces the built-in logs template, so a setting the built-in one carried, such as index.mode, can be lost; to set one explicitly use set.<setting>=<value>): %s", strings.Join(problems, "; "))
	}
	return nil
}

func esLicenseType(addr string) (string, error) {
	body, err := doQuery(context.Background(), "GET", addr+"/_license?filter_path=license.type", "", nil)
	if err != nil {
		return "", err
	}
	var r struct {
		License struct {
			Type string `json:"type"`
		} `json:"license"`
	}
	if err := json.Unmarshal(body, &r); err != nil {
		return "", fmt.Errorf("parse _license response: %w: %s", err, truncate(string(body), 200))
	}
	return r.License.Type, nil
}

// esEnsureTrial switches a Basic cluster to the trial license and verifies it.
func esEnsureTrial(addr string) error {
	typ, err := esLicenseType(addr)
	if err != nil {
		return fmt.Errorf("read license: %w", err)
	}
	if typ == "trial" || typ == "enterprise" || typ == "platinum" {
		return nil
	}
	// The writer and the querier both call WaitReady, so another caller may
	// have started the trial between our read and this POST. Elasticsearch
	// then answers 403 (a trial can be started once). That is fine as long as
	// the license is a trial afterwards, so only the re-read decides.
	_, _, _, startErr := doReq(context.Background(), "POST", addr+"/_license/start_trial?acknowledge=true", "", nil)
	typ, err = esLicenseType(addr)
	if err != nil {
		return fmt.Errorf("read license after start_trial: %w", err)
	}
	if typ != "trial" {
		if startErr != nil {
			return fmt.Errorf("start trial: %w", startErr)
		}
		return fmt.Errorf("license is %q after start_trial, want trial", typ)
	}
	return nil
}

// noIndexPoolMapping is the doc-values-only mapping of a workload pool field:
// numeric fields stay long and ip stays ip, so only the index structures
// (terms, points) go away, not the type.
func noIndexPoolMapping(f synthetic.PoolField) map[string]any {
	switch {
	case f.Name == "ip":
		return map[string]any{"type": "ip", "index": false}
	case f.Numeric:
		return map[string]any{"type": "long", "index": false}
	}
	return map[string]any{"type": "keyword", "index": false}
}

// noIndexMapping returns the mapping of a named field that keeps doc values
// only. Names resolve through the workload pool, so an explicit list and
// "noindex=all" give the same mapping for the same field, and a name the
// workload does not emit is an error instead of a silent no-op.
func noIndexMapping(field string) (map[string]any, error) {
	for _, pf := range synthetic.AllPoolFields() {
		if pf.Name == field {
			return noIndexPoolMapping(pf), nil
		}
	}
	return nil, fmt.Errorf("noindex field %q: not a field of the workload pool", field)
}

// indexPoolMapping is the mirror of noIndexPoolMapping: same type, "index" set
// to true instead of false. Used to selectively restore the inverted index on
// specific fields in a mode (such as logsdb_columnar) that otherwise defaults
// keyword and numeric fields to doc-values-only.
func indexPoolMapping(f synthetic.PoolField) map[string]any {
	switch {
	case f.Name == "ip":
		return map[string]any{"type": "ip", "index": true}
	case f.Numeric:
		return map[string]any{"type": "long", "index": true}
	}
	return map[string]any{"type": "keyword", "index": true}
}

// indexMapping returns the mapping of a named field with its inverted index
// restored. Like noIndexMapping, names resolve through the workload pool, so a
// name the workload does not emit is an error instead of a silent no-op.
func indexMapping(field string) (map[string]any, error) {
	for _, pf := range synthetic.AllPoolFields() {
		if pf.Name == field {
			return indexPoolMapping(pf), nil
		}
	}
	return nil, fmt.Errorf("index field %q: not a field of the workload pool", field)
}

// mappingProps is the explicit field mapping the options ask for, and the
// index sort settings that need it. It is what the template carries and what
// verifyMappings reads back from the backing index.
func (o esOptions) mappingProps() (props map[string]any, settings map[string]any, err error) {
	settings = map[string]any{}
	props = map[string]any{}
	if len(o.sort) > 0 {
		settings["index.sort.field"] = o.sort
		settings["index.sort.order"] = sortOrder(o.sort)
		// Sort fields must be mapped when the index is created; dynamic mapping
		// is too late. @timestamp comes from logs@mappings.
		for _, f := range o.sort {
			switch f {
			case "@timestamp":
			case "host.name":
				props["host"] = map[string]any{"properties": map[string]any{
					"name": map[string]any{"type": "keyword", "ignore_above": 1024},
				}}
			default:
				props[f] = map[string]any{"type": "keyword"}
			}
		}
	}
	if o.message != "" {
		props["message"] = map[string]any{"type": o.message}
	}
	noIndex := o.noIndex
	if len(noIndex) == 1 && noIndex[0] == "all" {
		noIndex = nil
		for _, pf := range synthetic.AllPoolFields() {
			props[pf.Name] = noIndexPoolMapping(pf)
		}
	}
	for _, f := range noIndex {
		if strings.Contains(f, ".") {
			return nil, nil, fmt.Errorf("noindex field %q: dotted names are not supported", f)
		}
		m, err := noIndexMapping(f)
		if err != nil {
			return nil, nil, err
		}
		props[f] = m
	}
	for _, f := range o.indexed {
		if strings.Contains(f, ".") {
			return nil, nil, fmt.Errorf("index field %q: dotted names are not supported", f)
		}
		m, err := indexMapping(f)
		if err != nil {
			return nil, nil, err
		}
		props[f] = m
	}
	return props, settings, nil
}

// esBuiltinComponents are the components of the built-in logs template of
// 9.7. The harness reads the real list from the cluster (esBuiltinLogsTemplate)
// so a version that composes something else cannot diverge silently; this
// list is only what templateJSON uses when it is not given one.
var (
	esBuiltinComposedOf    = []string{"logs@mappings", "logs@settings", "logs@custom", "ecs@mappings"}
	esBuiltinIgnoreMissing = []string{"logs@custom"}
)

// esBuiltinLogsTemplate reads which components the cluster's built-in logs-*-*
// template composes. The variant template (priority 200) is built from the same
// list, so an arm differs from the built-in setup only by what its options say.
// It fails if the template is missing: running on a guess would put the arm on a
// different base than the baseline without any sign of it.
func esBuiltinLogsTemplate(addr string) (composedOf, ignoreMissing []string, err error) {
	body, err := doQuery(context.Background(), "GET", addr+"/_index_template/logs?filter_path=index_templates.index_template.composed_of,index_templates.index_template.ignore_missing_component_templates", "", nil)
	if err != nil {
		return nil, nil, fmt.Errorf("read the built-in logs template: %w", err)
	}
	return parseBuiltinLogsTemplate(body)
}

func parseBuiltinLogsTemplate(body []byte) (composedOf, ignoreMissing []string, err error) {
	var r struct {
		IndexTemplates []struct {
			IndexTemplate struct {
				ComposedOf    []string `json:"composed_of"`
				IgnoreMissing []string `json:"ignore_missing_component_templates"`
			} `json:"index_template"`
		} `json:"index_templates"`
	}
	if err := json.Unmarshal(body, &r); err != nil {
		return nil, nil, fmt.Errorf("parse the built-in logs template: %w: %s", err, truncate(string(body), 200))
	}
	if len(r.IndexTemplates) != 1 || len(r.IndexTemplates[0].IndexTemplate.ComposedOf) == 0 {
		return nil, nil, fmt.Errorf("the built-in logs template has no components: %s", truncate(string(body), 200))
	}
	t := r.IndexTemplates[0].IndexTemplate
	return t.ComposedOf, t.IgnoreMissing, nil
}

// templateJSON renders the template with the default components.
func (o esOptions) templateJSON() ([]byte, error) {
	return o.templateJSONWith(esBuiltinComposedOf, esBuiltinIgnoreMissing)
}

// templateJSONWith renders the composable template for the target on top of
// the given components.
func (o esOptions) templateJSONWith(composedOf, ignoreMissing []string) ([]byte, error) {
	props, settings, err := o.mappingProps()
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(o.settings))
	for k := range o.settings {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	// Values stay strings: Elasticsearch accepts "2" for number_of_shards and
	// "30s" for durations, so no per-setting typing is needed.
	for _, k := range keys {
		settings[k] = o.settings[k]
	}
	tpl := map[string]any{
		"index_patterns":                     []string{o.target},
		"priority":                           200,
		"data_stream":                        map[string]any{},
		"composed_of":                        composedOf,
		"ignore_missing_component_templates": ignoreMissing,
		"template": map[string]any{
			"settings": settings,
			"mappings": map[string]any{"properties": props},
		},
	}
	return json.Marshal(tpl)
}

// esStoredRowsSeqNo counts indexed documents without touching a searcher: every
// indexed document of an append-only data stream takes one sequence number, so
// the sum over primaries of max_seq_no+1 is the document count. That holds only
// while nothing but index/create operations reach the stream: deletes, updates
// and no-ops also take sequence numbers. It is true for this workload, so do not
// reuse probe=seqno for a different one. Unlike _count it does not wait for a
// refresh, so it sees a document as soon as the primary has indexed it, which
// is what "stored" means for durable engines.
func esStoredRowsSeqNo(ctx context.Context, addr, target string) (*int64, error) {
	u := addr + "/" + target + "/_stats/docs?level=shards&filter_path=indices.*.shards.*.routing.primary,indices.*.shards.*.seq_no.max_seq_no"
	body, err := doQuery(ctx, "GET", u, "", nil)
	if err != nil {
		return nil, err
	}
	n, err := parseSeqNoTotal(body)
	if err != nil {
		return nil, err
	}
	return &n, nil
}

func parseSeqNoTotal(body []byte) (int64, error) {
	var r struct {
		Indices map[string]struct {
			Shards map[string][]struct {
				Routing struct {
					Primary bool `json:"primary"`
				} `json:"routing"`
				SeqNo struct {
					MaxSeqNo int64 `json:"max_seq_no"`
				} `json:"seq_no"`
			} `json:"shards"`
		} `json:"indices"`
	}
	dec := json.NewDecoder(bytes.NewReader(bytes.TrimSpace(body)))
	if err := dec.Decode(&r); err != nil {
		return 0, fmt.Errorf("parse _stats response: %w: %s", err, truncate(string(body), 200))
	}
	var total int64
	for _, idx := range r.Indices {
		for _, copies := range idx.Shards {
			for _, c := range copies {
				if c.Routing.Primary {
					// max_seq_no is -1 for an empty shard, so +1 is 0
					total += c.SeqNo.MaxSeqNo + 1
				}
			}
		}
	}
	return total, nil
}
