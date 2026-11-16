package systems

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// Every system type in the variant run configs must parse, so a typo in an
// option fails here and not on a cluster.
func TestVariantRunConfigTypesParse(t *testing.T) {
	files, err := runConfigFiles()
	if err != nil || len(files) == 0 {
		t.Fatalf("no run configs found: %v", err)
	}
	re := variantTypeRE
	names := regexp.MustCompile(`(?m)^\s+- name:`)
	n, want := 0, 0
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		want += len(names.FindAllString(string(b), -1))
		for _, m := range re.FindAllStringSubmatch(string(b), -1) {
			n++
			if base, opts := splitSystemType(m[1]); base != "elasticsearch" {
				// The only other engine is VictoriaLogs, which takes no options.
				if base != "victorialogs" || len(opts) != 0 {
					t.Errorf("%s: %q: the variant run configs hold Elasticsearch and plain victorialogs systems", f, m[1])
				}
			} else if o, err := parseESOptions(opts); err != nil {
				t.Errorf("%s: %q: %v", f, m[1], err)
			} else if o.version == "" || o.build == "" {
				// Every Elasticsearch arm names the node it must run on.
				t.Errorf("%s: %q must assert version= and build=", f, m[1])
			} else if o.build != "release" && !strings.HasSuffix(o.version, "-SNAPSHOT") {
				// A snapshot reports its number with the suffix (9.7.0-SNAPSHOT).
				t.Errorf("%s: %q: a snapshot build must assert a -SNAPSHOT version, got %q", f, m[1], o.version)
			}
			if _, err := NewWriteClient(m[1], "http://x"); err != nil {
				t.Errorf("%s: %q: %v", f, m[1], err)
			}
			if _, err := NewQueryClient(m[1], "http://x"); err != nil {
				t.Errorf("%s: %q: %v", f, m[1], err)
			}
		}
	}
	if n == 0 || n != want {
		t.Fatalf("parsed %d system types for %d systems: the pattern missed some", n, want)
	}
}

// runConfigFiles lists the example run config and, when LOGBENCH_RUN_CONFIGS is
// set to a glob, any run configs you keep outside the repository.
func runConfigFiles() ([]string, error) {
	files, err := filepath.Glob("../../deploy/benchmarks/elasticsearch-variants/*.yaml")
	if err != nil {
		return nil, err
	}
	if g := os.Getenv("LOGBENCH_RUN_CONFIGS"); g != "" {
		more, err := filepath.Glob(g)
		if err != nil {
			return nil, err
		}
		files = append(files, more...)
	}
	return files, nil
}

// variantTypeRE matches a system's type in a run config, quoted or not.
var variantTypeRE = regexp.MustCompile(`(?m)^\s+type:\s*"?([^"\n]+?)"?\s*$`)

func splitOpts(t string) []string {
	_, opts := splitSystemType(t)
	return opts
}

// Every Elasticsearch system type must also render its template: a misspelled
// noindex field is an error there, not at parse time.
func TestVariantRunConfigTemplatesRender(t *testing.T) {
	files, _ := runConfigFiles()
	re := variantTypeRE
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range re.FindAllStringSubmatch(string(b), -1) {
			if base, _ := splitSystemType(m[1]); base != "elasticsearch" {
				continue
			}
			o, err := parseESOptions(splitOpts(m[1]))
			if err != nil {
				t.Fatalf("%s: %q: %v", f, m[1], err)
			}
			if _, err := o.templateJSON(); err != nil {
				t.Errorf("%s: %q: %v", f, m[1], err)
			}
		}
	}
}

// A run config that leaves the writer out runs the chart defaults: a fixed rate
// and no dropOnFull, so one slow system blocks the generator for all of them.
// Every config must set a ramp and dropOnFull, and its own seed.
func TestVariantRunConfigsSetTheWriter(t *testing.T) {
	files, _ := runConfigFiles()
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for _, want := range []string{`(?m)^writer:`, `(?m)^\s+dropOnFull:\s*true\s*$`, `(?m)^\s+rampMax:\s*[1-9]`, `(?m)^\s+seed:\s*\d`, `(?m)^image:`, `(?m)^querier:`} {
			if !regexp.MustCompile(want).Match(b) {
				t.Errorf("%s: no match for %s: the config must set its own image, writer and querier blocks", f, want)
			}
		}
	}
}
