// Command fieldstats generates harness log lines with the settings of the
// Elasticsearch ladder run (125 hosts, 40 apps, seed 1) and reports, per field,
// how much information the values carry: presence rate, distinct values,
// empirical entropy and the size of the column under DEFLATE.
//
// Empirical entropy is the lower bound for a per-value code of the column
// given the distribution seen in the sample. For a field that is nearly unique
// (distinct_ratio close to 1, such as request_id) it is capped at log2 of the record count and
// understates what the generator's values really carry, so read it together
// with distinct_ratio.
// DEFLATE over the values in document order is what a general-purpose
// compressor achieves, so it is a practical reference, not a floor. Use the two
// to judge how close the stored doc values are to what the data allows.
//
// Field names follow the Elasticsearch document (host.name, not host), so they
// line up with the _disk_usage output. It is an offline helper, not part of the
// benchmark, and sends nothing anywhere.
//
// usage: go run ./cmd/fieldstats -n 2000000 > fieldstats.json
package main

import (
	"bytes"
	"compress/flate"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"sort"
	"time"

	"github.com/VictoriaMetrics/logs-benchmark/internal/source/synthetic"
)

type colStats struct {
	present  int64
	rawBytes int64
	counts   map[string]int64
	sample   bytes.Buffer // first sampleN values, newline separated, doc order
	sampleN  int
}

type fieldOut struct {
	Field         string  `json:"field"`
	Presence      float64 `json:"presence"`
	Distinct      int     `json:"distinct"`
	DistinctRatio float64 `json:"distinct_ratio"`
	AvgRawBytes   float64 `json:"avg_raw_bytes"`
	EntropyBits   float64 `json:"entropy_bits"`
	FlateBytes    float64 `json:"flate_bytes_per_value"`
}

func main() {
	n := flag.Int("n", 2000000, "records to generate")
	hosts := flag.Int("hosts", 125, "host cardinality (ladder run: 125)")
	apps := flag.Int("apps", 40, "app cardinality (ladder run: 40)")
	seed := flag.Int64("seed", 1, "generator seed (ladder run: 1)")
	sampleCap := flag.Int("sample", 200000, "values per field kept for the DEFLATE size")
	flag.Parse()

	fleet := synthetic.NewFleet(*seed, *hosts, *apps)
	gen := synthetic.New(fleet, *seed)
	// One map entry per distinct value, so a high-cardinality field such as
	// request_id holds up to -n entries: expect a few hundred MB at 2M records.
	cols := map[string]*colStats{}
	add := func(k, v string) {
		c := cols[k]
		if c == nil {
			c = &colStats{counts: map[string]int64{}}
			cols[k] = c
		}
		c.present++
		c.rawBytes += int64(len(v))
		c.counts[v]++
		if c.sampleN < *sampleCap {
			c.sample.WriteString(v)
			c.sample.WriteByte('\n')
			c.sampleN++
		}
	}

	var buf []byte
	ts := time.Unix(1_700_000_000, 0).UTC()
	for i := 0; i < *n; i++ {
		rec := gen.Next()
		buf = synthetic.AppendJSON(buf[:0], rec, ts)
		// UseNumber keeps numbers as written; fmt.Sprint of a float64 would turn
		// 1000000 into 1e+06 and skew the byte counts and the distinct values.
		var m map[string]any
		dec := json.NewDecoder(bytes.NewReader(buf))
		dec.UseNumber()
		if err := dec.Decode(&m); err != nil {
			fmt.Fprintln(os.Stderr, "decode:", err)
			os.Exit(1)
		}
		for k, v := range m {
			switch k {
			case "timestamp":
				continue
			case "host":
				k = "host.name" // the Elasticsearch document nests it
			}
			add(k, fmt.Sprint(v))
		}
	}

	out := make([]fieldOut, 0, len(cols))
	for k, c := range cols {
		var h float64
		for _, cnt := range c.counts {
			p := float64(cnt) / float64(c.present)
			h -= p * math.Log2(p)
		}
		var z bytes.Buffer
		w, _ := flate.NewWriter(&z, 9)
		_, _ = w.Write(c.sample.Bytes())
		_ = w.Close()
		out = append(out, fieldOut{
			Field:         k,
			Presence:      float64(c.present) / float64(*n),
			Distinct:      len(c.counts),
			DistinctRatio: float64(len(c.counts)) / float64(c.present),
			AvgRawBytes:   float64(c.rawBytes) / float64(c.present),
			EntropyBits:   h,
			FlateBytes:    float64(z.Len()) / float64(c.sampleN),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Field < out[j].Field })
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", " ")
	_ = enc.Encode(out)
}
