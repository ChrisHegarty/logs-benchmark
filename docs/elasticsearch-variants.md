# Comparing Elasticsearch variants

The `elasticsearch` system can be installed and driven in several configurations
side by side, so you can measure what a setting, a mapping, a license or a build
changes for log ingestion and storage. This page describes the mechanisms; the
choice of variants and the load is yours. `deploy/benchmarks/elasticsearch-variants/example.yaml`
is a starting point. An option-less `elasticsearch` system behaves as before, and
the other engines are unchanged.

## Variant options on the system type

The `type` of a system can carry options after semicolons, for example
`elasticsearch;version=<n>;build=release;license=trial;set.index.refresh_interval=30s`.
Unknown options and repeated keys are an error, and the other engines reject any
option. `internal/systems/elasticsearch_options.go` has the full list:

| Option | Effect |
| --- | --- |
| `probe=count\|seqno\|none` | How stored rows are counted. `count` is the original `_count`, which is a search and refreshes the shard every `index.refresh_interval`. `seqno` sums `max_seq_no`, valid for an append-only stream. `none` sends nothing and uses the lines the bulk API acknowledged. |
| `license=trial` | Starts the trial license before the first write. |
| `version=<number>`, `build=release\|<hash prefix>` | Assert the node's version and build before anything else, read-only. A snapshot reports its number with the `-SNAPSHOT` suffix. Asserting them on every system keeps an image meant for one instance from silently ending up on another. |
| `mode=<index mode>` | Asserts `index.mode` of the backing index. With `mode` or `set.*` the harness creates the data stream before the first write, reads the backing index settings back and stops the run if the mode or any `set.*` value differs. |
| `set.<setting>=<value>` | Any index setting, applied through a composable template built on the components of the cluster's own built-in `logs` template (read at startup), so a variant differs from the built-in setup only by its options. |
| `sort=<fields>` | The index sort. |
| `message=keyword` | Maps `message` as `keyword` instead of the built-in `match_only_text`. |
| `noindex=<f>+<f>+...` | Maps only the named workload fields doc-values only (`index: false`). An unknown name is an error. |
| `noindex=all` | Maps every field of the workload's field pool doc-values only. `message`, `app`, `level` and `host` are left alone. |
| `target=<name>` | Overrides the data stream name. |

A variant template (priority 200) replaces the built-in `logs` template, so it
can lose what that one carried, such as `index.mode`. That is why `mode=` is
checked on the backing index rather than assumed. The explicit mappings
(`message`, `noindex`, `sort`) are read back and checked the same way.

## Chart and Makefile hooks

- `deploy/charts/elasticsearch`: `image` sets `spec.image` on the ECK resource,
  and `env` sets container environment variables, for example `ES_JAVA_OPTS` to
  change the heap (set no memory limit if you want only the heap to change). The
  chart refuses to render `image` without `version`, and refuses
  `cluster.logsdb_columnar.enabled` without an image.
- `ES_IMAGE` (install) sets the Elasticsearch image; `BENCH_IMAGE_REPO` and
  `BENCH_IMAGE_TAG` (deploy) set the harness image. Environment or `make`
  command line; empty by default. `ES_IMAGE` applies to every install made with
  it set, so give it per install, not with `export`.
- `deploy-bench` refuses a run config that sets its own `image.tag` without a
  harness repository (in the config or `BENCH_IMAGE_REPO`), because the default
  repository does not have that tag.

## Building the images

- **Harness:** `docker build -t <registry>/logs-benchmark:variants .` and push it
  where your cluster can pull from.
- **Elasticsearch build you provide** (a snapshot, a branch): build the Docker
  image with `./gradlew :distribution:docker:buildDockerImage`
  (`buildAarch64DockerImage` on arm) from your checkout, push it, and install the
  instance with `ES_IMAGE=<registry>/elasticsearch:<tag>`. Assert it with
  `version=` and `build=<commit prefix>`.

## Running

```
make install-elasticsearch POOL=<instance> BENCH=deploy/benchmarks/<your-config-dir> \
  [EXTRA_VALUES=<values file>] [ES_IMAGE=<image>] CONTEXT=<kubectl context>
make deploy-bench BENCH=deploy/benchmarks/<your-config-dir> RUN=<config name> \
  BENCH_IMAGE_REPO=<registry>/logs-benchmark CONTEXT=<kubectl context>
```

`POOL` is the instance name used in the run config, so repeat the install for
each system. `BENCH` may be any directory with a `systems/elasticsearch.yaml`
sizing overlay and the run config, in or outside this repository. Use the same
hardware, volume size and storage class for every instance.

To validate run configs you keep outside the repository, point
`LOGBENCH_RUN_CONFIGS` at a glob when running `go test ./internal/systems`: every
matching file is parsed and rendered like the example.

## Checks after a run

`deploy/benchmarks/audit-elasticsearch.sh` saves what each instance reports about
itself while the cluster is up:

```
CONTEXT=<kubectl context> deploy/benchmarks/audit-elasticsearch.sh <out-dir> <instance>...
```

Per Elasticsearch instance it saves the version and build, the JVM heap and
arguments, the license, the cluster settings, the index templates, the settings
and mapping of the backing index, the stats, the document count, the count of
documents with an `_ignored` field, `_field_caps` and the pod (node and
resources). `SETTLE=1` first waits until merges are idle and the store size is
stable for two minutes (an instance that does not settle is a failure), and
`DISK_USAGE=1` adds `_disk_usage`, which is fast on a settled index; use both
before comparing storage between instances. Instances named `vl-*` are read as
VictoriaLogs (flags, metrics and `field_names`). A failed file is listed in
`MANIFEST.txt` and the script exits non-zero. Useful things to check from it:

- the version and build, which the harness already asserts for you;
- the index mode and settings of the backing index, and the template that applied;
- `_nodes/stats/ingest`: `ingest.total.count` is 0 when the default pipeline is off;
- `_nodes/jvm`: `heap_max_in_bytes` and a single `-Xms`/`-Xmx` pair in
  `input_arguments`, and no `limits.memory` on the pod if you only wanted to change the heap;
- the document count against what the harness sent: lines whose bulk timed out
  can still have been applied, so stored can exceed sent by the timed-out lines.
  Compute storage per million rows from the stored count;
- the `_ignored` count and `docs.deleted` (both expected 0), and the same field
  list on every instance (`_field_caps`; VictoriaLogs `field_names`);
- not indexing a field is shown by the mapping (`index: false`) and by
  `_disk_usage`, where the field's `inverted_index` is 0. A query on such a field
  is not a test: doc values answer it, so it does not fail;
- `_disk_usage` for per-field storage, and `go run ./cmd/fieldstats` for what the
  generated values carry offline.
