#!/usr/bin/env bash
# Saves what each instance of a run reports about itself, so every claim
# made from a run can be checked later: the version and build, the heap, the
# license, the index mode, the settings and mappings the backing index really got,
# the templates, the node the pod ran on and its resources, and the stored
# document counts. Run it after the load has finished (the settled disk size needs
# DISK_USAGE=1, which takes a long time on a big index).
#
# usage: CONTEXT=<kubectl context> deploy/benchmarks/audit-elasticsearch.sh <out-dir> <instance>...
#
# <instance> is the system name of the run config (the POOL it was installed
# with). Names starting with "vl-" are VictoriaLogs instances (read through the
# API server's service proxy: the image has no shell); the others are
# Elasticsearch instances (read with curl in the elasticsearch container).
#
# environment:
#   CONTEXT      kubectl context (required)
#   NS           namespace of the instances (default logs-bench)
#   TARGET       data stream name (default logs-bench-default)
#   DISK_USAGE   set to 1 to also run _disk_usage?run_expensive_tasks=true
#   SETTLE       set to 1 to wait first until merges are idle and the store size
#                is stable for two minutes (SETTLE_MAX seconds at most, default
#                1800); an instance that does not settle is a failure. Use it
#                with DISK_USAGE=1, since a size read before the merges settle
#                is not comparable between instances.
set -u

: "${CONTEXT:?set CONTEXT to the kubectl context}"
NS=${NS:-logs-bench}
TARGET=${TARGET:-logs-bench-default}
DISK_USAGE=${DISK_USAGE:-0}
SETTLE=${SETTLE:-0}
SETTLE_MAX=${SETTLE_MAX:-1800}

if [ "$#" -lt 2 ]; then
	echo "usage: CONTEXT=<ctx> $0 <out-dir> <instance>..." >&2
	exit 2
fi
out=$1
shift
mkdir -p "$out"

kc() { kubectl --context "$CONTEXT" --namespace "$NS" "$@"; }
failed=0

{
	echo "audit started: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
	echo "context: $CONTEXT"
	echo "namespace: $NS"
	echo "data stream: $TARGET"
	echo "instances: $*"
} >"$out/MANIFEST.txt"

# save <file> <command...>: run the command, keep its output, count a failure.
save() {
	local file=$1
	shift
	if ! "$@" >"$file" 2>"$file.err"; then
		echo "FAILED: $file" >>"$out/MANIFEST.txt"
		failed=$((failed + 1))
	fi
	[ -s "$file.err" ] || rm -f "$file.err"
}

# settle <function that reads "merges size">: wait until merges are idle and the
# store size is the same on two reads a minute apart.
settle() {
	local read=$1 start now prev="" cur stable=0
	start=$(date +%s)
	while :; do
		cur=$($read)
		case "$cur" in
		"0 "*) if [ "$cur" = "$prev" ]; then stable=$((stable + 1)); else stable=0; fi ;;
		*) stable=0 ;;
		esac
		prev=$cur
		[ "$stable" -ge 2 ] && return 0
		now=$(date +%s)
		[ $((now - start)) -ge "$SETTLE_MAX" ] && return 1
		sleep 60
	done
}

audit_es() {
	local inst=$1 dir=$2 pod="$1-es-default-0"
	es() { kc exec "$pod" -c elasticsearch -- curl -s -m 600 "localhost:9200/$1"; }
	esq() { kc exec "$pod" -c elasticsearch -- curl -s -m 600 -H 'content-type: application/json' "localhost:9200/$1" -d "$2"; }
	merges_and_size() {
		local b
		b=$(es "$TARGET/_stats/merge,store?filter_path=_all.primaries.merges.current,_all.primaries.store.size_in_bytes")
		echo "$(echo "$b" | sed -n 's/.*"merges":{"current":\([0-9]*\).*/\1/p') $(echo "$b" | sed -n 's/.*"size_in_bytes":\([0-9]*\).*/\1/p')"
	}
	if [ "$SETTLE" = 1 ]; then
		if settle merges_and_size; then echo "settled: $inst" >>"$out/MANIFEST.txt"; else
			echo "FAILED: $inst did not settle within ${SETTLE_MAX}s" >>"$out/MANIFEST.txt"
			failed=$((failed + 1))
		fi
	fi
	save "$dir/pod.json" kc get pod "$pod" -o json
	save "$dir/root.json" es '?filter_path=version'
	save "$dir/nodes-jvm.json" es '_nodes/jvm?filter_path=nodes.*.jvm.mem,nodes.*.jvm.input_arguments,nodes.*.jvm.version'
	save "$dir/nodes-stats.json" es '_nodes/stats/ingest,indices,jvm?filter_path=nodes.*.ingest.total,nodes.*.indices.docs.count,nodes.*.indices.refresh,nodes.*.indices.merges,nodes.*.indices.flush,nodes.*.jvm.mem'
	save "$dir/license.json" es '_license?filter_path=license.type'
	save "$dir/cluster-settings.json" es '_cluster/settings?flat_settings=true'
	save "$dir/template-logs.json" es '_index_template/logs'
	# Absent on an arm without options (the baseline): the error body is the record.
	save "$dir/template-variant.json" es "_index_template/logs-bench-variant-$TARGET"
	save "$dir/settings.json" es "$TARGET/_settings?flat_settings=true"
	save "$dir/mapping.json" es "$TARGET/_mapping"
	save "$dir/stats.json" es "$TARGET/_stats/docs,store,indexing,merge,refresh,flush?level=shards"
	save "$dir/count.json" es "$TARGET/_count"
	# Documents with a field the mapping could not take (expected 0 on every arm).
	save "$dir/ignored-count.json" esq "$TARGET/_count" '{"query":{"exists":{"field":"_ignored"}}}'
	save "$dir/field-caps.json" es "$TARGET/_field_caps?fields=*"
	if [ "$DISK_USAGE" = 1 ]; then
		save "$dir/disk-usage.json" kc exec "$pod" -c elasticsearch -- curl -s -m 7200 -XPOST "localhost:9200/$TARGET/_disk_usage?run_expensive_tasks=true"
	fi
}

audit_vl() {
	local inst=$1 dir=$2 svc="$1-victoria-logs-single-server"
	vl() { kc get --raw "/api/v1/namespaces/$NS/services/http:$svc:9428/proxy/$1"; }
	save "$dir/pod.json" kc get pod "$svc-0" -o json
	save "$dir/flags.txt" vl flags
	save "$dir/metrics.txt" vl metrics
	save "$dir/field-names.json" vl "select/logsql/field_names?query=*&start=2000-01-01T00:00:00Z"
}

for inst in "$@"; do
	dir="$out/$inst"
	mkdir -p "$dir"
	echo "== $inst"
	case "$inst" in
	vl-*) audit_vl "$inst" "$dir" ;;
	*) audit_es "$inst" "$dir" ;;
	esac
done

echo "audit finished: $(date -u +%Y-%m-%dT%H:%M:%SZ), $failed failed" >>"$out/MANIFEST.txt"
if [ "$failed" -gt 0 ]; then
	echo "$failed file(s) failed, see $out/MANIFEST.txt" >&2
	exit 1
fi
echo "saved to $out"
