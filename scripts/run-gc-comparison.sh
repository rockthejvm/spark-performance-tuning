#!/usr/bin/env bash
#
# Proves the G1GC lesson: runs the SAME Spark workload under three garbage
# collectors and prints a side-by-side table of GC behaviour.
#
# GC flags can only be set at JVM launch, so we can't switch collectors inside
# one process — instead we launch part6spark4.G1GCTuning three times, once per GC.
#
# Usage:  ./scripts/run-gc-comparison.sh [rows] [iterations] [heap]
# Example: ./scripts/run-gc-comparison.sh 8000000 6 2g
#
set -euo pipefail
cd "$(dirname "$0")/.."

ROWS="${1:-8000000}"
ITERS="${2:-6}"
HEAP="${3:-2g}"

echo "Compiling..."
sbt --batch -error compile

echo "Resolving runtime classpath..."
CP="$(sbt --batch -error 'export runtime:fullClasspath' | tail -n1)"

# Spark 4 on JDK 17+ needs these module opens when launched via plain `java`
# (spark-submit / the Spark launcher add them for you automatically).
OPENS=(
  --add-opens=java.base/java.lang=ALL-UNNAMED
  --add-opens=java.base/java.lang.invoke=ALL-UNNAMED
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED
  --add-opens=java.base/java.io=ALL-UNNAMED
  --add-opens=java.base/java.net=ALL-UNNAMED
  --add-opens=java.base/java.nio=ALL-UNNAMED
  --add-opens=java.base/java.util=ALL-UNNAMED
  --add-opens=java.base/java.util.concurrent=ALL-UNNAMED
  --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED
  --add-opens=java.base/jdk.internal.ref=ALL-UNNAMED
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED
  --add-opens=java.base/sun.nio.cs=ALL-UNNAMED
  --add-opens=java.base/sun.security.action=ALL-UNNAMED
  --add-opens=java.base/sun.util.calendar=ALL-UNNAMED
)

run_gc () {
  local label="$1"; shift
  echo
  echo "############################################################"
  echo "#  $label   (heap=$HEAP, rows=$ROWS, iters=$ITERS)"
  echo "############################################################"
  java -Xms"$HEAP" -Xmx"$HEAP" "$@" "${OPENS[@]}" \
    -cp "$CP" part6spark4.G1GCTuning "$ROWS" "$ITERS" 2>/dev/null \
    | grep -E "Active garbage|Wall-clock|GC collections|Total GC pause|Average pause|GC overhead|>>"
}

run_gc "SerialGC   (old single-threaded baseline)" -XX:+UseSerialGC
run_gc "ParallelGC (Spark 3 throughput default)"   -XX:+UseParallelGC
run_gc "G1GC       (Spark 4 default, JDK 17+)"      -XX:+UseG1GC -XX:MaxGCPauseMillis=200

echo
echo "Read the 'Average pause / GC' row across the three runs:"
echo "  SerialGC   -> few, very long pauses (single-threaded)"
echo "  ParallelGC -> high throughput but larger individual pauses"
echo "  G1GC       -> many short pauses, bounded by the MaxGCPauseMillis goal"
echo "That shorter-pause trade-off is why Spark 4 defaults to G1GC on JDK 17+."
