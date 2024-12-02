#!/bin/bash
readonly messageSize="${1:-200}"
readonly batchSize="${2:-200}"
readonly memQueueSize="${3:-1000000}"
readonly dataPath="${4:-}"
set -e
set -u

echo "# using --mem-queue-size=$memQueueSize --data-path=$dataPath --size=$messageSize --batch-size=$batchSize"
echo "# compiling/running nsql"
pushd ../../nsql/ >/dev/null
zig build -Doptimize=ReleaseFast
rm -f ./tmp/nsql.dump ./tmp/sub_bench
./zig-out/bin/nsql &
nsqd_pid=$!

popd >/dev/null

cleanup() {
    kill -9 $nsqd_pid
    rm -f nsqd/*.dat
}
trap cleanup INT TERM EXIT


# curl --silent 'http://127.0.0.1:4151/create_topic?topic=sub_bench' >/dev/null 2>&1
# curl --silent 'http://127.0.0.1:4151/create_channel?topic=sub_bench&channel=ch' >/dev/null 2>&1

echo "# compiling bench_reader/bench_writer"
pushd bench >/dev/null
for app in bench_reader bench_writer; do
    pushd $app >/dev/null
    go build
    popd >/dev/null
done
popd >/dev/null

sleep 0.3
# echo "# creating topic/channel"
# bench/bench_reader/bench_reader --runfor 0 2>/dev/null

echo -n "PUB: "
bench/bench_writer/bench_writer --size=$messageSize --batch-size=$batchSize 2>&1

# curl -s -o cpu.pprof http://127.0.0.1:4151/debug/pprof/profile &
# pprof_pid=$!

echo -n "SUB: "
bench/bench_reader/bench_reader --size=$messageSize --runfor 7s --channel=ch 2>&1

#echo "waiting for pprof..."
#wait $pprof_pid
