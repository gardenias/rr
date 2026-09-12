# rr production perf report — 20260912-234352 (git `9fa8347`)

Rendered by `scripts/prod-perf-report.py` from raw JSON/JSONL in this directory; no number is hand-copied. rr-prod-perf plan: `.sisyphus/plans/rr-prod-perf.md`.

## Meta

- host: `Darwin 25.6.0 arm64` | docker: `29.4.0` | smoke: `0`
- rustc: `unknown` | tool: rr-perf-bench `0.2.0`
- cluster: 3× kv-store (cpuset 2 cores + 2GiB each, see `.env`), durable Raft log, TLS+mTLS+token secure mode unless a leg says otherwise; load runs in-network

## Tier status

| tier | status | note |
|---|---|---|
| sec | ✅ | plaintext negative control |
| w1 | ✅ | write ladder payload=128B |
| w1 | ✅ | write ladder payload=1024B |
| w1 | ✅ | write ladder payload=4096B |
| w2 | ✅ | mix write:90,local:10 @ 3000/s |
| w2 | ✅ | mix write:50,local:50 @ 3000/s |
| w3 | ✅ | read mix local:100 @ 6000/s |
| w3 | ✅ | read mix local:90,lin:10 @ 6000/s |
| w3 | ✅ | read mix local:50,lin:50 @ 6000/s |
| w4 | ✅ | write zipfian @ 3000/s |
| w4 | ✅ | write uniform @ 3000/s |
| w5 | ✅ | write payload=128B @ 3000/s |
| w5 | ✅ | write payload=1024B @ 3000/s |
| w5 | ✅ | write payload=4096B @ 3000/s |
| w9 | ❌ | soak 7200s @ 2000/s + TransferLeader |
| w7 | ✅ | snapshot pressure (5000) wladder |
| w6 | ✅ | kafka ingest @ 2000/s for 60s |
| w11 | ✅ | mem-log control wladder |
| w8 | ✅ | insecure control wladder |

## W1 — write rate ladder (open-loop, knee detection)

### payload w1-1024B

| rung ops/s | offered | ok | err | TPS | p50 | p99 | p999 | lag-p99 |
|---|---|---|---|---|---|---|---|---|
| 100 | 3000 | 3000 | 0 | 100 | 10.2ms | 17.7ms | 38.9ms | 5.3ms |
| 500 | 15000 | 15000 | 0 | 500 | 9.8ms | 83.3ms | 171.2ms | 4.7ms |
| 1000 | 30000 | 30000 | 0 | 1.0k | 9.7ms | 615.0ms | 884.5ms | 4.7ms |
| 2000 | 60000 | 60000 | 0 | 2.0k | 10.3ms | 476.3ms | 715.1ms | 4.6ms |
| 4000 | 120000 | 120000 | 0 | 4.0k | 11.0ms | 595.1ms | 770.7ms | 4.7ms |
| 8000 | 240000 | 231079 | 8921 | 7.7k | 1165.0ms | 11945.4ms | 12918.5ms | 2340.4ms |
| 16000 | 258320 | 249654 | 8666 | 8.3k | 17750.3ms | 34124.9ms | 34359.7ms | 34259.1ms |

knee (p99 > 2× previous rung): **100**
server-side window: rate=4.6k/s p99=5000.0ms proposal-p99=5000.0ms apply-p99=497µs appendRTT-p99=23.9ms lag-max=2535807.0

### payload w1-128B

| rung ops/s | offered | ok | err | TPS | p50 | p99 | p999 | lag-p99 |
|---|---|---|---|---|---|---|---|---|
| 100 | 3000 | 3000 | 0 | 100 | 10.4ms | 19.7ms | 55.5ms | 5.4ms |
| 500 | 15000 | 15000 | 0 | 500 | 9.6ms | 19.6ms | 130.5ms | 5.1ms |
| 1000 | 30000 | 30000 | 0 | 1.0k | 8.7ms | 102.3ms | 331.4ms | 4.5ms |
| 2000 | 60000 | 60000 | 0 | 2.0k | 8.3ms | 52.6ms | 179.3ms | 4.5ms |
| 4000 | 120000 | 120000 | 0 | 4.0k | 9.0ms | 104.8ms | 213.5ms | 4.6ms |
| 8000 | 240000 | 240000 | 0 | 8.0k | 9.6ms | 150.7ms | 213.3ms | 4.7ms |
| 16000 | 480000 | 480000 | 0 | 16.0k | 12.3ms | 310.6ms | 361.5ms | 4.8ms |
| 32000 | 919118 | 919118 | 0 | 30.6k | 356.3ms | 2346.7ms | 2367.7ms | 1318.1ms |

knee (p99 > 2× previous rung): **500**
server-side window: rate=7.3k/s p99=2267.3ms proposal-p99=2267.3ms apply-p99=496µs appendRTT-p99=20.9ms lag-max=32612.0

### payload w1-4096B

| rung ops/s | offered | ok | err | TPS | p50 | p99 | p999 | lag-p99 |
|---|---|---|---|---|---|---|---|---|
| 100 | 3000 | 3000 | 0 | 100 | 10.3ms | 17.0ms | 37.2ms | 5.3ms |
| 500 | 15000 | 15000 | 0 | 500 | 12.3ms | 4580.2ms | 4781.5ms | 5.1ms |
| 1000 | 30000 | 30000 | 0 | 1.0k | 13.7ms | 77.3ms | 129.7ms | 4.9ms |
| 2000 | 60000 | 59739 | 261 | 2.0k | 116.7ms | 5221.9ms | 5595.2ms | 4.8ms |
| 4000 | 120000 | 102835 | 17165 | 3.4k | 2189.4ms | 17104.4ms | 17951.6ms | 2885.7ms |
| 8000 | 240000 | 218917 | 21083 | 7.3k | 18220.1ms | 34259.1ms | 34359.7ms | 31927.0ms |
| 16000 | 30376 | 9431 | 20945 | 314 | 0µs | 0µs | 0µs | 0µs |

knee (p99 > 2× previous rung): **100**
server-side window: rate=2.9k/s p99=5000.0ms proposal-p99=5000.0ms apply-p99=496µs appendRTT-p99=45.7ms lag-max=3108871.0

## W2 — mixed read/write
### w2-write_50_local_50

| op | offered | ok | err | TPS | p50 | p99 | p999 | svc-p99 | lag-p99 |
|---|---|---|---|---|---|---|---|---|---|
| write | 44625 | 44625 | 0 | 1.5k | 7.8ms | 298.6ms | 556.3ms | 297.5ms | 4.4ms |
| local | 45374 | 45374 | 0 | 1.5k | 1.4ms | 5.0ms | 7.1ms | 1.1ms | 4.5ms |

config: `mix=write:50,local:50 key-dist=uniform space=1000000 payload=128B poisson=False`


### w2-write_90_local_10

| op | offered | ok | err | TPS | p50 | p99 | p999 | svc-p99 | lag-p99 |
|---|---|---|---|---|---|---|---|---|---|
| write | 80899 | 80899 | 0 | 2.7k | 8.6ms | 865.1ms | 1149.2ms | 864.0ms | 4.6ms |
| local | 9100 | 9100 | 0 | 303 | 1.4ms | 5.5ms | 13.2ms | 2.0ms | 4.5ms |

config: `mix=write:90,local:10 key-dist=uniform space=1000000 payload=128B poisson=False`

## W3 — linearizable-read share

### w3-local_100

| op | offered | ok | err | TPS | p50 | p99 | p999 | svc-p99 | lag-p99 |
|---|---|---|---|---|---|---|---|---|---|
| local | 179994 | 179994 | 0 | 6.0k | 2.1ms | 6.9ms | 9.3ms | 3.8ms | 4.9ms |

config: `mix=local:100 key-dist=uniform space=1000000 payload=128B poisson=False`

### w3-local_50_lin_50

| op | offered | ok | err | TPS | p50 | p99 | p999 | svc-p99 | lag-p99 |
|---|---|---|---|---|---|---|---|---|---|
| local | 89433 | 89433 | 0 | 3.0k | 1.6ms | 6.2ms | 7.9ms | 2.2ms | 5.3ms |
| lin | 90557 | 90557 | 0 | 3.0k | 2.6ms | 8.6ms | 11.8ms | 5.5ms | 5.2ms |

config: `mix=local:50,lin:50 key-dist=uniform space=1000000 payload=128B poisson=False`

### w3-local_90_lin_10

| op | offered | ok | err | TPS | p50 | p99 | p999 | svc-p99 | lag-p99 |
|---|---|---|---|---|---|---|---|---|---|
| local | 161818 | 161818 | 0 | 5.4k | 2.1ms | 6.7ms | 8.0ms | 3.0ms | 5.3ms |
| lin | 18157 | 18157 | 0 | 605 | 2.9ms | 8.4ms | 10.0ms | 4.8ms | 5.3ms |

config: `mix=local:90,lin:10 key-dist=uniform space=1000000 payload=128B poisson=False`

## W4 — hot keys (zipfian vs uniform, same rate)

### zipfian(0.99)

| op | offered | ok | err | TPS | p50 | p99 | p999 | svc-p99 | lag-p99 |
|---|---|---|---|---|---|---|---|---|---|
| write | 89999 | 89999 | 0 | 3.0k | 9.3ms | 1004.0ms | 1249.9ms | 1001.9ms | 4.5ms |

config: `mix=write:100 key-dist=zipfian(0.99) space=100000 payload=128B poisson=False`

### uniform

| op | offered | ok | err | TPS | p50 | p99 | p999 | svc-p99 | lag-p99 |
|---|---|---|---|---|---|---|---|---|---|
| write | 90000 | 90000 | 0 | 3.0k | 9.8ms | 2218.8ms | 2487.2ms | 2218.8ms | 4.6ms |

config: `mix=write:100 key-dist=uniform space=100000 payload=128B poisson=False`

hot-key tax: zipfian p99 / uniform p99 = **0.45×**

## W5 — payload scan

| payload | TPS | p50 | p99 | p999 |
|---|---|---|---|---|
| 1024B | 3.0k | 12.6ms | 1938.8ms | 2176.8ms |
| 128B | 3.0k | 10.4ms | 1228.9ms | 1487.9ms |
| 4096B | 3.0k | 1810.9ms | 10494.1ms | 11609.8ms |

## W6 — Kafka ingest path

- window: 63s | ingestor batch commit rate: **2.0k/s** | batch proposal p99: 24.7ms
- producer log: `producer-w6.log`

## W7 — snapshot pressure

### snapshot-logs-since-last lowered

| rung ops/s | offered | ok | err | TPS | p50 | p99 | p999 | lag-p99 |
|---|---|---|---|---|---|---|---|---|
| 100 | 3000 | 3000 | 0 | 100 | 10.6ms | 36.0ms | 61.5ms | 5.3ms |
| 500 | 15000 | 15000 | 0 | 500 | 9.4ms | 19.9ms | 66.5ms | 4.9ms |
| 1000 | 30000 | 30000 | 0 | 1.0k | 8.5ms | 26.7ms | 141.4ms | 4.3ms |
| 2000 | 60000 | 60000 | 0 | 2.0k | 10.0ms | 144.3ms | 317.2ms | 4.7ms |
| 4000 | 120000 | 120000 | 0 | 4.0k | 9.0ms | 90.6ms | 169.0ms | 4.6ms |
| 8000 | 239992 | 239992 | 0 | 8.0k | 10.3ms | 326.1ms | 445.9ms | 4.7ms |

knee (p99 > 2× previous rung): **1.0k**
server-side window: rate=2.5k/s p99=135.0ms proposal-p99=135.0ms apply-p99=495µs appendRTT-p99=21.7ms lag-max=4792728.0

Compare the time series against W1 (`ts-w7.jsonl` vs `ts-w1-128.jsonl`): snapshot spikes show as 1s-bucket p99 jumps.

## W8 — security tax (A/B)

### INSECURE control leg (plaintext, no mTLS/token)

| rung ops/s | offered | ok | err | TPS | p50 | p99 | p999 | lag-p99 |
|---|---|---|---|---|---|---|---|---|
| 100 | 3000 | 3000 | 0 | 100 | 10.2ms | 19.8ms | 44.9ms | 5.2ms |
| 500 | 15000 | 15000 | 0 | 500 | 9.5ms | 24.9ms | 152.6ms | 5.0ms |
| 1000 | 30000 | 30000 | 0 | 1.0k | 8.8ms | 37.6ms | 187.4ms | 4.7ms |
| 2000 | 60000 | 60000 | 0 | 2.0k | 8.3ms | 35.4ms | 92.0ms | 4.5ms |
| 4000 | 120000 | 120000 | 0 | 4.0k | 8.5ms | 47.6ms | 144.4ms | 4.5ms |
| 8000 | 240000 | 240000 | 0 | 8.0k | 9.7ms | 152.4ms | 213.5ms | 4.7ms |
| 16000 | 480000 | 480000 | 0 | 16.0k | 12.3ms | 300.7ms | 446.7ms | 4.8ms |
| 32000 | 863387 | 863387 | 0 | 28.8k | 1754.3ms | 4055.9ms | 4076.9ms | 3032.5ms |

knee (p99 > 2× previous rung): **4.0k**
server-side window: rate=7.0k/s p99=2431.1ms proposal-p99=2431.5ms apply-p99=496µs appendRTT-p99=21.3ms lag-max=42091.0

## W9 — production soak

- duration: 7200s @ 2000/s | sent=970984 acked=970984 failed=0 | reads ok/failed: 96/623
- a `TransferLeader` was issued mid-window (see bench-w9.log)

## W11 — durable raft-log cost (A/B, ADR A-0010 re-judgement)

### mem-log control leg (pre-M0 shape)

| rung ops/s | offered | ok | err | TPS | p50 | p99 | p999 | lag-p99 |
|---|---|---|---|---|---|---|---|---|
| 100 | 3000 | 3000 | 0 | 100 | 4.1ms | 7.6ms | 17.5ms | 5.3ms |
| 500 | 15000 | 15000 | 0 | 500 | 2.9ms | 7.5ms | 8.6ms | 5.4ms |
| 1000 | 30000 | 30000 | 0 | 1.0k | 2.6ms | 7.1ms | 8.3ms | 5.5ms |
| 2000 | 60000 | 60000 | 0 | 2.0k | 2.3ms | 7.1ms | 21.1ms | 5.4ms |
| 4000 | 120000 | 120000 | 0 | 4.0k | 2.5ms | 7.6ms | 50.1ms | 5.4ms |
| 8000 | 240000 | 240000 | 0 | 8.0k | 2.8ms | 102.5ms | 157.5ms | 5.4ms |

knee (p99 > 2× previous rung): **4.0k**
server-side window: rate=2.5k/s p99=60.7ms proposal-p99=60.7ms apply-p99=495µs appendRTT-p99=1.7ms lag-max=82.0

> Reading note: the control leg also switches the KV state machine to memory (mem-SM confound; R-7 measured mem-vs-rocks SM at <2%). The raft-log fsync path is the dominant term being isolated.

## Criteria (plan §9)

| metric | value |
|---|---|
| knee TPS (write, 128B) | not reached / not run |
| security tax (W8) | see W8 table |
| raft-log persistence cost (W11) | see W11 table — feeds the ADR A-0010 re-judgement |
