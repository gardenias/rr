# rr — Fuzz Targets

cargo-fuzz harness for the wire-format decoders. The targets all
follow the "no panic on arbitrary input" contract: feed bytes,
ignore Ok/Err, fail only on panic / abort / oom.

## Targets

| Target | Function under test |
|---|---|
| `fuzz_framed_snapshot_decode` | `rr_core::framed_snapshot::FramedSnapshot::decode` |
| `fuzz_framed_entry_decode` | `rr_core::framed_entry::FramedEntry::decode` |
| `fuzz_apply_outcome_decode` | `rr_core::idempotency::ApplyOutcome::decode` |
| `fuzz_client_request_proto` | `<rr_network::proto::client::ClientRequest as prost::Message>::decode` |

## Prerequisites

cargo-fuzz needs nightly + libFuzzer:

```bash
rustup toolchain install nightly
cargo install cargo-fuzz
```

## Running

```bash
# Run a target until you ctrl-C (or it finds a crash):
cargo +nightly fuzz run fuzz_framed_snapshot_decode

# Time-bounded (recommended for CI):
cargo +nightly fuzz run fuzz_framed_snapshot_decode -- -max_total_time=60

# All four targets, 30s each:
for target in fuzz_framed_snapshot_decode fuzz_framed_entry_decode \
              fuzz_apply_outcome_decode fuzz_client_request_proto; do
    cargo +nightly fuzz run "$target" -- -max_total_time=30 || exit 1
done
```

## Corpus

Initial corpus is empty — libFuzzer generates inputs from scratch.
On first crash, the failing input lands in
`fuzz/artifacts/<target>/crash-*`. Add it to
`fuzz/corpus/<target>/` to ensure regressions get caught on every
run.

## Why this crate is NOT in the workspace

cargo-fuzz requires a dedicated sub-crate with `panic = "abort"`
profile + nightly toolchain + `libfuzzer-sys` (which links a C++
runtime). The parent workspace pins stable-1.88 and uses
`panic = "unwind"` everywhere; mixing these in one workspace
breaks `cargo build --workspace` on stable.

## CI integration

Add to a separate workflow (recommend a nightly cron, not on every
push, so the matrix builds stay fast):

```yaml
- name: cargo-fuzz smoke (60s per target)
  run: |
    rustup toolchain install nightly --profile minimal
    cargo install cargo-fuzz --locked
    for t in fuzz_framed_snapshot_decode fuzz_framed_entry_decode \
             fuzz_apply_outcome_decode fuzz_client_request_proto; do
      cargo +nightly fuzz run "$t" --fuzz-dir fuzz \
        -- -max_total_time=60 -error_exitcode=42
    done
```
