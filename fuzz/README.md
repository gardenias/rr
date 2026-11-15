# Rr — Fuzz Targets

cargo-fuzz harness for the wire-format decoders. The decode targets
follow the "no panic on arbitrary input" contract: feed bytes,
ignore Ok/Err, fail only on panic / abort / oom. The roundtrip
targets go one level deeper: any input the decoder *accepts* must
re-encode and re-decode to an equal value (decode→encode→decode
idempotence), catching "decode accepted but semantics drifted" bugs.

## Targets

| Target | Function under test |
|---|---|
| `fuzz_framed_snapshot_decode` | `rr_core::framed_snapshot::FramedSnapshot::decode` |
| `fuzz_framed_entry_decode` | `rr_core::framed_entry::FramedEntry::decode` |
| `fuzz_apply_outcome_decode` | `rr_core::idempotency::ApplyOutcome::decode` |
| `fuzz_client_request_proto` | `<rr_network::proto::client::ClientRequest as prost::Message>::decode` |
| `fuzz_batch_frame_decode` | `rr_core::batch::decode_batch` + `rr_core::batch::validate_batch_frame` |
| `fuzz_framed_entry_roundtrip` | `FramedEntry` decode→encode→decode idempotence |
| `fuzz_apply_outcome_roundtrip` | `ApplyOutcome` decode→encode→decode idempotence |
| `fuzz_batch_frame_roundtrip` | `decode_batch`→`encode_batch`→`decode_batch` idempotence |

## Not fuzzed here: the rr-raft on-disk envelopes (PST / MBR)

rr-raft's two persisted envelopes — the snapshot store (`\0rrPST01`,
`crates/rr-raft/src/snapshot_store.rs`) and the membership sidecar
(`sm-membership.bin`, `\0rrMBR01`, `crates/rr-raft/src/sm_adapter.rs`) —
compile fine without the `rocks` feature, so feature-gating is NOT the
blocker. The blocker is visibility: `SnapshotStore::load` is `pub(crate)`
and `membership_envelope_body` is private, and the only public entry point
(`RrStateMachine::with_snapshot_store`) does real disk I/O per call — a
poor fuzz harness. Adding `fuzz_snapshot_store_decode` /
`fuzz_membership_sidecar_decode` targets first needs rr-raft to expose
pure `&[u8] -> Result<…>` envelope decoders (a one-line visibility change
per module); tracked as a follow-up so this crate stays dependency-light
(rr-core + rr-network only).

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

# All eight targets, 30s each:
for target in fuzz_framed_snapshot_decode fuzz_framed_entry_decode \
              fuzz_apply_outcome_decode fuzz_client_request_proto \
              fuzz_batch_frame_decode \
              fuzz_framed_entry_roundtrip fuzz_apply_outcome_roundtrip \
              fuzz_batch_frame_roundtrip; do
    cargo +nightly fuzz run "$target" -- -max_total_time=30 || exit 1
done
```

## Corpus

Seed corpora are committed under `fuzz/corpus/<target>/` (the cargo-fuzz
default convention, picked up automatically by `cargo fuzz run`). They
were generated through the crates' real encode paths by
`fuzz/examples/gen_corpus.rs` — regenerate after any wire-format change:

```bash
cargo run --example gen_corpus    # from the fuzz/ directory
```

Each target gets a handful of minimal valid frames (including the
legacy/unframed `Ok(None)` inputs for the framed formats) plus rejection
samples (truncated body, future version) so those paths are covered from
the first run. Keep the corpus KB-scale; minimize periodically with
`cargo fuzz cmin <target>`.

On a crash, the failing input lands in
`fuzz/artifacts/<target>/crash-*`. Add it to
`fuzz/corpus/<target>/` to ensure regressions get caught on every
run.

## Why this crate is NOT in the workspace

cargo-fuzz requires a dedicated sub-crate with `panic = "abort"`
profile + nightly toolchain + `libfuzzer-sys` (which links a C++
runtime). The parent workspace pins stable-1.88 and uses
`panic = "unwind"` everywhere; mixing these in one workspace
breaks `cargo build --workspace` on stable. (`prost` is therefore
pinned by version in `Cargo.toml` — the `workspace = true` inheritance
used elsewhere in the repo is unavailable to this crate.)

## CI integration

`.github/workflows/fuzz-nightly.yml` runs all eight targets for 15 minutes
each on a nightly toolchain, daily at 03:23 UTC + `workflow_dispatch`,
with cargo-fuzz pinned to the 0.12 series for reproducibility.
It is informational (not a PR gate): on a crash the reproducer is
minimized with `cargo fuzz tmin`, archived via
`actions/upload-artifact`, and a `nightly-failure` issue is auto-filed
(one open issue per target; repeat failures comment). A PR-level smoke
was deliberately omitted to keep noise down; revisit after the nightly
signal proves stable.
