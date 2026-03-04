# Issues — rr-phase2-production

## [2026-03-03] Known Issues to Fix
- RaftNode hardcodes NoopNetworkFactory (T1 fixes)
- No production RaftGroupLookupImpl (T2 fixes)
- No production ClientDispatchImpl (T3 fixes)
- SnapshotResponse proto missing `bytes payload` field (T4 fixes)
- RrLogStorage is in-memory only — no persistence (T9 fixes)
- Vote storage in-memory — split-brain risk after restart (T9 fixes)
- RrNode config watch loop missing (T14 fixes)
- openraft membership APIs not exposed (T15 fixes)
- RoutingTableChange missing node/membership events (T13 fixes)
