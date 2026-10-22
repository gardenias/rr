# proptest-regressions/

Failing-seed files written by proptest's `failure_persistence` (R-6 review
item L7a, 2026-10-21). Each `*.txt` here is the regression corpus for the
same-named proptest source file of this crate: on every run, proptest
replays the persisted seeds FIRST, so a committed failure reproduces
verbatim across runs and machines.

Workflow:

1. A proptest fails locally or in CI → proptest writes/updates the seed file
   in this directory and prints its path.
2. **Commit the seed file together with the fix.** The bug stays pinned as a
   regression case forever; deleting the file silently drops the coverage.
3. An empty directory (only this README) means "no known failing seeds" —
   the normal steady state.

The `.gitignore` at the repository root deliberately does NOT exclude this
directory — the whole point is cross-run, cross-machine reproducibility.
