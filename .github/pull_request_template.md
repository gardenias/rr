<!--
Thanks for sending a pull request! Please fill out the sections below.
-->

## Summary

<!-- 1-3 sentences describing the change and why it's needed. -->

## Related Issues

<!-- Link related issues: "Closes #123", "Related to #456" -->

## Type of Change

- [ ] Bug fix (non-breaking change which fixes an issue)
- [ ] New feature (non-breaking change which adds functionality)
- [ ] Breaking change (fix or feature that would cause existing functionality to not work as expected)
- [ ] Documentation-only change
- [ ] Performance improvement
- [ ] Refactor (no functional change)
- [ ] Test improvement
- [ ] CI/infrastructure

## Testing Performed

<!--
Describe what testing you performed. For code changes, at minimum:
  - `cargo test --workspace` output
  - `cargo clippy --workspace -- -D warnings` output

For protocol/persistence changes:
  - Integration test scenarios run
  - Chaos test results if applicable

For docs changes:
  - Links checked
  - Rendered preview (Docusaurus)
-->

## Checklist

- [ ] Tests pass locally (`cargo test --workspace` + `--features rr-storage/rocks` if relevant)
- [ ] `cargo clippy --workspace -- -D warnings` is clean
- [ ] `cargo fmt --check` is clean
- [ ] Documentation updated (API docs, guide, architecture, or reference — whichever applies)
- [ ] `CHANGELOG.md` updated under `## [Unreleased]`
- [ ] No new `panic!`, `unwrap()`, or `expect()` in library crate production code paths
- [ ] No `println!`/`eprintln!` in library crates (use `tracing`)
- [ ] Commit messages follow [Conventional Commits](https://www.conventionalcommits.org/)
- [ ] PR scope is tight — no unrelated drive-by changes

## Screenshots / Logs (optional)

<!-- Attach screenshots for UI/docs changes, or log snippets for debugging. -->

## Reviewer Notes

<!-- Anything you want reviewers to focus on, or specific concerns? -->
