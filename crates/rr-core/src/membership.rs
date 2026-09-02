//! Membership-change safety policy shared by client and server.
//!
//! openraft's joint consensus keeps membership changes *correct* (no
//! split-brain) but says nothing about *availability*: shrinking a group
//! to a single voter leaves a SPOF, and shrinking to zero tears the
//! group down. That policy is rr's own, and it must live here — not in
//! a client binary — so every enforcement point (AdminService handler,
//! `rr-admin` decommission workflow) evaluates the same invariant.
//!
//! Enforcement point: `AdminServiceImpl::change_membership`
//! (`rr-server`). Design rationale: ADR A-0012.

/// Smallest voter-set size a `change_membership` request may leave behind.
///
/// `< 2` voters means single-point-of-failure or a torn-down group, both
/// of which require an explicit operator action (`force` on the RPC, or
/// `node-reset` for teardown). The value is deliberately small — the
/// guard protects availability, not delta size (see ADR A-0012, Option D).
pub const MIN_VOTERS_AFTER_REMOVAL: usize = 2;

/// Guard predicate: `true` iff a target voter set of `target_voters`
/// satisfies the minimum-voter invariant without a `force` override.
///
/// Note: an empty target (`target_voters == 0`) is unsafe under this
/// predicate, but callers must reject it *unconditionally* (even with
/// `force`) as an invalid argument — n→0 is teardown, not a membership
/// change, and is routed to `node-reset` instead.
pub fn membership_change_is_safe(target_voters: usize) -> bool {
    target_voters >= MIN_VOTERS_AFTER_REMOVAL
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Truth table from ADR A-0012 §4: the guard constrains only the
    /// *target* voter-set size, never the delta. `force` handling is
    /// decided by the caller (`!safe && !force → reject`); here we pin
    /// the predicate itself.
    #[test]
    fn guard_predicate_truth_table() {
        // Shrinks landing at exactly the minimum are allowed (3→2).
        assert!(membership_change_is_safe(2));
        // Shrinks below the minimum are rejected (3→1, 2→1, 1→1).
        assert!(!membership_change_is_safe(1));
        // n→0 is unsafe; the handler additionally refuses to let
        // `force` bypass the empty-set check.
        assert!(!membership_change_is_safe(0));
        // Growth is never constrained (2→3, 3→5, …).
        for target in [3usize, 4, 5, 9] {
            assert!(
                membership_change_is_safe(target),
                "expansion to {target} voters must not be guarded"
            );
        }
    }

    #[test]
    fn min_voters_constant_is_two() {
        assert_eq!(MIN_VOTERS_AFTER_REMOVAL, 2);
    }
}
