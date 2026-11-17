//! Versioned protocol compatibility rules.

/// Raft wire protocol version advertised by one replica.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ProtocolVersion {
    pub major: u32,
    pub minor: u32,
    pub min_peer_minor: u32,
}

/// Return whether two replicas can communicate using their advertised Raft wire versions.
pub fn raft_pair_compatible(a: ProtocolVersion, b: ProtocolVersion) -> bool {
    fn known(version: ProtocolVersion) -> bool {
        version.major > 0 && version.min_peer_minor <= version.minor
    }

    known(a)
        && known(b)
        && a.major == b.major
        && a.minor >= b.min_peer_minor
        && b.minor >= a.min_peer_minor
}

#[cfg(test)]
mod tests {
    use super::{raft_pair_compatible, ProtocolVersion};

    fn version(major: u32, minor: u32, min_peer_minor: u32) -> ProtocolVersion {
        ProtocolVersion {
            major,
            minor,
            min_peer_minor,
        }
    }

    #[test]
    fn raft_compatibility_enforces_both_minor_requirements() {
        let older = version(1, 3, 2);
        let newer = version(1, 5, 4);
        assert!(!raft_pair_compatible(older, newer));
        assert!(!raft_pair_compatible(newer, older));

        let mutually_compatible = version(1, 5, 3);
        assert!(raft_pair_compatible(older, mutually_compatible));
        assert!(raft_pair_compatible(mutually_compatible, older));
    }

    #[test]
    fn raft_compatibility_rejects_major_mismatch() {
        assert!(!raft_pair_compatible(version(1, 4, 2), version(2, 4, 2)));
    }

    #[test]
    fn raft_compatibility_rejects_unknown_or_inconsistent_versions() {
        for invalid in [version(0, 0, 0), version(0, 1, 0), version(1, 2, 3)] {
            assert!(!raft_pair_compatible(invalid, version(1, 3, 1)));
            assert!(!raft_pair_compatible(version(1, 3, 1), invalid));
        }
    }
}
