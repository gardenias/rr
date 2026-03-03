//! TypeConfig for openraft via `declare_raft_types!`.

use std::io::Cursor;

use openraft::BasicNode;
use openraft::TokioRuntime;

openraft::declare_raft_types!(
    /// The type configuration for the rr Raft implementation.
    pub RrTypeConfig:
        D            = Vec<u8>,
        R            = Vec<u8>,
        NodeId       = u64,
        Node         = BasicNode,
        Entry        = openraft::Entry<RrTypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = TokioRuntime,
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_type_config_exists() {
        // Verify RrTypeConfig can be named and implements the expected traits.
        let _cfg: std::marker::PhantomData<RrTypeConfig> = std::marker::PhantomData;
    }
}
