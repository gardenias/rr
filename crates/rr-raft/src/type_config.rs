//! TypeConfig for openraft via `declare_raft_types!`.

use std::io::Cursor;
use std::ops::Deref;

use openraft::BasicNode;
use openraft::TokioRuntime;

/// Application request payload carried by OpenRaft log entries.
#[derive(Debug, Clone, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct RrAppData(pub Vec<u8>);

impl std::fmt::Display for RrAppData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} bytes", self.0.len())
    }
}

impl From<Vec<u8>> for RrAppData {
    fn from(value: Vec<u8>) -> Self {
        Self(value)
    }
}

impl From<RrAppData> for Vec<u8> {
    fn from(value: RrAppData) -> Self {
        value.0
    }
}

impl AsRef<[u8]> for RrAppData {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl Deref for RrAppData {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.0.as_slice()
    }
}

/// Application response payload returned from OpenRaft state machine apply.
#[derive(Debug, Clone, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct RrAppResponse(pub Vec<u8>);

impl From<Vec<u8>> for RrAppResponse {
    fn from(value: Vec<u8>) -> Self {
        Self(value)
    }
}

impl From<RrAppResponse> for Vec<u8> {
    fn from(value: RrAppResponse) -> Self {
        value.0
    }
}

impl AsRef<[u8]> for RrAppResponse {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

openraft::declare_raft_types!(
    /// The type configuration for the rr Raft implementation.
    pub RrTypeConfig:
        D            = RrAppData,
        R            = RrAppResponse,
        NodeId       = u64,
        Node         = BasicNode,
        Term         = u64,
        LeaderId     = openraft::impls::leader_id_adv::LeaderId<Self::Term, Self::NodeId>,
        Vote         = openraft::impls::Vote<Self::LeaderId>,
        Entry        = openraft::Entry<<Self::LeaderId as openraft::vote::RaftLeaderId>::Committed, Self::D, Self::NodeId, Self::Node>,
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
