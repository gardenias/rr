//! rr-server — Main server and orchestration
//!
//! Part of the `rr` distributed framework.

pub mod config;
pub mod dispatch;
pub mod lookup;
pub mod node;

pub use config::NodeConfig;
pub use dispatch::ClientDispatchImpl;
pub use lookup::RaftGroupLookupImpl;
pub use node::RrNode;
