//! rr-server — Main server and orchestration
//!
//! Part of the `rr` distributed framework.

pub mod config;
pub mod node;

pub use config::NodeConfig;
pub use node::RrNode;
