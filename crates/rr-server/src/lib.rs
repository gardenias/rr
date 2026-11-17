//! rr-server — Main server and orchestration
//!
//! Part of the `rr` distributed framework.

pub mod admin;
pub mod boot_journal;
pub mod builtin_service;
pub mod cli;
pub mod config;
pub mod config_file;
pub mod dispatch;
pub mod formation;
pub mod health;
pub mod identity;
pub mod lookup;
pub mod node;
pub mod resources;
pub mod runtime;

pub use config::NodeConfig;
pub use dispatch::ClientDispatchImpl;
pub use formation::{FormationConfig, FormationMode, GroupFormationOutcome};
pub use lookup::RaftGroupLookupImpl;
pub use node::RrNode;
