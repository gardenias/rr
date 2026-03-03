//! KV Store example — part of the `rr` distributed framework.
//!
//! Provides a simple key-value store built on top of the `rr` framework.

pub mod service;
pub mod state_machine;
pub mod types;

pub use service::KvService;
pub use state_machine::KvStateMachine;
pub use types::{KvRequest, KvResponse};
