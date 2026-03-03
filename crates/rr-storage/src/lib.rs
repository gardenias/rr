//! rr-storage — In-memory StorageEngine implementation for the rr framework.

pub mod mem;
pub use mem::MemStorage;

#[cfg(feature = "rocks")]
pub mod rocks;
