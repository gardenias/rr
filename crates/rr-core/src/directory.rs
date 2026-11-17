//! Shared contracts for leased node discovery.

use std::pin::Pin;

use async_trait::async_trait;
use futures::Stream;
use serde::{Deserialize, Serialize};

use crate::error::DirectoryError;
use crate::types::NodeId;

pub const DEFAULT_PAGE_SIZE: usize = 50;
pub const MAX_PAGE_SIZE: usize = 200;

/// Explicit node endpoints and placement metadata published to a directory.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeDescriptor {
    pub node_id: NodeId,
    pub client_addr: String,
    pub admin_addr: String,
    pub observability_addr: String,
    pub advertise_addr: String,
    pub environment: String,
    pub region: Option<String>,
    pub zone: Option<String>,
    pub rack: Option<String>,
    pub boot_id: String,
}

impl NodeDescriptor {
    pub fn validate(&self) -> Result<(), DirectoryError> {
        if self.node_id == 0 {
            return Err(DirectoryError::InvalidDescriptor(
                "node_id must be greater than zero".into(),
            ));
        }
        for (field, value) in [
            ("client_addr", &self.client_addr),
            ("admin_addr", &self.admin_addr),
            ("observability_addr", &self.observability_addr),
            ("advertise_addr", &self.advertise_addr),
        ] {
            validate_endpoint(field, value)?;
        }
        for (field, value) in [
            ("environment", &self.environment),
            ("boot_id", &self.boot_id),
        ] {
            if value.trim().is_empty() {
                return Err(DirectoryError::InvalidDescriptor(format!(
                    "{field} cannot be empty"
                )));
            }
        }
        for (field, value) in [
            ("region", &self.region),
            ("zone", &self.zone),
            ("rack", &self.rack),
        ] {
            if value
                .as_deref()
                .is_some_and(|value| value.trim().is_empty())
            {
                return Err(DirectoryError::InvalidDescriptor(format!(
                    "{field} cannot be empty when present"
                )));
            }
        }
        Ok(())
    }
}

fn validate_endpoint(field: &str, value: &str) -> Result<(), DirectoryError> {
    let invalid = |reason: &str| {
        DirectoryError::InvalidDescriptor(format!("invalid {field} {value:?}: {reason}"))
    };
    if value.trim() != value || value.is_empty() || value.chars().any(char::is_whitespace) {
        return Err(invalid("must be a non-empty host:port without whitespace"));
    }
    let (host, port) = value
        .rsplit_once(':')
        .ok_or_else(|| invalid("missing port"))?;
    let host = if host.starts_with('[') && host.ends_with(']') {
        &host[1..host.len() - 1]
    } else if host.contains(':') {
        return Err(invalid("IPv6 hosts must be bracketed"));
    } else {
        host
    };
    if host.is_empty() {
        return Err(invalid("missing host"));
    }
    if host
        .parse::<std::net::IpAddr>()
        .is_ok_and(|address| address.is_unspecified())
    {
        return Err(invalid("wildcard addresses cannot be advertised"));
    }
    match port.parse::<u16>() {
        Ok(0) => Err(invalid("port zero cannot be advertised")),
        Ok(_) => Ok(()),
        Err(_) => Err(invalid("invalid port")),
    }
}

/// One directory value together with its backend ownership metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeDirectoryRecord {
    pub descriptor: NodeDescriptor,
    pub revision: u64,
    pub lease_id: i64,
}

/// A stable page of directory records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeDirectoryPage {
    pub records: Vec<NodeDirectoryRecord>,
    pub next_cursor: Option<String>,
    pub revision: u64,
}

/// An idempotent directory upsert or removal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeDirectoryChange {
    Upsert(Box<NodeDirectoryRecord>),
    Remove { node_id: NodeId, revision: u64 },
}

pub fn directory_page_size(page_size: Option<usize>) -> Result<usize, DirectoryError> {
    let page_size = page_size.unwrap_or(DEFAULT_PAGE_SIZE);
    if page_size == 0 || page_size > MAX_PAGE_SIZE {
        return Err(DirectoryError::InvalidPageSize {
            requested: page_size,
            maximum: MAX_PAGE_SIZE,
        });
    }
    Ok(page_size)
}

#[async_trait]
pub trait NodeDirectoryRegistration: Send + Sync {
    fn record(&self) -> &NodeDirectoryRecord;
    async fn heartbeat(&self) -> Result<(), DirectoryError>;
    async fn revoke(&self) -> Result<(), DirectoryError>;
}

#[async_trait]
pub trait NodeDirectory: Send + Sync {
    async fn get(&self, node_id: NodeId) -> Result<Option<NodeDirectoryRecord>, DirectoryError>;
    async fn list(
        &self,
        cursor: Option<&str>,
        page_size: Option<usize>,
    ) -> Result<NodeDirectoryPage, DirectoryError>;
    async fn watch(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = NodeDirectoryChange> + Send>>, DirectoryError>;
    async fn register(
        &self,
        descriptor: NodeDescriptor,
    ) -> Result<Box<dyn NodeDirectoryRegistration>, DirectoryError>;
}

#[cfg(test)]
mod tests {
    use super::{
        directory_page_size, NodeDescriptor, NodeDirectory, NodeDirectoryRegistration,
        DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE,
    };

    fn descriptor() -> NodeDescriptor {
        NodeDescriptor {
            node_id: 7,
            client_addr: "10.0.0.7:5000".into(),
            admin_addr: "10.0.0.7:5001".into(),
            observability_addr: "10.0.0.7:5002".into(),
            advertise_addr: "10.0.0.7:6000".into(),
            environment: "production".into(),
            region: Some("us-east-1".into()),
            zone: Some("us-east-1a".into()),
            rack: None,
            boot_id: "boot-2026-11-16".into(),
        }
    }

    #[test]
    fn descriptor_rejects_wildcard_advertised_addresses() {
        for (field, value) in [
            ("client_addr", "0.0.0.0:5000"),
            ("admin_addr", "[::]:5001"),
            ("observability_addr", "0.0.0.0:5002"),
            ("advertise_addr", "[::]:6000"),
        ] {
            let mut descriptor = descriptor();
            match field {
                "client_addr" => descriptor.client_addr = value.into(),
                "admin_addr" => descriptor.admin_addr = value.into(),
                "observability_addr" => descriptor.observability_addr = value.into(),
                "advertise_addr" => descriptor.advertise_addr = value.into(),
                _ => unreachable!(),
            }
            assert!(descriptor.validate().is_err(), "accepted {field}={value}");
        }
    }

    #[test]
    fn descriptor_rejects_unknown_json_fields() {
        let mut value = serde_json::to_value(descriptor()).unwrap();
        value["unexpected"] = serde_json::Value::Null;
        assert!(serde_json::from_value::<NodeDescriptor>(value).is_err());
    }

    #[test]
    fn descriptor_round_trips_valid_json() {
        let descriptor = descriptor();
        let encoded = serde_json::to_vec(&descriptor).unwrap();
        let decoded: NodeDescriptor = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(decoded, descriptor);
        assert!(decoded.validate().is_ok());
    }

    #[test]
    fn page_size_defaults_to_50_and_rejects_values_outside_1_to_200() {
        assert_eq!(directory_page_size(None).unwrap(), DEFAULT_PAGE_SIZE);
        assert_eq!(DEFAULT_PAGE_SIZE, 50);
        assert_eq!(MAX_PAGE_SIZE, 200);
        assert_eq!(directory_page_size(Some(1)).unwrap(), 1);
        assert_eq!(directory_page_size(Some(200)).unwrap(), 200);
        assert!(directory_page_size(Some(0)).is_err());
        assert!(directory_page_size(Some(201)).is_err());
    }

    #[test]
    fn directory_traits_are_object_safe() {
        let _: Option<Box<dyn NodeDirectory>> = None;
        let _: Option<Box<dyn NodeDirectoryRegistration>> = None;
    }
}
