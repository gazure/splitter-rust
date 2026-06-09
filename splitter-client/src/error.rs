//! Client error types and gRPC status translation.
//!
//! Mirrors `pkg/model/proxy.go` (ownership errors) and `pkg/model/errors.go`
//! (domain-to-grpc mapping):
//!
//! - `NotOwned` ↔ `Code::OutOfRange`
//! - `Draining` ↔ `Code::Aborted`
//! - `Code::Unavailable` is treated as an ownership-class error for retry purposes
//!   (matches `proxy.go:43`).

use thiserror::Error;
use tonic::{Code, Status};

pub type Result<T> = std::result::Result<T, ClientError>;

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("not owned")]
    NotOwned,
    #[error("draining")]
    Draining,
    #[error("no resolution")]
    NoResolution,
    #[error("grant revoked")]
    Revoked,
    #[error("grant expired")]
    Expired,
    #[error("session closed: {0}")]
    SessionClosed(String),
    #[error("cancelled")]
    Cancelled,
    #[error("invalid message: {0}")]
    InvalidMessage(String),
    #[error("transport: {0}")]
    Transport(#[from] Status),
    #[error("transport setup: {0}")]
    TransportSetup(#[from] tonic::transport::Error),
}

impl ClientError {
    /// Translate a domain error into a gRPC status for forwarding.
    /// Mirrors `proxy.go:71`.
    pub fn into_grpc(self) -> Status {
        match self {
            ClientError::NotOwned => Status::out_of_range("not owned"),
            ClientError::Draining => Status::aborted("draining"),
            ClientError::NoResolution => Status::unavailable("no resolution"),
            ClientError::Revoked => Status::aborted("revoked"),
            ClientError::Expired => Status::deadline_exceeded("expired"),
            ClientError::SessionClosed(msg) => Status::unavailable(msg),
            ClientError::Cancelled => Status::cancelled("cancelled"),
            ClientError::InvalidMessage(m) => Status::invalid_argument(m),
            ClientError::Transport(s) => s,
            ClientError::TransportSetup(e) => Status::unavailable(e.to_string()),
        }
    }

    /// Recover a domain error from a gRPC status returned by a peer.
    /// Mirrors `proxy.go:85`.
    pub fn from_grpc(status: Status) -> Self {
        match status.code() {
            Code::OutOfRange => ClientError::NotOwned,
            Code::Aborted => ClientError::Draining,
            _ => ClientError::Transport(status),
        }
    }

    /// True when the error is an ownership-class failure that's worth retrying.
    /// Mirrors `IsOwnershipError` (`proxy.go:43`).
    pub fn is_ownership(&self) -> bool {
        match self {
            ClientError::NotOwned | ClientError::Draining => true,
            ClientError::Transport(s) => s.code() == Code::Unavailable,
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn not_owned_roundtrips_via_grpc() {
        let status = ClientError::NotOwned.into_grpc();
        assert_eq!(status.code(), Code::OutOfRange);
        assert!(matches!(
            ClientError::from_grpc(status),
            ClientError::NotOwned
        ));
    }

    #[test]
    fn draining_roundtrips_via_grpc() {
        let status = ClientError::Draining.into_grpc();
        assert_eq!(status.code(), Code::Aborted);
        assert!(matches!(
            ClientError::from_grpc(status),
            ClientError::Draining
        ));
    }

    #[test]
    fn unavailable_is_ownership_but_preserves_status() {
        let status = Status::unavailable("peer down");
        let err = ClientError::from_grpc(status);
        assert!(err.is_ownership());
        assert!(matches!(err, ClientError::Transport(_)));
    }

    #[test]
    fn other_codes_are_not_ownership() {
        let err = ClientError::from_grpc(Status::internal("boom"));
        assert!(!err.is_ownership());
    }
}
