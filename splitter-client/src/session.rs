//! Session sub-protocol driver.
//!
//! Wraps the `atoms.splitter.lib.service.service.Message` sub-protocol that is
//! embedded inside every `JoinMessage` stream (see `consumer.proto:100-106`).
//! Exposes helpers to build outbound session frames and to classify inbound
//! ones.
//!
//! The session lifecycle is:
//!   1. client sends `Establish { client, id }`
//!   2. server responds `Established { ttl, server }` — record ttl
//!   3. client periodically sends `Heartbeat { now }`
//!   4. server responds `HeartbeatAck { ttl }` — refresh ttl
//!   5. either side sends `Closed { error }` to terminate
//!
//! Heartbeats are sent at `ttl / 3` to give the server ample opportunity to
//! respond before the lease would lapse.

use std::time::{Duration, SystemTime};

use splitter_proto::location_pb;
use splitter_proto::pb::{join_message, JoinMessage};
use splitter_proto::session_pb;

use crate::wrap::{now_timestamp, timestamp_to_system_time};

/// Events extracted from an inbound session frame.
pub enum SessionEvent {
    Established { ttl: SystemTime },
    HeartbeatAck { ttl: SystemTime },
    Closed { error: String },
}

pub fn classify(msg: &session_pb::Message) -> Option<SessionEvent> {
    use session_pb::message::Request;
    match msg.request.as_ref()? {
        Request::Established(e) => e.ttl.as_ref().map(|t| SessionEvent::Established {
            ttl: timestamp_to_system_time(t),
        }),
        Request::Ack(a) => a.ttl.as_ref().map(|t| SessionEvent::HeartbeatAck {
            ttl: timestamp_to_system_time(t),
        }),
        Request::Closed(c) => Some(SessionEvent::Closed {
            error: c.error.clone(),
        }),
        Request::Establish(_) | Request::Heartbeat(_) => None,
    }
}

pub fn establish_frame(client: location_pb::Instance, session_id: String) -> JoinMessage {
    wrap(session_pb::message::Request::Establish(
        session_pb::message::Establish {
            client: Some(client),
            id: session_id,
        },
    ))
}

pub fn heartbeat_frame() -> JoinMessage {
    wrap(session_pb::message::Request::Heartbeat(
        session_pb::message::Heartbeat {
            now: Some(now_timestamp()),
        },
    ))
}

pub fn closed_frame(error: impl Into<String>) -> JoinMessage {
    wrap(session_pb::message::Request::Closed(
        session_pb::message::Closed {
            error: error.into(),
        },
    ))
}

fn wrap(req: session_pb::message::Request) -> JoinMessage {
    JoinMessage {
        msg: Some(join_message::Msg::Session(session_pb::Message {
            request: Some(req),
        })),
    }
}

/// Given a lease expiration, compute when to next send a heartbeat.
/// Returns a duration from `now`. Bounded to at least 250ms so the loop
/// doesn't spin on a stale ttl, and at most 30s as a sanity ceiling.
pub fn heartbeat_delay(ttl: SystemTime, now: SystemTime) -> Duration {
    let remaining = ttl.duration_since(now).unwrap_or(Duration::ZERO);
    let third = remaining / 3;
    third.clamp(Duration::from_millis(250), Duration::from_secs(30))
}
