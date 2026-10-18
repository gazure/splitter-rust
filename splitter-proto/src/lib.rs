//! Generated tonic/prost bindings for the splitter data plane.
//!
//! Module layout mirrors proto package paths:
//! - `atoms::splitter` — consumer/cluster/model types and `ConsumerServiceClient`.
//! - `atoms::splitter::lib::service::service` — session sub-protocol (note the
//!   package in session.proto is `atoms.splitter.lib.service.service`).
//! - `atoms::splitter::lib::service::location` — Location/Instance types.

#![allow(rustdoc::invalid_rust_codeblocks)]

// Prost and tonic generate the contents of this module.
#[allow(clippy::all, clippy::pedantic)]
pub mod atoms {
    pub mod splitter {
        tonic::include_proto!("atoms.splitter");

        pub mod lib {
            pub mod service {
                pub mod service {
                    tonic::include_proto!("atoms.splitter.lib.service.service");
                }
                pub mod location {
                    tonic::include_proto!("atoms.splitter.lib.service.location");
                }
            }
        }
    }
}

pub use atoms::{
    splitter as pb,
    splitter::lib::service::{location as location_pb, service as session_pb},
};
