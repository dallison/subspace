// Copyright 2024-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

pub mod bitset;
pub mod channel;
pub mod checksum;
pub mod client;
pub mod error;
pub mod message;
pub mod options;
pub mod publisher;
pub mod socket;
pub mod subscriber;

pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/subspace.rs"));
}

pub use client::{ChannelInfo, ChannelStats, Client, Publisher, Subscriber};
pub use error::SubspaceError;
pub use message::Message;
pub use options::{PublisherOptions, SubscriberOptions};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadMode {
    ReadNext,
    ReadNewest,
}
