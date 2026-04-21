// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

use subspace_client::{Publisher, Subscriber};

pub const RPC_REQUEST_SLOT_SIZE: i32 = 128;
pub const RPC_RESPONSE_SLOT_SIZE: i32 = 128;
pub const RPC_REQUEST_NUM_SLOTS: i32 = 100;
pub const RPC_RESPONSE_NUM_SLOTS: i32 = 100;

pub const DEFAULT_METHOD_SLOT_SIZE: i32 = 256;
pub const DEFAULT_METHOD_NUM_SLOTS: i32 = 100;

pub const CANCEL_CHANNEL_SLOT_SIZE: i32 = 64;
pub const CANCEL_CHANNEL_NUM_SLOTS: i32 = 8;

#[derive(Debug, Clone)]
pub struct MethodOptions {
    pub slot_size: i32,
    pub num_slots: i32,
    pub id: i32,
}

impl Default for MethodOptions {
    fn default() -> Self {
        Self {
            slot_size: DEFAULT_METHOD_SLOT_SIZE,
            num_slots: DEFAULT_METHOD_NUM_SLOTS,
            id: -1,
        }
    }
}

#[allow(dead_code)]
pub(crate) struct MethodInfo {
    pub name: String,
    pub id: i32,
    pub request_type: String,
    pub response_type: String,
    pub slot_size: i32,
    pub num_slots: i32,
    pub request_publisher: Publisher,
    pub response_subscriber: Subscriber,
    pub cancel_publisher: Option<Publisher>,
}

pub(crate) fn service_request_channel(service: &str) -> String {
    format!("/rpc/{}/request", service)
}

pub(crate) fn service_response_channel(service: &str) -> String {
    format!("/rpc/{}/response", service)
}
