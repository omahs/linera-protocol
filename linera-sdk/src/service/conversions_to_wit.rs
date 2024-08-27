// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Conversions from types declared in [`linera-sdk`] to types generated by [`wit-bindgen`].

use linera_base::{
    crypto::CryptoHash,
    data_types::BlockHeight,
    identifiers::{ApplicationId, ChainId, Owner},
};

use super::wit::service_system_api as wit_system_api;

impl From<log::Level> for wit_system_api::LogLevel {
    fn from(level: log::Level) -> Self {
        match level {
            log::Level::Trace => wit_system_api::LogLevel::Trace,
            log::Level::Debug => wit_system_api::LogLevel::Debug,
            log::Level::Info => wit_system_api::LogLevel::Info,
            log::Level::Warn => wit_system_api::LogLevel::Warn,
            log::Level::Error => wit_system_api::LogLevel::Error,
        }
    }
}

impl From<CryptoHash> for wit_system_api::CryptoHash {
    fn from(hash_value: CryptoHash) -> Self {
        let parts = <[u64; 4]>::from(hash_value);

        wit_system_api::CryptoHash {
            part1: parts[0],
            part2: parts[1],
            part3: parts[2],
            part4: parts[3],
        }
    }
}

impl From<Owner> for wit_system_api::Owner {
    fn from(owner: Owner) -> Self {
        wit_system_api::Owner {
            inner0: owner.0.into(),
        }
    }
}

impl From<BlockHeight> for wit_system_api::BlockHeight {
    fn from(block_height: BlockHeight) -> Self {
        wit_system_api::BlockHeight {
            inner0: block_height.0,
        }
    }
}

impl From<ChainId> for wit_system_api::ChainId {
    fn from(chain_id: ChainId) -> Self {
        wit_system_api::ChainId {
            inner0: chain_id.0.into(),
        }
    }
}

impl From<ApplicationId> for wit_system_api::ApplicationId {
    fn from(application_id: ApplicationId) -> Self {
        wit_system_api::ApplicationId {
            application_description_hash: application_id.application_description_hash.into(),
        }
    }
}
