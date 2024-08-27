// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

// Some of these items are only used by some tests, but Rust will complain about unused
// items for the tests where they aren't used
#![allow(unused_imports)]

mod mock_application;
mod system_execution_state;

use std::{sync::Arc, thread, vec};

use linera_base::{
    crypto::{BcsSignable, CryptoHash},
    data_types::{Blob, BlockHeight},
    identifiers::{BlobId, BytecodeId, ChainId, MessageId},
};
use linera_views::{
    context::Context,
    views::{View, ViewError},
};
use serde::{Deserialize, Serialize};

pub use self::{
    mock_application::{ExpectedCall, MockApplication, MockApplicationInstance},
    system_execution_state::SystemExecutionState,
};
use crate::{
    execution::ServiceRuntimeEndpoint, ExecutionRequest, ExecutionRuntimeContext,
    ExecutionStateView, QueryContext, ServiceRuntimeRequest, ServiceSyncRuntime,
    TestExecutionRuntimeContext, UserApplicationDescription, UserApplicationId,
};

pub fn create_dummy_user_application_description(index: u64) -> UserApplicationDescription {
    let chain_id = ChainId::root(1);
    let contract_blob_hash = CryptoHash::new(&FakeBlob(String::from("contract")));
    let service_blob_hash = CryptoHash::new(&FakeBlob(String::from("service")));
    UserApplicationDescription {
        bytecode_id: BytecodeId::new(contract_blob_hash, service_blob_hash),
        creator_chain_id: chain_id,
        block_height: BlockHeight(index),
        block_effect_counter: 1,
        required_application_ids: vec![],
        parameters: vec![],
    }
}

#[derive(Deserialize, Serialize)]
pub struct FakeBlob(String);

impl BcsSignable for FakeBlob {}

/// Creates `count` [`MockApplication`]s and registers them in the provided [`ExecutionStateView`].
///
/// Returns an iterator over pairs of [`UserApplicationId`]s and their respective
/// [`MockApplication`]s.
pub async fn register_mock_applications<C>(
    state: &mut ExecutionStateView<C>,
    count: u64,
) -> anyhow::Result<vec::IntoIter<(UserApplicationId, MockApplication)>>
where
    C: Context + Clone + Send + Sync + 'static,
    C::Extra: ExecutionRuntimeContext,
{
    let mock_applications: Vec<_> = create_dummy_user_applications(count)
        .await?
        .into_iter()
        .map(|(id, description)| (id, description, MockApplication::default()))
        .collect();
    let extra = state.context().extra();

    for (id, description, mock_application) in &mock_applications {
        extra
            .user_contracts()
            .insert(*id, Arc::new(mock_application.clone()));
        extra
            .user_services()
            .insert(*id, Arc::new(mock_application.clone()));

        let app_blob = Blob::new_application_description(description.clone())?;
        extra.blobs().insert(app_blob.id(), app_blob);
    }

    Ok(mock_applications
        .into_iter()
        .map(|(id, _, mock_application)| (id, mock_application))
        .collect::<Vec<_>>()
        .into_iter())
}

pub async fn create_dummy_user_applications(
    count: u64,
) -> anyhow::Result<Vec<(UserApplicationId, UserApplicationDescription)>> {
    let mut ids = Vec::with_capacity(count as usize);

    for index in 0..count {
        let description = create_dummy_user_application_description(index);
        ids.push((UserApplicationId::from(&description), description));
    }

    Ok(ids)
}

impl QueryContext {
    /// Spawns a thread running the [`ServiceSyncRuntime`] actor.
    ///
    /// Returns the endpoints to communicate with the actor.
    pub fn spawn_service_runtime_actor(self) -> ServiceRuntimeEndpoint {
        let (execution_state_sender, incoming_execution_requests) =
            futures::channel::mpsc::unbounded();
        let (runtime_request_sender, runtime_request_receiver) = std::sync::mpsc::channel();

        thread::spawn(move || {
            ServiceSyncRuntime::new(execution_state_sender, self).run(runtime_request_receiver)
        });

        ServiceRuntimeEndpoint {
            incoming_execution_requests,
            runtime_request_sender,
        }
    }
}
