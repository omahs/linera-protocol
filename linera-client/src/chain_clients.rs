// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::BTreeMap, sync::Arc};

use futures::lock::{Mutex, MutexGuard};
use linera_base::identifiers::ChainId;
use linera_core::client::ChainClient;
use linera_storage::Storage;
use rand::{seq::IteratorRandom, thread_rng};

use crate::{
    chain_listener::ClientContext,
    error::{self, Error},
};

pub type ClientMapInner<P, S> = BTreeMap<ChainId, ChainClient<P, S>>;
pub struct ChainClients<P, S>(pub Arc<Mutex<ClientMapInner<P, S>>>)
where
    S: Storage;

impl<P, S> Clone for ChainClients<P, S>
where
    S: Storage,
{
    fn clone(&self) -> Self {
        ChainClients(self.0.clone())
    }
}

impl<P, S> ChainClients<P, S>
where
    S: Storage,
    P: 'static,
{
    pub async fn client_lock(&self, chain_id: &ChainId) -> Option<ChainClient<P, S>> {
        Some(self.map_lock().await.get(chain_id)?.clone())
    }

    pub async fn try_client_lock(&self, chain_id: &ChainId) -> Result<ChainClient<P, S>, Error> {
        self.client_lock(chain_id)
            .await
            .ok_or(error::Inner::NonexistentChain(*chain_id).into())
    }

    async fn random_client_lock(&self) -> Option<ChainClient<P, S>> {
        self.map_lock()
            .await
            .values()
            .choose(&mut thread_rng())
            .cloned()
    }

    pub async fn try_lock_random_client(&self) -> Result<ChainClient<P, S>, Error> {
        self.random_client_lock()
            .await
            .ok_or(error::Inner::NoChainClientsFound.into())
    }

    pub async fn map_lock(&self) -> MutexGuard<ClientMapInner<P, S>> {
        self.0.lock().await
    }

    pub async fn add_client(&self, client: ChainClient<P, S>) {
        self.map_lock().await.insert(client.chain_id(), client);
    }

    pub async fn request_client(
        &self,
        chain_id: ChainId,
        context: Arc<Mutex<impl ClientContext<ValidatorNodeProvider = P, Storage = S>>>,
    ) -> ChainClient<P, S> {
        let mut guard = self.map_lock().await;
        match guard.get(&chain_id) {
            Some(client) => client.clone(),
            None => {
                let context = context.lock().await;
                let client = context.make_chain_client(chain_id);
                guard.insert(chain_id, client.clone());
                client
            }
        }
    }

    pub async fn from_clients(chains: impl IntoIterator<Item = ChainClient<P, S>>) -> Self {
        let chain_clients = Self(Default::default());
        for chain_client in chains {
            let mut map_guard = chain_clients.map_lock().await;
            map_guard.insert(chain_client.chain_id(), chain_client);
        }
        chain_clients
    }
}
