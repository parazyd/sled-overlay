/* This file is part of sled-overlay
 *
 * Copyright (C) 2023 Dyne.org foundation
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

use std::collections::{BTreeMap, BTreeSet};

use sled::IVec;

/// Struct representing [`SledTreeOverlay`] cache state
#[derive(Clone)]
pub struct SledTreeOverlayState {
    /// The cache is the actual overlayed data represented as a [`BTreeMap`].
    cache: BTreeMap<IVec, IVec>,
    /// In `removed`, we keep track of keys that were removed in the overlay.
    removed: BTreeSet<IVec>,
}

impl SledTreeOverlayState {
    /// Instantiate a new [`SledTreeOverlayState`].
    pub fn new() -> Self {
        Self {
            cache: BTreeMap::new(),
            removed: BTreeSet::new(),
        }
    }
}

impl Default for SledTreeOverlayState {
    fn default() -> Self {
        Self::new()
    }
}

/// An overlay on top of a single [`sled::Tree`] instance
#[derive(Clone)]
pub struct SledTreeOverlay {
    /// The [`sled::Tree`] that is being overlayed.
    tree: sled::Tree,
    /// Current overlay cache state
    state: SledTreeOverlayState,
    /// Checkpointed cache state to revert to
    checkpoint: SledTreeOverlayState,
}

impl SledTreeOverlay {
    /// Instantiate a new [`SledTreeOverlay`] on top of a given [`sled::Tree`].
    pub fn new(tree: &sled::Tree) -> Self {
        Self {
            tree: tree.clone(),
            state: SledTreeOverlayState::new(),
            checkpoint: SledTreeOverlayState::new(),
        }
    }

    /// Returns `true` if the overlay contains a value for a specified key.
    pub fn contains_key(&self, key: &[u8]) -> Result<bool, sled::Error> {
        // First check if the key was removed in the overlay
        if self.state.removed.contains::<IVec>(&key.into()) {
            return Ok(false);
        }

        // Then check the cache and the main tree
        if self.state.cache.contains_key::<IVec>(&key.into()) || self.tree.contains_key(key)? {
            return Ok(true);
        }

        Ok(false)
    }

    /// Returns `true` if the overlay is empty.
    pub fn is_empty(&self) -> bool {
        // Keep a counter of all elements
        let mut counter: i64 = 0;

        // Add existing keys
        counter += self.tree.len() as i64;

        // Add new keys
        counter += self.state.cache.len() as i64;

        // Subtract removed keys
        counter -= self.state.removed.len() as i64;

        counter <= 0
    }

    /// Returns last value from the overlay or `None` if its empty,
    /// based on the `Ord` implementation for `Vec<u8>`.
    pub fn last(&self) -> Result<Option<(IVec, IVec)>, sled::Error> {
        // First check if cache has keys
        if let Some(record) = self.state.cache.last_key_value() {
            return Ok(Some((record.0.clone(), record.1.clone())));
        }

        // Then the main tree
        self.tree.last()
    }

    /// Retrieve a value from the overlay if it exists.
    pub fn get(&self, key: &[u8]) -> Result<Option<IVec>, sled::Error> {
        // First check if the key was removed in the overlay
        if self.state.removed.contains::<IVec>(&key.into()) {
            return Ok(None);
        }

        // Then check the cache
        if let Some(v) = self.state.cache.get::<IVec>(&key.into()) {
            return Ok(Some(v.clone()));
        }

        // And finally the main tree
        self.tree.get(key)
    }

    /// Insert a key to a new value, returning the last value if it was set.
    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<Option<IVec>, sled::Error> {
        // Insert the value into the cache. We then optionally add the previous value
        // into `prev`.
        let mut prev: Option<IVec> = self.state.cache.insert(key.into(), value.into());

        // In case this key was previously removed from the cache, we have to
        // delete it from the `removed` set.
        if self.state.removed.contains::<IVec>(&key.into()) {
            self.state.removed.remove(key);
            // And in that case, a previous value isn't supposed to exist
            return Ok(None);
        }

        // If cache didn't contain this key previously, and it wasn't removed
        // either, then check if it's in the main tree.
        if prev.is_none() {
            prev = self.tree.get::<IVec>(key.into())?;
        }

        Ok(prev)
    }

    /// Delete a value, if it exists, returning the old value.
    pub fn remove(&mut self, key: &[u8]) -> Result<Option<IVec>, sled::Error> {
        // If it was previously removed, we can just return None
        if self.state.removed.contains::<IVec>(&key.into()) {
            return Ok(None);
        }

        // Attempt to remove from cache, and if it wasn't in the cache before,
        // we have to get the previous value from the sled tree:
        let mut prev: Option<IVec> = self.state.cache.remove::<IVec>(&key.into());
        if prev.is_none() {
            prev = self.tree.get(key)?;
        }

        // Previous value must existed
        if prev.is_none() {
            return Err(sled::Error::CollectionNotFound(key.into()));
        }

        // Mark the key as removed
        self.state.removed.insert(key.into());

        Ok(prev)
    }

    /// Aggregate all the current overlay changes into a [`sled::Batch`] ready for
    /// further operation. If there are no changes, return `None`.
    pub fn aggregate(&self) -> Option<sled::Batch> {
        if self.state.cache.is_empty() && self.state.removed.is_empty() {
            return None;
        }

        let mut batch = sled::Batch::default();

        // This kind of first-insert-then-remove operation should be fine
        // provided it's handled correctly in the above functions.
        for (k, v) in self.state.cache.iter() {
            batch.insert(k, v);
        }

        for k in self.state.removed.iter() {
            batch.remove(k);
        }

        Some(batch)
    }

    /// Checkpoint current cache state so we can revert to it, if needed.
    pub fn checkpoint(&mut self) {
        self.checkpoint = self.state.clone();
    }

    /// Revert to current cache state checkpoint.
    pub fn revert_to_checkpoint(&mut self) {
        self.state = self.checkpoint.clone();
    }
}
