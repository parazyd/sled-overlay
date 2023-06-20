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

/// An overlay on top of a single [`sled::Tree`] instance
pub struct SledTreeOverlay {
    /// The [`sled::Tree`] that is being overlayed.
    tree: sled::Tree,
    /// The cache is the actual overlayed data represented as a [`BTreeMap`].
    cache: BTreeMap<IVec, IVec>,
    /// In `removed`, we keep track of keys that were removed in the overlay.
    removed: BTreeSet<IVec>,
}

impl SledTreeOverlay {
    /// Instantiate a new [`SledTreeOverlay`] on top of a given [`sled::Tree`].
    pub fn new(tree: &sled::Tree) -> Self {
        Self {
            tree: tree.clone(),
            cache: BTreeMap::new(),
            removed: BTreeSet::new(),
        }
    }

    /// Returns `true` if the overlay contains a value for a specified key.
    pub fn contains_key(&self, key: &[u8]) -> Result<bool, sled::Error> {
        // First check if the key was removed in the overlay
        if self.removed.contains::<IVec>(&key.into()) {
            return Ok(false);
        }

        // Then check the cache and the main tree
        if self.cache.contains_key::<IVec>(&key.into()) || self.tree.contains_key(key)? {
            return Ok(true);
        }

        Ok(false)
    }

    /// Retrieve a value from the overlay if it exists.
    pub fn get(&self, key: &[u8]) -> Result<Option<IVec>, sled::Error> {
        // First check if the key was removed in the overlay
        if self.removed.contains::<IVec>(&key.into()) {
            return Ok(None);
        }

        // Then check the cache
        if let Some(v) = self.cache.get::<IVec>(&key.into()) {
            return Ok(Some(v.clone()));
        }

        // And finally the main tree
        self.tree.get(key)
    }

    /// Insert a key to a new value, returning the last value if it was set.
    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<Option<IVec>, sled::Error> {
        // Insert the value into the cache. We then optionally add the previous value
        // into `prev`.
        let mut prev: Option<IVec> = self.cache.insert(key.into(), value.into());

        // In case this key was previously removed from the cache, we have to
        // delete it from the `removed` set.
        if self.removed.contains::<IVec>(&key.into()) {
            self.removed.remove(key);
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

    /// Delete a value, returning the old value if it existed.
    pub fn remove(&mut self, key: &[u8]) -> Result<Option<IVec>, sled::Error> {
        // If it was previously removed, we can just return None
        if self.removed.contains::<IVec>(&key.into()) {
            return Ok(None);
        }

        // Mark the key as removed
        self.removed.insert(key.into());

        // Attempt to remove from cache, and if it wasn't in the cache before,
        // we have to get the previous value from the sled tree:
        let mut prev: Option<IVec> = self.cache.remove::<IVec>(&key.into());
        if prev.is_none() {
            prev = self.tree.get(key)?;
        }

        Ok(prev)
    }

    /// Aggregate all the current overlay changes into a [`sled::Batch`] ready for
    /// further operation. If there are no changes, return `None`.
    pub fn aggregate(&self) -> Option<sled::Batch> {
        if self.cache.is_empty() && self.removed.is_empty() {
            return None;
        }

        let mut batch = sled::Batch::default();

        // This kind of first-insert-then-remove operation should be fine
        // provided it's handled correctly in the above functions.
        for (k, v) in self.cache.iter() {
            batch.insert(k, v);
        }

        for k in self.removed.iter() {
            batch.remove(k);
        }

        Some(batch)
    }
}
