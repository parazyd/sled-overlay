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

//! sled-overlay is a small crate that serves as tooling to have intermediate
//! writes to some sled database. With it, we're able to write data into an
//! in-memory cache, and only flush to the actual sled trees after we decide
//! that everything in some batch was executed correctly.
//! This gives some minimal infrastructure to be able to transparently have
//! rollback-like functionality.

use std::collections::{BTreeMap, BTreeSet};

use sled::transaction::{ConflictableTransactionError, TransactionError};
use sled::{IVec, Transactional};

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

/// An overlay on top of an entire [`sled::Db`] which can span multiple trees
pub struct SledDbOverlay {
    /// The [`sled::Db`] that is being overlayed.
    db: sled::Db,
    /// Existing trees in `db` at the time of instantiation, so we can track newly opened trees.
    initial_tree_names: Vec<IVec>,
    /// New trees that have been opened, but didn't exist in `db` before.
    new_tree_names: Vec<IVec>,
    /// Pointers to sled trees that we have opened.
    trees: BTreeMap<IVec, sled::Tree>,
    /// Pointers to [`SledTreeOverlay`] instances that have been created.
    caches: BTreeMap<IVec, SledTreeOverlay>,
}

impl SledDbOverlay {
    /// Instantiate a new [`SledDbOverlay`] on top of a given [`sled::Db`].
    pub fn new(db: &sled::Db) -> Self {
        Self {
            db: db.clone(),
            initial_tree_names: db.tree_names(),
            new_tree_names: vec![],
            trees: BTreeMap::new(),
            caches: BTreeMap::new(),
        }
    }

    /// Create a new [`SledTreeOverlay`] on top of a given `tree_name`.
    /// This function will also open a new tree inside `db` regardless of if it has
    /// existed before, so for convenience, we also provide [`SledDbOverlay::purge_new_trees`]
    /// in case we decide we don't want to write the batches, and drop the new trees.
    pub fn open_tree(&mut self, tree_name: &str) -> Result<(), sled::Error> {
        let tree_key: IVec = tree_name.into();

        if self.trees.contains_key(&tree_key) {
            // We have already opened this tree.
            return Ok(());
        }

        // Open this tree in sled. In case it hasn't existed before, we also need
        // to track it in `self.new_tree_names`.
        let tree = self.db.open_tree(&tree_key)?;
        let cache = SledTreeOverlay::new(&tree);

        if !self.initial_tree_names.contains(&tree_key) {
            self.new_tree_names.push(tree_key.clone());
        }

        self.trees.insert(tree_key.clone(), tree);
        self.caches.insert(tree_key, cache);

        Ok(())
    }

    /// Drop newly created trees from the sled database. This is a convenience
    /// function that should be used when we decide that we don't want to apply
    /// any cache changes, and we want to revert back to the initial state.
    pub fn purge_new_trees(&self) -> Result<(), sled::Error> {
        for i in &self.new_tree_names {
            self.db.drop_tree(i)?;
        }

        Ok(())
    }

    /// Fetch the cache for a given tree.
    fn get_cache(&self, tree_key: &IVec) -> Result<&SledTreeOverlay, sled::Error> {
        if let Some(v) = self.caches.get(tree_key) {
            return Ok(v);
        }

        Err(sled::Error::CollectionNotFound(tree_key.into()))
    }

    /// Fetch a mutable reference to the cache for a given tree.
    fn get_cache_mut(&mut self, tree_key: &IVec) -> Result<&mut SledTreeOverlay, sled::Error> {
        if let Some(v) = self.caches.get_mut(tree_key) {
            return Ok(v);
        }
        Err(sled::Error::CollectionNotFound(tree_key.clone()))
    }

    /// Returns `true` if the overlay contains a value for a specified key in the specified
    /// tree cache.
    pub fn contains_key(&self, tree_key: &str, key: &[u8]) -> Result<bool, sled::Error> {
        let cache = self.get_cache(&tree_key.into())?;
        cache.contains_key(key)
    }

    /// Retrieve a value from the overlay if it exists in the specified tree cache.
    pub fn get(&self, tree_key: &str, key: &[u8]) -> Result<Option<IVec>, sled::Error> {
        let cache = self.get_cache(&tree_key.into())?;
        cache.get(key)
    }

    /// Insert a key to a new value in the specified tree cache, returning the last value
    /// if it was set.
    pub fn insert(
        &mut self,
        tree_key: &str,
        key: &[u8],
        value: &[u8],
    ) -> Result<Option<IVec>, sled::Error> {
        let cache = self.get_cache_mut(&tree_key.into())?;
        cache.insert(key, value)
    }

    /// Delete a value in the specified tree cache, returning the old value if it existed.
    pub fn remove(&mut self, tree_key: &str, key: &[u8]) -> Result<Option<IVec>, sled::Error> {
        let cache = self.get_cache_mut(&tree_key.into())?;
        cache.remove(key)
    }

    /// Aggregate all the current overlay changes into a [`sled::Batch`] ready for
    /// further operation. Corresponding ['sled::Tree'] is also returned, enforcing
    /// the order of execution. If there are no changes, both vectors will be empty.
    pub fn aggregate(&self) -> Result<(Vec<sled::Tree>, Vec<sled::Batch>), sled::Error> {
        let mut trees = vec![];
        let mut batches = vec![];

        for (key, tree) in &self.trees {
            let cache = self.get_cache(key)?;
            if let Some(batch) = cache.aggregate() {
                trees.push(tree.clone());
                batches.push(batch);
            }
        }

        Ok((trees, batches))
    }

    /// Atomically apply all batches on all trees as a transaction.
    /// This function **does not** perform a db flush. This should be done externally,
    /// since then there is a choice to perform either blocking or async IO.
    pub fn apply(&self) -> Result<(), TransactionError<sled::Error>> {
        let (trees, batches) = self.aggregate()?;
        if trees.is_empty() {
            return Ok(());
        }

        // Perform an atomic transaction over all the collected trees and
        // apply the batches.
        trees.transaction(|trees| {
            for (index, tree) in trees.iter().enumerate() {
                tree.apply_batch(&batches[index])?;
            }

            Ok::<(), ConflictableTransactionError<sled::Error>>(())
        })?;

        Ok(())
    }
}
