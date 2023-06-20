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

use std::collections::BTreeMap;

use sled::transaction::{ConflictableTransactionError, TransactionError};
use sled::{IVec, Transactional};

use crate::SledTreeOverlay;

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
    pub fn open_tree(&mut self, tree_name: &[u8]) -> Result<(), sled::Error> {
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
    pub fn contains_key(&self, tree_key: &[u8], key: &[u8]) -> Result<bool, sled::Error> {
        let cache = self.get_cache(&tree_key.into())?;
        cache.contains_key(key)
    }

    /// Retrieve a value from the overlay if it exists in the specified tree cache.
    pub fn get(&self, tree_key: &[u8], key: &[u8]) -> Result<Option<IVec>, sled::Error> {
        let cache = self.get_cache(&tree_key.into())?;
        cache.get(key)
    }

    /// Insert a key to a new value in the specified tree cache, returning the last value
    /// if it was set.
    pub fn insert(
        &mut self,
        tree_key: &[u8],
        key: &[u8],
        value: &[u8],
    ) -> Result<Option<IVec>, sled::Error> {
        let cache = self.get_cache_mut(&tree_key.into())?;
        cache.insert(key, value)
    }

    /// Delete a value in the specified tree cache, returning the old value if it existed.
    pub fn remove(&mut self, tree_key: &[u8], key: &[u8]) -> Result<Option<IVec>, sled::Error> {
        let cache = self.get_cache_mut(&tree_key.into())?;
        cache.remove(key)
    }

    /// Aggregate all the current overlay changes into [`sled::Batch`] instances and
    /// return vectors of [`sled::Tree`] and their respective [`sled::Batch`] that can
    /// be used for further operations. If there are no changes, both vectors will be empty.
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

    /// Ensure all new trees that have been opened exist in sled by reopening them,
    /// and atomically apply all batches on all trees as a transaction.
    /// This function **does not** perform a db flush. This should be done externally,
    /// since then there is a choice to perform either blocking or async IO.
    pub fn apply(&mut self) -> Result<(), TransactionError<sled::Error>> {
        // Ensure new trees exist
        for tree_key in &self.new_tree_names {
            let tree = self.db.open_tree(tree_key)?;
            self.trees.insert(tree_key.clone(), tree);
        }

        // Aggregate batches
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
