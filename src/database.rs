/* This file is part of sled-overlay
 *
 * Copyright (C) 2023-2024 Dyne.org foundation
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

use sled::transaction::{ConflictableTransactionError, TransactionError};
use sled::{IVec, Transactional};

use crate::{SledTreeOverlay, SledTreeOverlayStateDiff};

/// Struct representing [`SledDbOverlay`] cache state
#[derive(Debug, Clone)]
pub struct SledDbOverlayState {
    /// Existing trees in `db` at the time of instantiation, so we can track newly opened trees.
    pub initial_tree_names: Vec<IVec>,
    /// New trees that have been opened, but didn't exist in `db` before.
    pub new_tree_names: Vec<IVec>,
    /// Pointers to [`SledTreeOverlay`] instances that have been created.
    pub caches: BTreeMap<IVec, SledTreeOverlay>,
    /// Trees that were dropped.
    pub dropped_tree_names: Vec<IVec>,
    /// Protected trees, that we don't allow their removal,
    /// and don't drop their references if they become stale.
    pub protected_tree_names: Vec<IVec>,
}

impl SledDbOverlayState {
    /// Instantiate a new [`SledDbOverlayState`].
    pub fn new(initial_tree_names: Vec<IVec>, protected_tree_names: Vec<IVec>) -> Self {
        Self {
            initial_tree_names,
            new_tree_names: vec![],
            caches: BTreeMap::new(),
            dropped_tree_names: vec![],
            protected_tree_names,
        }
    }

    /// Aggregate all the current overlay changes into [`sled::Batch`] instances and
    /// return vectors of [`sled::Tree`] and their respective [`sled::Batch`] that can
    /// be used for further operations. If there are no changes, both vectors will be empty.
    fn aggregate(&self) -> Result<(Vec<sled::Tree>, Vec<sled::Batch>), sled::Error> {
        let mut trees = vec![];
        let mut batches = vec![];

        for (key, cache) in self.caches.iter() {
            if self.dropped_tree_names.contains(key) {
                return Err(sled::Error::CollectionNotFound(key.into()));
            }

            if let Some(batch) = cache.aggregate() {
                trees.push(cache.tree.clone());
                batches.push(batch);
            }
        }

        Ok((trees, batches))
    }

    /// Add provided `db` overlay state changes from our own.
    pub fn add_diff2(
        &mut self,
        db: &sled::Db,
        diff: &SledDbOverlayStateDiff,
    ) -> Result<(), sled::Error> {
        self.initial_tree_names
            .retain(|x| diff.initial_tree_names.contains(x));

        for new_tree_name in &diff.new_tree_names {
            if self.new_tree_names.contains(new_tree_name) {
                continue;
            }
            self.new_tree_names.push(new_tree_name.clone());
        }

        for (k, v) in diff.caches.iter() {
            let Some(tree_overlay) = self.caches.get_mut(k) else {
                // We don't allow reopening a dropped tree.
                if self.dropped_tree_names.contains(k) {
                    return Err(sled::Error::CollectionNotFound(k.clone()));
                }

                // Open this tree in sled. In case it hasn't existed before, we also need
                // to track it in `self.new_tree_names`.
                let tree = db.open_tree(k)?;
                let mut cache = SledTreeOverlay::new(&tree);

                if !self.initial_tree_names.contains(k) && !self.new_tree_names.contains(k) {
                    self.new_tree_names.push(k.clone());
                }

                // Add the diff from our tree overlay state
                cache.add_diff2(v);

                self.caches.insert(k.clone(), cache);
                continue;
            };

            // If the state is unchanged, we skip it
            if tree_overlay.state == v.into() {
                continue;
            }

            // Add the diff from our tree overlay state
            tree_overlay.add_diff2(v);
        }

        for dropped_tree_name in &diff.dropped_tree_names {
            if self.dropped_tree_names.contains(dropped_tree_name) {
                continue;
            }
            self.new_tree_names.retain(|x| x != dropped_tree_name);
            self.caches.remove(dropped_tree_name);
            self.dropped_tree_names.push(dropped_tree_name.clone());
        }

        Ok(())
    }

    /// Add provided `db` overlay state changes from our own.
    pub fn add_diff(&mut self, other: &Self) {
        self.initial_tree_names
            .retain(|x| other.initial_tree_names.contains(x));

        for new_tree_name in &other.new_tree_names {
            if self.new_tree_names.contains(new_tree_name) {
                continue;
            }
            self.new_tree_names.push(new_tree_name.clone());
        }

        for (k, v) in other.caches.iter() {
            let Some(tree_overlay) = self.caches.get_mut(k) else {
                self.caches.insert(k.clone(), v.clone());
                continue;
            };

            // If the state is unchanged, we skip it
            if tree_overlay.state == v.state {
                continue;
            }

            // Add the diff from our tree overlay state
            tree_overlay.add_diff(&v.state);
        }

        for dropped_tree_name in &other.dropped_tree_names {
            if self.dropped_tree_names.contains(dropped_tree_name) {
                continue;
            }
            self.new_tree_names.retain(|x| x != dropped_tree_name);
            self.caches.remove(dropped_tree_name);
            self.dropped_tree_names.push(dropped_tree_name.clone());
        }
    }

    /// Remove provided `db` overlay state changes from our own.
    pub fn remove_diff2(&mut self, diff: &SledDbOverlayStateDiff) -> Result<(), sled::Error> {
        // We have some assertions here to catch catastrophic
        // logic bugs here, as all our fields are depending on each
        // other when checking for differences.
        for initial_tree_name in &diff.initial_tree_names {
            assert!(self.initial_tree_names.contains(initial_tree_name));
        }

        for new_tree_name in &diff.new_tree_names {
            self.new_tree_names.retain(|x| x != new_tree_name);
            self.initial_tree_names.push(new_tree_name.clone());
        }

        for (k, v) in diff.caches.iter() {
            // If the key is not in the cache, it must
            // be in the dropped tree names
            let Some(tree_overlay) = self.caches.get_mut(k) else {
                assert!(self.dropped_tree_names.contains(k));
                continue;
            };

            // If the state is unchanged, handle the stale tree
            if tree_overlay.state == v.into() {
                // If tree is protected, we simply reset its cache
                if self.protected_tree_names.contains(k) {
                    tree_overlay.state.cache = BTreeMap::new();
                    tree_overlay.state.removed = BTreeSet::new();
                    tree_overlay.checkpoint();
                    continue;
                }

                // Drop the stale reference
                self.caches.remove(k);
                continue;
            }

            // Remove the diff from our tree overlay state
            tree_overlay.remove_diff2(v);
        }

        // Since we don't allow reopenning dropped trees, we must
        // have all the dropped tree names.
        for dropped_tree_name in &diff.dropped_tree_names {
            assert!(!self.caches.contains_key(dropped_tree_name));
            assert!(self.dropped_tree_names.contains(dropped_tree_name));
            self.dropped_tree_names.retain(|x| x != dropped_tree_name);
            self.initial_tree_names.retain(|x| x != dropped_tree_name);
        }

        assert_eq!(
            self.initial_tree_names.len(),
            diff.initial_tree_names.len() + diff.new_tree_names.len()
                - diff.dropped_tree_names.len()
        );

        Ok(())
    }

    /// Remove provided `db` overlay state changes from our own.
    pub fn remove_diff(&mut self, other: &Self) {
        // We have some assertions here to catch catastrophic
        // logic bugs here, as all our fields are depending on each
        // other when checking for differences.
        for initial_tree_name in &other.initial_tree_names {
            assert!(self.initial_tree_names.contains(initial_tree_name));
        }

        for new_tree_name in &other.new_tree_names {
            self.new_tree_names.retain(|x| x != new_tree_name);
            self.initial_tree_names.push(new_tree_name.clone());
        }

        for (k, v) in other.caches.iter() {
            // If the key is not in the cache, it must
            // be in the dropped tree names
            let Some(tree_overlay) = self.caches.get_mut(k) else {
                assert!(self.dropped_tree_names.contains(k));
                continue;
            };

            // If the state is unchanged, handle the stale tree
            if tree_overlay.state == v.state {
                // If tree is protected, we simply reset its cache
                if self.protected_tree_names.contains(k) {
                    tree_overlay.state.cache = BTreeMap::new();
                    tree_overlay.state.removed = BTreeSet::new();
                    tree_overlay.checkpoint();
                    continue;
                }

                // Drop the stale reference
                self.caches.remove(k);
                continue;
            }

            // Remove the diff from our tree overlay state
            tree_overlay.remove_diff(&v.state);
        }

        // Since we don't allow reopenning dropped trees, we must
        // have all the dropped tree names.
        for dropped_tree_name in &other.dropped_tree_names {
            assert!(!self.caches.contains_key(dropped_tree_name));
            assert!(self.dropped_tree_names.contains(dropped_tree_name));
            self.dropped_tree_names.retain(|x| x != dropped_tree_name);
            self.initial_tree_names.retain(|x| x != dropped_tree_name);
        }

        assert_eq!(
            self.initial_tree_names.len(),
            other.initial_tree_names.len() + other.new_tree_names.len()
                - other.dropped_tree_names.len()
        );
    }
}

impl Default for SledDbOverlayState {
    fn default() -> Self {
        Self::new(vec![], vec![])
    }
}

/// Struct representing [`SledDbOverlay`] cache state
/// Auxilliary struct representing a [`SledDbOverlayState`] diff log.
#[derive(Debug, Default, Clone)]
pub struct SledDbOverlayStateDiff {
    /// Existing trees in `db` at the time of instantiation, so we can track newly opened trees.
    pub initial_tree_names: Vec<IVec>,
    /// New trees that have been opened, but didn't exist in `db` before.
    pub new_tree_names: Vec<IVec>,
    /// State deff logs of all [`SledTreeOverlay`] instances that have been created.
    pub caches: BTreeMap<IVec, SledTreeOverlayStateDiff>,
    /// Trees that were dropped.
    pub dropped_tree_names: Vec<IVec>,
    /// Protected trees, that we don't allow their removal,
    /// and don't drop their references if they become stale.
    pub protected_tree_names: Vec<IVec>,
}

impl SledDbOverlayStateDiff {
    /// Instantiate a new [`SledDbOverlayStateDiff`], over the provided
    /// [`SledDbOverlayState`].
    pub fn new(state: &SledDbOverlayState) -> Result<Self, sled::Error> {
        let mut caches = BTreeMap::new();
        for (key, cache) in state.caches.iter() {
            caches.insert(key.clone(), cache.diff2(&[])?);
        }

        Ok(Self {
            initial_tree_names: state.initial_tree_names.clone(),
            new_tree_names: state.new_tree_names.clone(),
            caches,
            dropped_tree_names: state.dropped_tree_names.clone(),
            protected_tree_names: state.protected_tree_names.clone(),
        })
    }

    /// Aggregate all the current overlay changes into [`sled::Batch`] instances and
    /// return vectors of [`sled::Tree`] and their respective [`sled::Batch`] that can
    /// be used for further operations. If there are no changes, both vectors will be empty.
    fn aggregate(
        &self,
        state_trees: &BTreeMap<IVec, sled::Tree>,
    ) -> Result<(Vec<sled::Tree>, Vec<sled::Batch>), sled::Error> {
        let mut trees = vec![];
        let mut batches = vec![];

        for (key, cache) in self.caches.iter() {
            if self.dropped_tree_names.contains(key) {
                return Err(sled::Error::CollectionNotFound(key.into()));
            }

            let Some(tree) = state_trees.get(key) else {
                return Err(sled::Error::CollectionNotFound(key.into()));
            };

            if let Some(batch) = cache.aggregate() {
                trees.push(tree.clone());
                batches.push(batch);
            }
        }

        Ok((trees, batches))
    }

    /// Remove provided `db` overlay state changes from our own.
    pub fn remove_diff(&mut self, other: &Self) {
        // We have some assertions here to catch catastrophic
        // logic bugs here, as all our fields are depending on each
        // other when checking for differences.
        for initial_tree_name in &other.initial_tree_names {
            assert!(self.initial_tree_names.contains(initial_tree_name));
        }

        for new_tree_name in &other.new_tree_names {
            self.new_tree_names.retain(|x| x != new_tree_name);
            self.initial_tree_names.push(new_tree_name.clone());
        }

        for (k, v) in other.caches.iter() {
            // If the key is not in the cache, it must
            // be in the dropped tree names
            let Some(tree_overlay) = self.caches.get_mut(k) else {
                assert!(self.dropped_tree_names.contains(k));
                continue;
            };

            // If the state is unchanged, handle the stale tree
            if tree_overlay == v {
                // If tree is protected, we simply reset its cache
                if self.protected_tree_names.contains(k) {
                    *tree_overlay = SledTreeOverlayStateDiff::default();
                    continue;
                }

                // Drop the stale reference
                self.caches.remove(k);
                continue;
            }

            // Remove the diff from our tree overlay state
            tree_overlay.remove_diff(v);
        }

        // Since we don't allow reopenning dropped trees, we must
        // have all the dropped tree names.
        for dropped_tree_name in &other.dropped_tree_names {
            assert!(!self.caches.contains_key(dropped_tree_name));
            assert!(self.dropped_tree_names.contains(dropped_tree_name));
            self.dropped_tree_names.retain(|x| x != dropped_tree_name);
            self.initial_tree_names.retain(|x| x != dropped_tree_name);
        }

        assert_eq!(
            self.initial_tree_names.len(),
            other.initial_tree_names.len() + other.new_tree_names.len()
                - other.dropped_tree_names.len()
        );
    }
}

/// An overlay on top of an entire [`sled::Db`] which can span multiple trees
#[derive(Clone)]
pub struct SledDbOverlay {
    /// The [`sled::Db`] that is being overlayed.
    db: sled::Db,
    /// Current overlay cache state
    pub state: SledDbOverlayState,
    /// Checkpointed cache state to revert to
    checkpoint: SledDbOverlayState,
}

impl SledDbOverlay {
    /// Instantiate a new [`SledDbOverlay`] on top of a given [`sled::Db`].
    /// Note: Provided protected trees don't have to be opened as protected,
    /// as they are setup as protected here.
    pub fn new(db: &sled::Db, protected_tree_names: Vec<&[u8]>) -> Self {
        let initial_tree_names = db.tree_names();
        let protected_tree_names: Vec<IVec> = protected_tree_names
            .into_iter()
            .map(|tree_name| tree_name.into())
            .collect();
        Self {
            db: db.clone(),
            state: SledDbOverlayState::new(
                initial_tree_names.clone(),
                protected_tree_names.clone(),
            ),
            checkpoint: SledDbOverlayState::new(initial_tree_names, protected_tree_names),
        }
    }

    /// Create a new [`SledTreeOverlay`] on top of a given `tree_name`.
    /// This function will also open a new tree inside `db` regardless of if it has
    /// existed before, so for convenience, we also provide [`SledDbOverlay::purge_new_trees`]
    /// in case we decide we don't want to write the batches, and drop the new trees.
    /// Additionally, a boolean flag is passed to mark the oppened tree as protected,
    /// meanning that it can't be removed and its references will never be dropped.
    pub fn open_tree(&mut self, tree_name: &[u8], protected: bool) -> Result<(), sled::Error> {
        let tree_key: IVec = tree_name.into();

        // We don't allow reopening a dropped tree.
        if self.state.dropped_tree_names.contains(&tree_key) {
            return Err(sled::Error::CollectionNotFound(tree_key));
        }

        if self.state.caches.contains_key(&tree_key) {
            // We have already opened this tree.
            return Ok(());
        }

        // Open this tree in sled. In case it hasn't existed before, we also need
        // to track it in `self.new_tree_names`.
        let tree = self.db.open_tree(&tree_key)?;
        let cache = SledTreeOverlay::new(&tree);

        if !self.state.initial_tree_names.contains(&tree_key) {
            self.state.new_tree_names.push(tree_key.clone());
        }

        self.state.caches.insert(tree_key.clone(), cache);

        // Mark tree as protected if requested
        if protected && !self.state.protected_tree_names.contains(&tree_key) {
            self.state.protected_tree_names.push(tree_key);
        }

        Ok(())
    }

    /// Drop a sled tree from the overlay.
    pub fn drop_tree(&mut self, tree_name: &[u8]) -> Result<(), sled::Error> {
        let tree_key: IVec = tree_name.into();

        // Check if tree is protected
        if self.state.protected_tree_names.contains(&tree_key) {
            return Err(sled::Error::Unsupported(
                "Protected tree can't be dropped".to_string(),
            ));
        }

        // Check if already removed
        if self.state.dropped_tree_names.contains(&tree_key) {
            return Err(sled::Error::CollectionNotFound(tree_key));
        }

        // Check if its a new tree we created
        if self.state.new_tree_names.contains(&tree_key) {
            self.state.new_tree_names.retain(|x| *x != tree_key);
            self.state.caches.remove(&tree_key);
            self.state.dropped_tree_names.push(tree_key);

            return Ok(());
        }

        // Check if tree existed in the database
        if !self.state.initial_tree_names.contains(&tree_key) {
            return Err(sled::Error::CollectionNotFound(tree_key));
        }

        self.state.caches.remove(&tree_key);
        self.state.dropped_tree_names.push(tree_key);

        Ok(())
    }

    /// Drop newly created trees from the sled database. This is a convenience
    /// function that should be used when we decide that we don't want to apply
    /// any cache changes, and we want to revert back to the initial state.
    pub fn purge_new_trees(&self) -> Result<(), sled::Error> {
        for i in &self.state.new_tree_names {
            self.db.drop_tree(i)?;
        }

        Ok(())
    }

    /// Fetch the cache for a given tree.
    fn get_cache(&self, tree_key: &IVec) -> Result<&SledTreeOverlay, sled::Error> {
        if self.state.dropped_tree_names.contains(tree_key) {
            return Err(sled::Error::CollectionNotFound(tree_key.into()));
        }

        if let Some(v) = self.state.caches.get(tree_key) {
            return Ok(v);
        }

        Err(sled::Error::CollectionNotFound(tree_key.into()))
    }

    /// Fetch a mutable reference to the cache for a given tree.
    fn get_cache_mut(&mut self, tree_key: &IVec) -> Result<&mut SledTreeOverlay, sled::Error> {
        if self.state.dropped_tree_names.contains(tree_key) {
            return Err(sled::Error::CollectionNotFound(tree_key.into()));
        }

        if let Some(v) = self.state.caches.get_mut(tree_key) {
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

    /// Returns `true` if specified tree cache is empty.
    pub fn is_empty(&self, tree_key: &[u8]) -> Result<bool, sled::Error> {
        let cache = self.get_cache(&tree_key.into())?;
        Ok(cache.is_empty())
    }

    /// Returns last value from the overlay if the specified tree cache is not empty.
    pub fn last(&self, tree_key: &[u8]) -> Result<Option<(IVec, IVec)>, sled::Error> {
        let cache = self.get_cache(&tree_key.into())?;
        cache.last()
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
    fn aggregate(&self) -> Result<(Vec<sled::Tree>, Vec<sled::Batch>), sled::Error> {
        self.state.aggregate()
    }

    /// Ensure all new trees that have been opened exist in sled by reopening them,
    /// atomically apply all batches on all trees as a transaction, and drop dropped
    /// trees from sled.
    /// This function **does not** perform a db flush. This should be done externally,
    /// since then there is a choice to perform either blocking or async IO.
    /// After execution is successful, caller should *NOT* use the overlay again.
    pub fn apply(&mut self) -> Result<(), TransactionError<sled::Error>> {
        // Ensure new trees exist
        let new_tree_names = self.state.new_tree_names.clone();
        for tree_key in &new_tree_names {
            let tree = self.db.open_tree(tree_key)?;
            // Update cache tree pointer, it must exist
            let cache = self.get_cache_mut(tree_key)?;
            cache.tree = tree;
        }

        // Drop removed trees
        for tree in &self.state.dropped_tree_names {
            self.db.drop_tree(tree)?;
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

    /// Checkpoint current cache state so we can revert to it, if needed.
    pub fn checkpoint(&mut self) {
        self.checkpoint = self.state.clone();
    }

    /// Revert to current cache state checkpoint.
    pub fn revert_to_checkpoint(&mut self) -> Result<(), sled::Error> {
        // We first check if any new trees were opened, so we can remove them.
        let new_trees: Vec<_> = self
            .state
            .new_tree_names
            .iter()
            .filter(|tree| !self.checkpoint.new_tree_names.contains(tree))
            .collect();
        for tree in &new_trees {
            self.db.drop_tree(tree)?;
        }

        self.state = self.checkpoint.clone();

        Ok(())
    }

    /// Calculate differences from provided overlay state changes
    /// sequence. This can be used when we want to keep track of
    /// consecutive individual changes performed over the current
    /// overlay state. If the sequence is empty, current state
    /// is returned as the diff.
    pub fn diff2(
        &self,
        sequence: &[SledDbOverlayStateDiff],
    ) -> Result<SledDbOverlayStateDiff, sled::Error> {
        // Grab current state
        let mut current = SledDbOverlayStateDiff::new(&self.state)?;

        // Remove provided diffs sequence
        for diff in sequence {
            current.remove_diff(diff);
        }

        Ok(current)
    }

    /// Calculate differences from provided overlay state changes
    /// sequence. This can be used when we want to keep track of
    /// consecutive individual changes performed over the current
    /// overlay state. If the sequence is empty, current state
    /// is returned as the diff.
    pub fn diff(&self, sequence: &[SledDbOverlayState]) -> SledDbOverlayState {
        // Grab current state
        let mut current = self.state.clone();

        // Remove provided diffs sequence
        for diff in sequence {
            current.remove_diff(diff);
        }

        current
    }

    /// Add provided `db` overlay state changes from our own.
    pub fn add_diff2(&mut self, diff: &SledDbOverlayStateDiff) -> Result<(), sled::Error> {
        self.state.add_diff2(&self.db, diff)
    }

    /// Add provided `db` overlay state changes from our own.
    pub fn add_diff(&mut self, other: &SledDbOverlayState) {
        self.state.add_diff(other)
    }

    /// Remove provided `db` overlay state changes from our own.
    pub fn remove_diff2(&mut self, diff: &SledDbOverlayStateDiff) -> Result<(), sled::Error> {
        self.state.remove_diff2(diff)
    }

    /// Remove provided `db` overlay state changes from our own.
    pub fn remove_diff(&mut self, other: &SledDbOverlayState) {
        self.state.remove_diff(other)
    }

    /// For a provided `SledDbOverlayStateDiff`, ensure all new trees that have been
    /// opened exist in sled by reopening them, atomically apply all batches on
    /// all trees as a transaction, and drop dropped trees from sled.
    /// After that, remove the state changes from our own. This is will also mutate
    /// the initial trees, based on what was oppened and/or dropped.
    /// This function **does not** perform a db flush. This should be done externally,
    /// since then there is a choice to perform either blocking or async IO.
    pub fn apply_diff2(
        &mut self,
        diff: &SledDbOverlayStateDiff,
    ) -> Result<(), TransactionError<sled::Error>> {
        // Grab our state tree pointers
        let mut state_trees = BTreeMap::new();
        for (key, cache) in self.state.caches.iter() {
            state_trees.insert(key.clone(), cache.tree.clone());
        }

        // Ensure new trees exist
        for tree_key in &diff.new_tree_names {
            let tree = self.db.open_tree(tree_key)?;
            state_trees.insert(tree_key.clone(), tree);
        }

        // Drop removed trees
        for tree in &diff.dropped_tree_names {
            self.db.drop_tree(tree)?;
        }

        // Aggregate batches
        let (trees, batches) = diff.aggregate(&state_trees)?;
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

        // Remove changes from our current state
        self.remove_diff2(diff)?;

        Ok(())
    }

    /// For a provided `SledDbOverlayState`, ensure all new trees that have been
    /// opened exist in sled by reopening them, atomically apply all batches on
    /// all trees as a transaction, and drop dropped trees from sled.
    /// After that, remove the state changes from our own. This is will also mutate
    /// the initial trees, based on what was oppened and/or dropped.
    /// This function **does not** perform a db flush. This should be done externally,
    /// since then there is a choice to perform either blocking or async IO.
    pub fn apply_diff(
        &mut self,
        other: &mut SledDbOverlayState,
    ) -> Result<(), TransactionError<sled::Error>> {
        // Ensure new trees exist
        for tree_key in &other.new_tree_names {
            let tree = self.db.open_tree(tree_key)?;
            // Update cache tree pointer, it must exist
            let cache = other.caches.get_mut(tree_key).unwrap();
            cache.tree = tree;
        }

        // Drop removed trees
        for tree in &other.dropped_tree_names {
            self.db.drop_tree(tree)?;
        }

        // Aggregate batches
        let (trees, batches) = other.aggregate()?;
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

        // Remove changes from our current state
        self.remove_diff(other);

        Ok(())
    }
}
