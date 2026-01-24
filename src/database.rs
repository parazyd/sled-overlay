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

use sled::{
    transaction::{ConflictableTransactionError, TransactionError},
    IVec, Transactional,
};

use crate::{SledTreeOverlay, SledTreeOverlayIter, SledTreeOverlayStateDiff};

/// Struct representing [`SledDbOverlay`] cache state
#[derive(Debug, Clone)]
pub struct SledDbOverlayState {
    /// Existing trees in `db` at the time of instantiation, so we can track newly opened trees.
    pub initial_tree_names: Vec<IVec>,
    /// New trees that have been opened, but didn't exist in `db` before.
    pub new_tree_names: Vec<IVec>,
    /// Pointers to [`SledTreeOverlay`] instances that have been created.
    pub caches: BTreeMap<IVec, SledTreeOverlay>,
    /// Trees that were dropped, along with their last state full diff.
    pub dropped_trees: BTreeMap<IVec, SledTreeOverlayStateDiff>,
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
            dropped_trees: BTreeMap::new(),
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
            if self.dropped_trees.contains_key(key) {
                return Err(sled::Error::CollectionNotFound(key.into()));
            }

            if let Some(batch) = cache.aggregate() {
                trees.push(cache.tree.clone());
                batches.push(batch);
            }
        }

        Ok((trees, batches))
    }

    /// Add provided `db` overlay state changes to our own.
    pub fn add_diff(
        &mut self,
        db: &sled::Db,
        diff: &SledDbOverlayStateDiff,
    ) -> Result<(), sled::Error> {
        self.initial_tree_names
            .retain(|x| diff.initial_tree_names.contains(x));

        for (k, (cache, drop)) in diff.caches.iter() {
            if *drop {
                assert!(!self.protected_tree_names.contains(k));
                self.new_tree_names.retain(|x| x != k);
                self.caches.remove(k);
                self.dropped_trees.insert(k.clone(), cache.clone());
                continue;
            }

            let Some(tree_overlay) = self.caches.get_mut(k) else {
                if !self.initial_tree_names.contains(k) && !self.new_tree_names.contains(k) {
                    self.new_tree_names.push(k.clone());
                }
                let mut overlay = SledTreeOverlay::new(&db.open_tree(k)?);
                overlay.add_diff(cache);
                self.caches.insert(k.clone(), overlay);
                continue;
            };

            // Add the diff to our tree overlay state
            tree_overlay.add_diff(cache);
        }

        for (k, (cache, restored)) in &diff.dropped_trees {
            // Drop the trees that are not restored
            if !restored {
                if self.dropped_trees.contains_key(k) {
                    continue;
                }
                self.new_tree_names.retain(|x| x != k);
                self.caches.remove(k);
                self.dropped_trees.insert(k.clone(), cache.clone());
                continue;
            }
            assert!(!self.protected_tree_names.contains(k));

            // Restore the tree
            self.initial_tree_names.retain(|x| x != k);
            if !self.new_tree_names.contains(k) {
                self.new_tree_names.push(k.clone());
            }

            let mut overlay = SledTreeOverlay::new(&db.open_tree(k)?);
            overlay.add_diff(cache);
            self.caches.insert(k.clone(), overlay);
        }

        Ok(())
    }

    /// Remove provided `db` overlay state changes from our own.
    pub fn remove_diff(&mut self, diff: &SledDbOverlayStateDiff) {
        // We have some assertions here to catch catastrophic
        // logic bugs here, as all our fields are depending on each
        // other when checking for differences.
        for (k, (cache, drop)) in diff.caches.iter() {
            // We must know the tree
            assert!(
                self.initial_tree_names.contains(k)
                    || self.new_tree_names.contains(k)
                    || self.dropped_trees.contains_key(k)
            );
            if !self.initial_tree_names.contains(k) {
                self.initial_tree_names.push(k.clone());
            }
            self.new_tree_names.retain(|x| x != k);

            // Check if tree is marked for drop
            if *drop {
                assert!(!self.protected_tree_names.contains(k));
                self.initial_tree_names.retain(|x| x != k);
                self.new_tree_names.retain(|x| x != k);
                self.caches.remove(k);
                self.dropped_trees.remove(k);
                continue;
            }

            // If the key is not in the cache, and it exists
            // in the dropped trees, update its diff
            let Some(tree_overlay) = self.caches.get_mut(k) else {
                let Some(tree_overlay) = self.dropped_trees.get_mut(k) else {
                    continue;
                };
                tree_overlay.update_values(cache);
                continue;
            };

            // If the state is unchanged, handle the stale tree
            if tree_overlay.state == cache.into() {
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
            tree_overlay.remove_diff(cache);
        }

        // Now we handle the dropped trees
        for (k, (cache, restored)) in diff.dropped_trees.iter() {
            // We must know the tree
            assert!(
                self.initial_tree_names.contains(k)
                    || self.new_tree_names.contains(k)
                    || self.dropped_trees.contains_key(k)
            );

            // Drop the trees that are not restored
            if !restored {
                assert!(!self.protected_tree_names.contains(k));
                self.initial_tree_names.retain(|x| x != k);
                self.new_tree_names.retain(|x| x != k);
                self.caches.remove(k);
                self.dropped_trees.remove(k);
                continue;
            }

            // Restore the tree
            self.initial_tree_names.retain(|x| x != k);
            if !self.new_tree_names.contains(k) {
                self.new_tree_names.push(k.clone());
            }

            // Skip if not in cache
            let Some(tree_overlay) = self.caches.get_mut(k) else {
                continue;
            };

            // If the state is unchanged, handle the stale tree
            if tree_overlay.state == cache.into() {
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
            tree_overlay.remove_diff(cache);
        }
    }
}

impl Default for SledDbOverlayState {
    fn default() -> Self {
        Self::new(vec![], vec![])
    }
}

/// Auxilliary struct representing a [`SledDbOverlayState`] diff log.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct SledDbOverlayStateDiff {
    /// Existing trees in `db` at the time of instantiation, so we can track newly opened trees.
    pub initial_tree_names: Vec<IVec>,
    /// State diff logs of all [`SledTreeOverlay`] instances that have been created,
    /// along with a boolean flag indicating if it should be dropped. The drop flag
    /// is always set to false, and change to true when we inverse the diff of a new
    /// tree(not in our initial tree names) and the inserts vector is empty, indicating
    /// that the tree should be dropped.
    pub caches: BTreeMap<IVec, (SledTreeOverlayStateDiff, bool)>,
    /// Trees that were dropped, along with their last state full diff, along with
    /// a boolean flag indicating if they should be restored. The restore flag is
    /// always set to false, and change to true when we inverse the diff, unless
    /// the tree is a new tree(not in our initial tree names).
    pub dropped_trees: BTreeMap<IVec, (SledTreeOverlayStateDiff, bool)>,
}

impl SledDbOverlayStateDiff {
    /// Instantiate a new [`SledDbOverlayStateDiff`], over the provided
    /// [`SledDbOverlayState`]. For newlly opened trees, all removed
    /// keys are ignored. New trees that get droped are also ignored.
    pub fn new(state: &SledDbOverlayState) -> Result<Self, sled::Error> {
        let mut caches = BTreeMap::new();
        let mut dropped_trees = BTreeMap::new();

        for (key, cache) in state.caches.iter() {
            let mut diff = cache.diff(&[])?;

            // Skip if diff is empty for an existing tree
            if diff.cache.is_empty()
                && diff.removed.is_empty()
                && !state.new_tree_names.contains(key)
            {
                continue;
            }

            // Ignore all dropped keys of new trees
            if state.new_tree_names.contains(key) {
                diff.removed = BTreeMap::new();
            }

            caches.insert(key.clone(), (diff, false));
        }

        for (key, cache) in state.dropped_trees.iter() {
            dropped_trees.insert(key.clone(), (cache.clone(), false));
        }

        Ok(Self {
            initial_tree_names: state.initial_tree_names.clone(),
            caches,
            dropped_trees,
        })
    }

    /// Aggregate all the overlay changes into [`sled::Batch`] instances and
    /// return vectors of [`sled::Tree`] and their respective [`sled::Batch`] that can
    /// be used for further operations. If there are no changes, both vectors will be empty.
    /// Provided state trees must contain all the [`sled::Tree`] pointers the diff mutates.
    fn aggregate(
        &self,
        state_trees: &BTreeMap<IVec, sled::Tree>,
    ) -> Result<(Vec<sled::Tree>, Vec<sled::Batch>), sled::Error> {
        let mut trees = vec![];
        let mut batches = vec![];

        for (key, (cache, drop)) in self.caches.iter() {
            if *drop {
                continue;
            }

            let Some(tree) = state_trees.get(key) else {
                return Err(sled::Error::CollectionNotFound(key.into()));
            };

            if let Some(batch) = cache.aggregate() {
                trees.push(tree.clone());
                batches.push(batch);
            }
        }

        for (key, (cache, restored)) in self.dropped_trees.iter() {
            if !restored {
                continue;
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

    /// Produces a [`SledDbOverlayStateDiff`] containing the inverse
    /// changes from our own.
    pub fn inverse(&self) -> Self {
        let mut diff = Self {
            initial_tree_names: self.initial_tree_names.clone(),
            ..Default::default()
        };

        for (key, (cache, drop)) in self.caches.iter() {
            let inverse = cache.inverse();
            // Flip its drop flag if its a new empty tree, otherwise
            // check if its cache is empty and its a new tree.
            let drop = if inverse.cache.is_empty()
                && inverse.removed.is_empty()
                && !self.initial_tree_names.contains(key)
            {
                !drop
            } else {
                inverse.cache.is_empty() && !self.initial_tree_names.contains(key)
            };
            diff.caches.insert(key.clone(), (inverse, drop));
        }

        for (key, (cache, restored)) in self.dropped_trees.iter() {
            if !self.initial_tree_names.contains(key) {
                continue;
            }
            diff.dropped_trees
                .insert(key.clone(), (cache.clone(), !restored));
        }

        diff
    }

    /// Remove provided `db` overlay state changes from our own.
    pub fn remove_diff(&mut self, other: &Self) {
        // We have some assertions here to catch catastrophic
        // logic bugs here, as all our fields are depending on each
        // other when checking for differences.
        for initial_tree_name in &other.initial_tree_names {
            assert!(self.initial_tree_names.contains(initial_tree_name));
        }

        // First we remove each cache diff
        for (key, cache_pair) in other.caches.iter() {
            if !self.initial_tree_names.contains(key) {
                self.initial_tree_names.push(key.clone());
            }

            // If the key is not in the cache, and it exists
            // in the dropped trees, update its diff.
            let Some(tree_overlay) = self.caches.get_mut(key) else {
                let Some((tree_overlay, _)) = self.dropped_trees.get_mut(key) else {
                    continue;
                };
                tree_overlay.update_values(&cache_pair.0);
                continue;
            };

            // If the state is unchanged, handle the stale tree
            if tree_overlay == cache_pair {
                // Drop the stale reference
                self.caches.remove(key);
                continue;
            }

            // Remove the diff from our tree overlay state
            tree_overlay.0.remove_diff(&cache_pair.0);
        }

        // Now we handle the dropped trees. We must have all
        // the keys in our dropped trees keys.
        for (key, (cache, restored)) in other.dropped_trees.iter() {
            // Check if the tree was reopened
            if let Some(tree_overlay) = self.caches.get_mut(key) {
                assert!(!self.dropped_trees.contains_key(key));
                // Remove the diff from our tree overlay state
                tree_overlay.0.remove_diff(cache);
                continue;
            }
            assert!(self.dropped_trees.contains_key(key));

            // Restore tree if its flag is set to true
            if *restored {
                self.caches.insert(key.clone(), (cache.clone(), false));
            }

            // Drop the tree
            self.initial_tree_names.retain(|x| x != key);
            self.dropped_trees.remove(key);
        }
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

        // Check if we have already opened this tree
        if self.state.caches.contains_key(&tree_key) {
            return Ok(());
        }

        // Open this tree in sled
        let tree = self.db.open_tree(&tree_key)?;
        let mut cache = SledTreeOverlay::new(&tree);

        // If we are reopenning a dropped tree, grab its cache
        if let Some(diff) = self.state.dropped_trees.remove(&tree_key) {
            cache.state = (&diff).into();
        }

        // In case it hasn't existed before, we also need to track it
        // in `self.new_tree_names`.
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
        if self.state.dropped_trees.contains_key(&tree_key) {
            return Err(sled::Error::CollectionNotFound(tree_key));
        }

        // Check if its a new tree we created
        if self.state.new_tree_names.contains(&tree_key) {
            self.state.new_tree_names.retain(|x| *x != tree_key);
            let tree = match self.get_cache(&tree_key) {
                Ok(cache) => &cache.tree,
                _ => &self.db.open_tree(&tree_key)?,
            };
            let diff = SledTreeOverlayStateDiff::new_dropped(tree);
            self.state.caches.remove(&tree_key);
            self.state.dropped_trees.insert(tree_key, diff);

            return Ok(());
        }

        // Check if tree existed in the database
        if !self.state.initial_tree_names.contains(&tree_key) {
            return Err(sled::Error::CollectionNotFound(tree_key));
        }

        let tree = match self.get_cache(&tree_key) {
            Ok(cache) => &cache.tree,
            _ => &self.db.open_tree(&tree_key)?,
        };
        let diff = SledTreeOverlayStateDiff::new_dropped(tree);
        self.state.caches.remove(&tree_key);
        self.state.dropped_trees.insert(tree_key, diff);

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
        if self.state.dropped_trees.contains_key(tree_key) {
            return Err(sled::Error::CollectionNotFound(tree_key.into()));
        }

        if let Some(v) = self.state.caches.get(tree_key) {
            return Ok(v);
        }

        Err(sled::Error::CollectionNotFound(tree_key.into()))
    }

    /// Fetch a mutable reference to the cache for a given tree.
    fn get_cache_mut(&mut self, tree_key: &IVec) -> Result<&mut SledTreeOverlay, sled::Error> {
        if self.state.dropped_trees.contains_key(tree_key) {
            return Err(sled::Error::CollectionNotFound(tree_key.into()));
        }

        if let Some(v) = self.state.caches.get_mut(tree_key) {
            return Ok(v);
        }
        Err(sled::Error::CollectionNotFound(tree_key.clone()))
    }

    /// Fetch all our caches current [`sled::Tree`] pointers.
    pub fn get_state_trees(&self) -> BTreeMap<IVec, sled::Tree> {
        // Grab our state tree pointers
        let mut state_trees = BTreeMap::new();
        for (key, cache) in self.state.caches.iter() {
            state_trees.insert(key.clone(), cache.tree.clone());
        }

        state_trees
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
        cache.is_empty()
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

    /// Removes all values from the specified tree cache and marks all
    /// its tree records as removed.
    pub fn clear(&mut self, tree_key: &[u8]) -> Result<(), sled::Error> {
        let cache = self.get_cache_mut(&tree_key.into())?;
        cache.clear()
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
        for tree in self.state.dropped_trees.keys() {
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
    pub fn diff(
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

    /// Add provided `db` overlay state changes from our own.
    pub fn add_diff(&mut self, diff: &SledDbOverlayStateDiff) -> Result<(), sled::Error> {
        self.state.add_diff(&self.db, diff)
    }

    /// Remove provided `db` overlay state changes from our own.
    pub fn remove_diff(&mut self, diff: &SledDbOverlayStateDiff) {
        self.state.remove_diff(diff)
    }

    /// For a provided `SledDbOverlayStateDiff`, ensure all trees exist in sled by
    /// reopening them, atomically apply all batches on all trees as a transaction,
    /// and drop dropped trees from sled. After that, remove the state changes from
    /// our own. This is will also mutate the initial trees, based on what was oppened
    /// and/or dropped. This function **does not** perform a db flush. This should be
    /// done externally, since then there is a choice to perform either blocking or
    /// async IO.
    pub fn apply_diff(
        &mut self,
        diff: &SledDbOverlayStateDiff,
    ) -> Result<(), TransactionError<sled::Error>> {
        // We assert that the diff doesn't try to drop any of our protected trees
        for tree in diff.dropped_trees.keys() {
            if self.state.protected_tree_names.contains(tree) {
                return Err(TransactionError::Storage(sled::Error::Unsupported(
                    "Protected tree can't be dropped".to_string(),
                )));
            }
        }
        for (tree_key, (_, drop)) in diff.caches.iter() {
            if *drop && self.state.protected_tree_names.contains(tree_key) {
                return Err(TransactionError::Storage(sled::Error::Unsupported(
                    "Protected tree can't be dropped".to_string(),
                )));
            }
        }

        // Grab current state trees
        let mut state_trees = self.get_state_trees();

        // Ensure diff trees exist
        for (tree_key, (_, drop)) in diff.caches.iter() {
            // Check if its an unknown tree
            if !self.state.initial_tree_names.contains(tree_key)
                && !self.state.new_tree_names.contains(tree_key)
            {
                self.state.new_tree_names.push(tree_key.clone());
            }

            // Check if it should be dropped
            if *drop {
                self.db.drop_tree(tree_key)?;
                continue;
            }

            if !state_trees.contains_key(tree_key) {
                let tree = self.db.open_tree(tree_key)?;
                state_trees.insert(tree_key.clone(), tree);
            }
        }

        // Drop removed trees and ensure restored trees exist
        for (tree_key, (_, restored)) in diff.dropped_trees.iter() {
            if !restored {
                state_trees.remove(tree_key);
                self.db.drop_tree(tree_key)?;
                continue;
            }

            // Check if its an unknown tree
            if !self.state.initial_tree_names.contains(tree_key)
                && !self.state.new_tree_names.contains(tree_key)
            {
                self.state.new_tree_names.push(tree_key.clone());
            }

            if !state_trees.contains_key(tree_key) {
                let tree = self.db.open_tree(tree_key)?;
                state_trees.insert(tree_key.clone(), tree);
            }
        }

        // Aggregate batches
        let (trees, batches) = diff.aggregate(&state_trees)?;
        if trees.is_empty() {
            self.remove_diff(diff);
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
        self.remove_diff(diff);

        Ok(())
    }

    /// Retrieve an immutable itterator from the overlay if the specified tree cache exists.
    pub fn iter(&self, tree_key: &[u8]) -> Result<SledTreeOverlayIter<'_>, sled::Error> {
        let cache = self.get_cache(&tree_key.into())?;
        Ok(cache.iter())
    }
}
