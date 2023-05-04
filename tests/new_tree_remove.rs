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

//! Simulate the creation of a [`SledDbOverlay`] on top of an entire
//! [`sled::Db`] instance, generate a new tree that doesn't exist in
//! sled, perform writes to verify overlay's cache functionality, and
//! verify that scratching everything will not write the new tree.

use sled::Config;

use sled_overlay::SledDbOverlay;

const TREE: &[u8] = b"_tree";

#[test]
fn new_tree_remove() -> Result<(), sled::Error> {
    // Initialize database
    let config = Config::new().temporary(true);
    let db = config.open()?;

    // Initialize overlay
    let mut overlay = SledDbOverlay::new(&db);

    // Open tree in the overlay
    overlay.open_tree(TREE)?;

    // We keep seperate tree for validation
    let tree = db.open_tree(TREE)?;

    // Insert some values to the overlay
    overlay.insert(TREE, b"key_a", b"val_a")?;
    overlay.insert(TREE, b"key_b", b"val_b")?;
    overlay.insert(TREE, b"key_c", b"val_c")?;

    // Verify they are in the overlay
    assert_eq!(overlay.get(TREE, b"key_a")?, Some(b"val_a".into()));
    assert_eq!(overlay.get(TREE, b"key_b")?, Some(b"val_b".into()));
    assert_eq!(overlay.get(TREE, b"key_c")?, Some(b"val_c".into()));

    // Verify they are not in sled
    assert_eq!(tree.get(b"key_a")?, None);
    assert_eq!(tree.get(b"key_b")?, None);
    assert_eq!(tree.get(b"key_c")?, None);

    // Now we asume something happened and want to scratch everything
    assert_eq!(overlay.purge_new_trees(), Ok(()));

    // Don't forget to flush
    db.flush()?;

    // Verify sled doesn't contain the tree
    assert!(!db.tree_names().contains(&TREE.into()));

    Ok(())
}

#[test]
fn new_tree_remove_multiple_overlays() -> Result<(), sled::Error> {
    // Initialize database
    let config = Config::new().temporary(true);
    let db = config.open()?;

    // Initialize overlays
    let mut overlay0 = SledDbOverlay::new(&db);
    let mut overlay1 = SledDbOverlay::new(&db);

    // Open tree in the overlays
    overlay0.open_tree(TREE)?;
    overlay1.open_tree(TREE)?;

    // We keep seperate tree for validation
    let tree = db.open_tree(TREE)?;

    // Insert some values to the overlays
    overlay0.insert(TREE, b"key_a", b"val_a")?;
    overlay0.insert(TREE, b"key_b", b"val_b")?;
    overlay0.insert(TREE, b"key_c", b"val_c")?;
    overlay1.insert(TREE, b"key_a", b"val_a")?;
    overlay1.insert(TREE, b"key_b", b"val_b")?;
    overlay1.insert(TREE, b"key_c", b"val_c")?;

    // Verify they are in the overlays
    assert_eq!(overlay0.get(TREE, b"key_a")?, Some(b"val_a".into()));
    assert_eq!(overlay0.get(TREE, b"key_b")?, Some(b"val_b".into()));
    assert_eq!(overlay0.get(TREE, b"key_c")?, Some(b"val_c".into()));
    assert_eq!(overlay1.get(TREE, b"key_a")?, Some(b"val_a".into()));
    assert_eq!(overlay1.get(TREE, b"key_b")?, Some(b"val_b".into()));
    assert_eq!(overlay1.get(TREE, b"key_c")?, Some(b"val_c".into()));

    // Verify they are not in sled
    assert_eq!(tree.get(b"key_a")?, None);
    assert_eq!(tree.get(b"key_b")?, None);
    assert_eq!(tree.get(b"key_c")?, None);

    // Now we asume something happened and want to scratch everything
    // in overlay0
    assert_eq!(overlay0.purge_new_trees(), Ok(()));

    // Now execute all tree batches in the overlay1
    assert_eq!(overlay1.apply(), Ok(()));

    // Don't forget to flush
    db.flush()?;

    // Verify sled contain the tree and the keys
    assert!(db.tree_names().contains(&TREE.into()));
    // We need to re-open the tree since we removed it when we
    // scratched overlay0 (overlay0.purge_new_trees())
    let tree = db.open_tree(TREE)?;
    assert_eq!(tree.get(b"key_a")?, Some(b"val_a".into()));
    assert_eq!(tree.get(b"key_b")?, Some(b"val_b".into()));
    assert_eq!(tree.get(b"key_c")?, Some(b"val_c".into()));

    Ok(())
}
