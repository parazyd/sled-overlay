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
//! [`sled::Db`] instance, and perform writes to verify overlay's cache
//! functionality.

use sled::Config;

use sled_overlay::SledDbOverlay;

const TREE_1: &[u8] = b"_tree1";
const TREE_2: &[u8] = b"_tree2";

#[test]
fn sled_db_overlay_remove_tree() -> Result<(), sled::Error> {
    // Initialize database
    let config = Config::new().temporary(true);
    let db = config.open()?;

    // Create tree in the database and insert some values
    let tree_1 = db.open_tree(TREE_1)?;
    tree_1.insert(b"key_a", b"val_a")?;
    tree_1.insert(b"key_b", b"val_b")?;
    tree_1.insert(b"key_c", b"val_c")?;

    // Don't forget to flush
    db.flush()?;

    // Initialize overlay
    let mut overlay = SledDbOverlay::new(&db);

    // Open tree in the overlay
    overlay.open_tree(TREE_1)?;

    // Verify values are in the overlay
    assert_eq!(overlay.get(TREE_1, b"key_a")?, Some(b"val_a".into()));
    assert_eq!(overlay.get(TREE_1, b"key_b")?, Some(b"val_b".into()));
    assert_eq!(overlay.get(TREE_1, b"key_c")?, Some(b"val_c".into()));

    // Drop tree
    overlay.drop_tree(TREE_1)?;

    // Try to drop the tree again
    assert!(overlay.drop_tree(TREE_1).is_err());

    // Try to drop a non existing tree
    assert!(overlay.drop_tree(TREE_2).is_err());

    // Open the new tree
    overlay.open_tree(TREE_2)?;

    // Drop the new tree
    overlay.drop_tree(TREE_2)?;

    // Now execute all tree batches in the overlay
    assert_eq!(overlay.apply(), Ok(()));

    // Don't forget to flush
    db.flush()?;

    // Verify sled doesn't contain the trees
    assert!(!db.tree_names().contains(&TREE_1.into()));
    assert!(!db.tree_names().contains(&TREE_2.into()));

    Ok(())
}
