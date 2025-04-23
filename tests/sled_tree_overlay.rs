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

//! Simulate the creation of two [`SledTreeOverlay`] on top of two
//! [`sled::Tree`] instances, and perform writes to verify overlay's cache
//! functionality.

use sled::{transaction::ConflictableTransactionError, Config, IVec, Transactional};

use sled_overlay::SledTreeOverlay;

const TREE_1: &[u8] = b"_tree1";
const TREE_2: &[u8] = b"_tree2";

#[test]
fn sled_tree_overlay() -> Result<(), sled::Error> {
    // Initialize database
    let config = Config::new().temporary(true);
    let db = config.open()?;

    // Initialize trees and their overlays
    let tree_1 = db.open_tree(TREE_1)?;
    let tree_2 = db.open_tree(TREE_2)?;
    let mut overlay_1 = SledTreeOverlay::new(&tree_1);
    let mut overlay_2 = SledTreeOverlay::new(&tree_2);

    // Check overlays are empty
    assert!(overlay_1.is_empty());
    assert!(overlay_2.is_empty());

    // Check last value is `None`
    assert_eq!(overlay_1.last()?, None);
    assert_eq!(overlay_1.last()?, None);

    // Insert some values to the overlays
    overlay_1.insert(b"key_a", b"val_a")?;
    overlay_1.insert(b"key_b", b"val_b")?;
    overlay_1.insert(b"key_c", b"val_c")?;

    overlay_2.insert(b"key_d", b"val_d")?;
    overlay_2.insert(b"key_e", b"val_e")?;
    overlay_2.insert(b"key_f", b"val_f")?;

    // Verify they are in the overlays
    assert_eq!(overlay_1.get(b"key_a")?, Some(b"val_a".into()));
    assert_eq!(overlay_1.get(b"key_b")?, Some(b"val_b".into()));
    assert_eq!(overlay_1.get(b"key_c")?, Some(b"val_c".into()));

    assert_eq!(overlay_2.get(b"key_d")?, Some(b"val_d".into()));
    assert_eq!(overlay_2.get(b"key_e")?, Some(b"val_e".into()));
    assert_eq!(overlay_2.get(b"key_f")?, Some(b"val_f".into()));

    // Check overlays are not empty
    assert!(!overlay_1.is_empty());
    assert!(!overlay_2.is_empty());

    // Check their last values
    assert_eq!(overlay_1.last()?, Some((b"key_c".into(), b"val_c".into())));
    assert_eq!(overlay_2.last()?, Some((b"key_f".into(), b"val_f".into())));

    // Verify they are not in sled
    assert_eq!(tree_1.get(b"key_a")?, None);
    assert_eq!(tree_1.get(b"key_b")?, None);
    assert_eq!(tree_1.get(b"key_c")?, None);

    assert_eq!(tree_2.get(b"key_d")?, None);
    assert_eq!(tree_2.get(b"key_e")?, None);
    assert_eq!(tree_2.get(b"key_f")?, None);

    // Aggregate all the batches for writing
    let batches = [overlay_1.aggregate(), overlay_2.aggregate()];

    // Now we write them to sled
    [&tree_1, &tree_2]
        .transaction(|trees| {
            for (i, tree) in trees.iter().enumerate() {
                if let Some(batch) = &batches[i] {
                    tree.apply_batch(batch)?;
                }
            }

            Ok::<(), ConflictableTransactionError<sled::Error>>(())
        })
        .unwrap();
    db.flush()?;

    // Verify sled contains keys
    assert_eq!(tree_1.get(b"key_a")?, Some(b"val_a".into()));
    assert_eq!(tree_1.get(b"key_b")?, Some(b"val_b".into()));
    assert_eq!(tree_1.get(b"key_c")?, Some(b"val_c".into()));

    assert_eq!(tree_2.get(b"key_d")?, Some(b"val_d".into()));
    assert_eq!(tree_2.get(b"key_e")?, Some(b"val_e".into()));
    assert_eq!(tree_2.get(b"key_f")?, Some(b"val_f".into()));

    Ok(())
}

#[test]
fn sled_tree_overlay_last() -> Result<(), sled::Error> {
    // Initialize database
    let config = Config::new().temporary(true);
    let db = config.open()?;

    // Initialize tree and its overlay
    let tree = db.open_tree(TREE_1)?;
    let mut overlay = SledTreeOverlay::new(&tree);
    assert!(overlay.is_empty());

    // Check last is None
    assert_eq!(overlay.last()?, None);

    // Insert a value to the tree
    tree.insert(b"key_a", b"val_a")?;

    // Check last is the last tree key
    let last = overlay.last()?.unwrap();
    assert_eq!(last.0, b"key_a");
    assert_eq!(last.1, b"val_a");

    // Remove the key from the overlay and check
    // last is None
    overlay.remove(b"key_a")?;
    assert_eq!(overlay.last()?, None);

    // Remove value from the tree
    tree.remove(b"key_a")?;

    // Insert key in overlay and check its last
    overlay.insert(b"key_a", b"val_a")?;
    assert!(tree.is_empty());
    let last = overlay.last()?.unwrap();
    assert_eq!(last.0, b"key_a");
    assert_eq!(last.1, b"val_a");

    // Insert a key in the tree that is supposed to be last
    tree.insert(b"key_b", b"val_b")?;
    let last = overlay.last()?.unwrap();
    assert_eq!(last.0, b"key_b");
    assert_eq!(last.1, b"val_b");

    // Remove the key from the overlay and check
    // last is the correct one
    overlay.remove(b"key_b")?;
    let last = overlay.last()?.unwrap();
    assert_eq!(last.0, b"key_a");
    assert_eq!(last.1, b"val_a");

    Ok(())
}

#[test]
fn sled_tree_overlay_iteration() -> Result<(), sled::Error> {
    // Initialize database
    let config = Config::new().temporary(true);
    let db = config.open()?;

    // Initialize tree and its overlay
    let tree = db.open_tree(TREE_1)?;
    tree.insert(b"key_a", b"val_a")?;
    tree.insert(b"key_c", b"val_c")?;
    tree.insert(b"key_e", b"val_e")?;
    let mut overlay = SledTreeOverlay::new(&tree);

    // Insert some values to the overlay
    overlay.insert(b"key_b", b"val_b")?;
    overlay.insert(b"key_d", b"val_d")?;
    overlay.insert(b"key_e", b"val_ee")?;
    overlay.insert(b"key_f", b"val_f")?;

    // Remove some values from the overlay
    overlay.remove(b"key_c")?;
    overlay.remove(b"key_d")?;

    // Iterate overlay to verify sequence
    let expected_sequence = vec![
        (IVec::from(b"key_a"), IVec::from(b"val_a")),
        (IVec::from(b"key_b"), IVec::from(b"val_b")),
        (IVec::from(b"key_e"), IVec::from(b"val_ee")),
        (IVec::from(b"key_f"), IVec::from(b"val_f")),
    ];
    for (index, record) in overlay.iter().enumerate() {
        assert_eq!(record?, expected_sequence[index]);
    }

    // We can even iterate without calling .iter()
    let mut index = 0;
    #[allow(clippy::explicit_counter_loop)]
    for record in &overlay {
        assert_eq!(record?, expected_sequence[index]);
        index += 1;
    }

    Ok(())
}
