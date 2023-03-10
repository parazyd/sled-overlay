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

//! Simulate the creation of two [`SledTreeOverlay`] on top of two
//! [`sled::Tree`] instances, and perform writes to verify overlay's cache
//! functionality.

use sled::{transaction::ConflictableTransactionError, Config, Transactional};

use sled_overlay::SledTreeOverlay;

const TREE_1: &str = "_tree1";
const TREE_2: &str = "_tree2";

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

    // Verify they are not in sled
    assert_eq!(tree_1.get(b"key_a")?, None);
    assert_eq!(tree_1.get(b"key_b")?, None);
    assert_eq!(tree_1.get(b"key_c")?, None);

    assert_eq!(tree_2.get(b"key_d")?, None);
    assert_eq!(tree_2.get(b"key_e")?, None);
    assert_eq!(tree_2.get(b"key_f")?, None);

    // Aggregate all the batches for writing
    let mut batches = vec![];
    batches.push(overlay_1.aggregate());
    batches.push(overlay_2.aggregate());

    // Now we write them to sled
    vec![&tree_1, &tree_2]
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
