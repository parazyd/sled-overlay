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

//! Simulate the creation of a [`SledDbOverlay`] on top of an entire
//! [`sled::Db`] instance, and perform perform async serializations and
//! deserializations of its diffs to verify correctness.

#![cfg(feature = "serial")]

use darkfi_serial::{deserialize, serialize};
use sled::Config;

use sled_overlay::SledDbOverlay;

const TREE_1: &[u8] = b"_tree1";
const TREE_2: &[u8] = b"_tree2";
const TREE_3: &[u8] = b"_tree3";
const TREE_4: &[u8] = b"_tree4";
const TREE_5: &[u8] = b"_tree5";

#[test]
fn sled_db_diff_serialization() -> Result<(), sled::Error> {
    // Initialize database
    let config = Config::new().temporary(true);
    let db = config.open()?;

    // Initialize trees with some values
    let tree_1 = db.open_tree(TREE_1)?;
    tree_1.insert(b"key_a", b"val_a")?;
    let tree_4 = db.open_tree(TREE_4)?;
    tree_4.insert(b"key_g", b"val_g")?;
    tree_4.insert(b"key_j", b"val_j")?;

    // Initialize overlay
    let mut overlay = SledDbOverlay::new(&db, vec![]);

    // Open trees in the overlay
    overlay.open_tree(TREE_1, false)?;
    overlay.open_tree(TREE_3, false)?;
    overlay.open_tree(TREE_4, false)?;

    // Make a vector to keep track of changes
    let mut sequence = vec![];

    // Perform some changes and grab their differences
    overlay.insert(TREE_1, b"key_b", b"val_b")?;
    overlay.insert(TREE_3, b"key_i", b"val_i")?;
    overlay.insert(TREE_4, b"key_k", b"val_k")?;
    overlay.remove(TREE_4, b"key_g")?;
    sequence.push(overlay.diff(&sequence)?);

    overlay.insert(TREE_1, b"key_b", b"val_bb")?;
    overlay.remove(TREE_1, b"key_a")?;
    overlay.open_tree(TREE_2, false)?;
    overlay.insert(TREE_2, b"key_d", b"val_d")?;
    overlay.insert(TREE_2, b"key_e", b"val_e")?;
    overlay.drop_tree(TREE_3)?;
    overlay.insert(TREE_4, b"key_k", b"val_kk")?;
    overlay.open_tree(TREE_5, false)?;
    overlay.insert(TREE_5, b"key_h", b"val_h")?;
    sequence.push(overlay.diff(&sequence)?);

    overlay.insert(TREE_1, b"key_a", b"val_a")?;
    overlay.remove(TREE_1, b"key_b")?;
    overlay.insert(TREE_1, b"key_c", b"val_c")?;
    overlay.remove(TREE_2, b"key_e")?;
    overlay.insert(TREE_2, b"key_f", b"val_f")?;
    overlay.insert(TREE_4, b"key_l", b"val_l")?;
    overlay.drop_tree(TREE_4)?;
    sequence.push(overlay.diff(&sequence)?);

    // Verify serialization and deserialization of each diff
    for diff in sequence {
        let serialized = serialize(&diff);
        let deserialized = deserialize(&serialized)?;
        assert_eq!(diff, deserialized);
    }

    Ok(())
}
