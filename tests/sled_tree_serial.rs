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

//! Simulate the creation of a [`SledTreeOverlay`] on top of a
//! [`sled::Tree`] instance, and perform serializations and
//! deserializations of its diffs and records to verify correctness.

#![cfg(feature = "serial")]

use darkfi_serial::{deserialize, serialize};
use sled::Config;

use sled_overlay::{
    serial::{parse_record, parse_u32_key_record, parse_u64_key_record},
    SledTreeOverlay,
};

#[test]
fn sled_tree_diff_serialization() -> Result<(), sled::Error> {
    // Initialize database
    let config = Config::new().temporary(true);
    let db = config.open()?;

    // Initialize tree with some values and its overlay
    let tree = db.open_tree(b"_tree")?;
    tree.insert(b"key_a", b"val_a")?;
    let mut overlay = SledTreeOverlay::new(&tree);
    assert!(!overlay.is_empty());

    // Make a vector to keep track of changes
    let mut sequence = vec![];

    // Perform some changes and grab their differences
    overlay.insert(b"key_b", b"val_b")?;
    sequence.push(overlay.diff(&sequence)?);

    overlay.insert(b"key_b", b"val_bb")?;
    overlay.remove(b"key_a")?;
    sequence.push(overlay.diff(&sequence)?);

    overlay.insert(b"key_a", b"val_a")?;
    overlay.remove(b"key_b")?;
    overlay.insert(b"key_c", b"val_c")?;
    sequence.push(overlay.diff(&sequence)?);

    // Verify serialization and deserialization of each diff
    for diff in sequence {
        let serialized = serialize(&diff);
        let deserialized = deserialize(&serialized)?;
        assert_eq!(diff, deserialized);
    }

    Ok(())
}

#[test]
fn sled_tree_record_deserialization() -> Result<(), sled::Error> {
    // Initialize database
    let config = Config::new().temporary(true);
    let db = config.open()?;

    // Create some dummy records
    let record0 = ("key_0", "val_0");
    let record1 = (1_u32, 1_u32);
    let record2 = (2_u64, 2_u64);

    // Initialize tree with the dummy records
    let tree = db.open_tree(b"_tree")?;
    tree.insert(serialize(&record0.0), serialize(&record0.1))?;
    tree.insert(record1.0.to_be_bytes(), serialize(&record1.1))?;
    tree.insert(record2.0.to_be_bytes(), serialize(&record2.1))?;

    // Grab each record and verify deserialization
    let key = serialize(&record0.0);
    let serialized_value = tree.get(&key)?.unwrap();
    let deserialized_record: (String, String) = parse_record((key.into(), serialized_value))?;
    assert_eq!(record0.0, deserialized_record.0);
    assert_eq!(record0.1, deserialized_record.1);

    let key = record1.0.to_be_bytes();
    let serialized_value = tree.get(key)?.unwrap();
    let deserialized_record: (u32, u32) = parse_u32_key_record(((&key).into(), serialized_value))?;
    assert_eq!(record1.0, deserialized_record.0);
    assert_eq!(record1.1, deserialized_record.1);

    let key = record2.0.to_be_bytes();
    let serialized_value = tree.get(key)?.unwrap();
    let deserialized_record: (u64, u64) = parse_u64_key_record(((&key).into(), serialized_value))?;
    assert_eq!(record2.0, deserialized_record.0);
    assert_eq!(record2.1, deserialized_record.1);

    Ok(())
}
