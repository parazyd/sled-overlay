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
//! deserializations of its diffs and records to verify correctness.

#![cfg(feature = "async-serial")]

use darkfi_serial::{deserialize_async, serialize_async};
use sled::Config;

use sled_overlay::{
    async_serial::{parse_record_async, parse_u32_key_record_async, parse_u64_key_record_async},
    SledDbOverlay,
};

const TREE_1: &[u8] = b"_tree1";
const TREE_2: &[u8] = b"_tree2";
const TREE_3: &[u8] = b"_tree3";
const TREE_4: &[u8] = b"_tree4";
const TREE_5: &[u8] = b"_tree5";

#[test]
fn sled_db_diff_async_serialization() -> Result<(), sled::Error> {
    smol::block_on(async {
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
            let serialized = serialize_async(&diff).await;
            let deserialized = deserialize_async(&serialized).await?;
            assert_eq!(diff, deserialized);
        }

        Ok(())
    })
}

#[test]
fn sled_db_record_deserialization_async() -> Result<(), sled::Error> {
    smol::block_on(async {
        // Initialize database
        let config = Config::new().temporary(true);
        let db = config.open()?;

        // Initialize overlay
        let mut overlay = SledDbOverlay::new(&db, vec![]);

        // Open trees in the overlay
        overlay.open_tree(TREE_1, false)?;
        overlay.open_tree(TREE_2, false)?;
        overlay.open_tree(TREE_3, false)?;

        // Create some dummy records
        let record0 = ("key_0", "val_0");
        let record1 = (1_u32, 1_u32);
        let record2 = (2_u64, 2_u64);

        // Insert records to the trees
        overlay.insert(
            TREE_1,
            &serialize_async(&record0.0).await,
            &serialize_async(&record0.1).await,
        )?;
        overlay.insert(
            TREE_2,
            &record1.0.to_be_bytes(),
            &serialize_async(&record1.1).await,
        )?;
        overlay.insert(
            TREE_3,
            &record2.0.to_be_bytes(),
            &serialize_async(&record2.1).await,
        )?;

        // Grab each record and verify deserialization
        let key = serialize_async(&record0.0).await;
        let serialized_value = overlay.get(TREE_1, &key)?.unwrap();
        let deserialized_record: (String, String) =
            parse_record_async((key.into(), serialized_value)).await?;
        assert_eq!(record0.0, deserialized_record.0);
        assert_eq!(record0.1, deserialized_record.1);

        let key = record1.0.to_be_bytes();
        let serialized_value = overlay.get(TREE_2, &key)?.unwrap();
        let deserialized_record: (u32, u32) =
            parse_u32_key_record_async(((&key).into(), serialized_value)).await?;
        assert_eq!(record1.0, deserialized_record.0);
        assert_eq!(record1.1, deserialized_record.1);

        let key = record2.0.to_be_bytes();
        let serialized_value = overlay.get(TREE_3, &key)?.unwrap();
        let deserialized_record: (u64, u64) =
            parse_u64_key_record_async(((&key).into(), serialized_value)).await?;
        assert_eq!(record2.0, deserialized_record.0);
        assert_eq!(record2.1, deserialized_record.1);

        Ok(())
    })
}
