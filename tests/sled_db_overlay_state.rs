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
//! [`sled::Db`] instance, and perform diffs and writes to verify
//! overlay's cache diff functionality.

use sled::Config;

use sled_overlay::SledDbOverlay;

const TREE_1: &[u8] = b"_tree1";
const TREE_2: &[u8] = b"_tree2";
const TREE_3: &[u8] = b"_tree3";
const TREE_4: &[u8] = b"_tree4";
const TREE_5: &[u8] = b"_tree5";

#[test]
fn sled_db_overlay_state() -> Result<(), sled::Error> {
    // Initialize database
    let config = Config::new().temporary(true);
    let db = config.open()?;

    // Initialize trees with some values
    let tree_1 = db.open_tree(TREE_1)?;
    tree_1.insert(b"key_a", b"val_a")?;
    let tree_4 = db.open_tree(TREE_4)?;
    tree_4.insert(b"key_g", b"val_g")?;

    // Initialize overlay
    let mut overlay = SledDbOverlay::new(&db);

    // Open trees in the overlay
    overlay.open_tree(TREE_1)?;
    overlay.open_tree(TREE_3)?;

    // Make a vector to keep track of changes
    let mut sequence = vec![];

    // Perform some changes and grab their differences
    overlay.insert(TREE_1, b"key_b", b"val_b")?;
    overlay.insert(TREE_3, b"key_i", b"val_i")?;
    sequence.push(overlay.diff(&sequence));

    overlay.insert(TREE_1, b"key_b", b"val_bb")?;
    overlay.remove(TREE_1, b"key_a")?;
    overlay.open_tree(TREE_2)?;
    overlay.insert(TREE_2, b"key_d", b"val_d")?;
    overlay.insert(TREE_2, b"key_e", b"val_e")?;
    overlay.drop_tree(TREE_3)?;
    overlay.open_tree(TREE_5)?;
    overlay.insert(TREE_5, b"key_h", b"val_h")?;
    sequence.push(overlay.diff(&sequence));

    overlay.insert(TREE_1, b"key_a", b"val_a")?;
    overlay.remove(TREE_1, b"key_b")?;
    overlay.insert(TREE_1, b"key_c", b"val_c")?;
    overlay.remove(TREE_2, b"key_e")?;
    overlay.insert(TREE_2, b"key_f", b"val_f")?;
    overlay.drop_tree(TREE_4)?;
    sequence.push(overlay.diff(&sequence));

    // Verify overlay has the correct state
    assert_eq!(overlay.state.initial_tree_names.len(), 3);
    assert!(overlay.state.initial_tree_names.contains(&TREE_1.into()));
    assert!(overlay.state.initial_tree_names.contains(&TREE_4.into()));
    assert_eq!(overlay.state.new_tree_names, [TREE_2, TREE_5]);
    assert_eq!(overlay.state.trees.len(), 3);
    assert!(overlay.state.trees.contains_key(TREE_1));
    assert!(overlay.state.trees.contains_key(TREE_2));
    assert!(overlay.state.trees.contains_key(TREE_5));
    assert_eq!(overlay.state.caches.len(), 3);
    assert!(overlay.state.caches.contains_key(TREE_1));
    assert!(overlay.state.caches.contains_key(TREE_2));
    assert!(overlay.state.caches.contains_key(TREE_5));
    let tree_1_cache = overlay.state.caches.get(TREE_1).unwrap();
    assert_eq!(tree_1_cache.state.cache.len(), 2);
    assert_eq!(
        tree_1_cache.state.cache.get::<sled::IVec>(&b"key_a".into()),
        Some(&b"val_a".into())
    );
    assert_eq!(
        tree_1_cache.state.cache.get::<sled::IVec>(&b"key_c".into()),
        Some(&b"val_c".into())
    );
    assert_eq!(tree_1_cache.state.removed.len(), 1);
    assert_eq!(
        tree_1_cache
            .state
            .removed
            .get::<sled::IVec>(&b"key_b".into()),
        Some(&b"key_b".into())
    );
    let tree_2_cache = overlay.state.caches.get(TREE_2).unwrap();
    assert_eq!(tree_2_cache.state.cache.len(), 2);
    assert_eq!(
        tree_2_cache.state.cache.get::<sled::IVec>(&b"key_d".into()),
        Some(&b"val_d".into())
    );
    assert_eq!(
        tree_2_cache.state.cache.get::<sled::IVec>(&b"key_f".into()),
        Some(&b"val_f".into())
    );
    assert_eq!(tree_2_cache.state.removed.len(), 1);
    assert_eq!(
        tree_2_cache
            .state
            .removed
            .get::<sled::IVec>(&b"key_e".into()),
        Some(&b"key_e".into())
    );
    let tree_5_cache = overlay.state.caches.get(TREE_5).unwrap();
    assert_eq!(tree_5_cache.state.cache.len(), 1);
    assert_eq!(
        tree_5_cache.state.cache.get::<sled::IVec>(&b"key_h".into()),
        Some(&b"val_h".into())
    );
    assert!(tree_5_cache.state.removed.is_empty());
    assert_eq!(overlay.state.dropped_tree_names, [TREE_3, TREE_4]);

    // Verify diffs sequence is correct
    assert_eq!(sequence.len(), 3);

    assert_eq!(sequence[0].initial_tree_names.len(), 3);
    assert!(sequence[0].initial_tree_names.contains(&TREE_1.into()));
    assert!(sequence[0].initial_tree_names.contains(&TREE_4.into()));
    assert_eq!(sequence[0].new_tree_names, [TREE_3]);
    assert_eq!(sequence[0].trees.len(), 2);
    assert!(sequence[0].trees.contains_key(TREE_1));
    assert!(sequence[0].trees.contains_key(TREE_3));
    assert_eq!(sequence[0].caches.len(), 2);
    let tree_1_cache = sequence[0].caches.get(TREE_1).unwrap();
    assert_eq!(tree_1_cache.state.cache.len(), 1);
    assert_eq!(
        tree_1_cache.state.cache.get::<sled::IVec>(&b"key_b".into()),
        Some(&b"val_b".into())
    );
    assert!(tree_1_cache.state.removed.is_empty());
    let tree_3_cache = sequence[0].caches.get(TREE_3).unwrap();
    assert_eq!(tree_3_cache.state.cache.len(), 1);
    assert_eq!(
        tree_3_cache.state.cache.get::<sled::IVec>(&b"key_i".into()),
        Some(&b"val_i".into())
    );
    assert!(tree_3_cache.state.removed.is_empty());
    assert!(sequence[0].dropped_tree_names.is_empty());

    assert_eq!(sequence[1].initial_tree_names.len(), 4);
    assert!(sequence[1].initial_tree_names.contains(&TREE_1.into()));
    assert!(sequence[1].initial_tree_names.contains(&TREE_4.into()));
    assert!(sequence[1].initial_tree_names.contains(&TREE_3.into()));
    assert_eq!(sequence[1].new_tree_names, [TREE_2, TREE_5]);
    assert_eq!(sequence[1].trees.len(), 3);
    assert!(sequence[1].trees.contains_key(TREE_1));
    assert!(sequence[1].trees.contains_key(TREE_2));
    assert!(sequence[1].trees.contains_key(TREE_5));
    assert_eq!(sequence[1].caches.len(), 3);
    let tree_1_cache = sequence[1].caches.get(TREE_1).unwrap();
    assert_eq!(tree_1_cache.state.cache.len(), 1);
    assert_eq!(
        tree_1_cache.state.cache.get::<sled::IVec>(&b"key_b".into()),
        Some(&b"val_bb".into())
    );
    assert_eq!(tree_1_cache.state.removed.len(), 1);
    assert_eq!(
        tree_1_cache
            .state
            .removed
            .get::<sled::IVec>(&b"key_a".into()),
        Some(&b"key_a".into())
    );
    let tree_2_cache = sequence[1].caches.get(TREE_2).unwrap();
    assert_eq!(tree_2_cache.state.cache.len(), 2);
    assert_eq!(
        tree_2_cache.state.cache.get::<sled::IVec>(&b"key_d".into()),
        Some(&b"val_d".into())
    );
    assert_eq!(
        tree_2_cache.state.cache.get::<sled::IVec>(&b"key_e".into()),
        Some(&b"val_e".into())
    );
    assert!(tree_2_cache.state.removed.is_empty());
    let tree_5_cache = sequence[1].caches.get(TREE_5).unwrap();
    assert_eq!(tree_5_cache.state.cache.len(), 1);
    assert_eq!(
        tree_5_cache.state.cache.get::<sled::IVec>(&b"key_h".into()),
        Some(&b"val_h".into())
    );
    assert!(tree_5_cache.state.removed.is_empty());
    assert_eq!(sequence[1].dropped_tree_names, [TREE_3]);

    assert_eq!(sequence[2].initial_tree_names.len(), 5);
    assert!(sequence[2].initial_tree_names.contains(&TREE_1.into()));
    assert!(sequence[2].initial_tree_names.contains(&TREE_4.into()));
    assert!(sequence[2].initial_tree_names.contains(&TREE_2.into()));
    assert!(sequence[2].initial_tree_names.contains(&TREE_5.into()));
    assert!(sequence[2].new_tree_names.is_empty());
    assert_eq!(sequence[2].trees.len(), 2);
    assert!(sequence[2].trees.contains_key(TREE_1));
    assert!(sequence[2].trees.contains_key(TREE_2));
    assert_eq!(sequence[2].caches.len(), 2);
    let tree_1_cache = sequence[2].caches.get(TREE_1).unwrap();
    assert_eq!(tree_1_cache.state.cache.len(), 2);
    assert_eq!(
        tree_1_cache.state.cache.get::<sled::IVec>(&b"key_a".into()),
        Some(&b"val_a".into())
    );
    assert_eq!(
        tree_1_cache.state.cache.get::<sled::IVec>(&b"key_c".into()),
        Some(&b"val_c".into())
    );
    assert_eq!(tree_1_cache.state.removed.len(), 1);
    assert_eq!(
        tree_1_cache
            .state
            .removed
            .get::<sled::IVec>(&b"key_b".into()),
        Some(&b"key_b".into())
    );
    let tree_2_cache = sequence[2].caches.get(TREE_2).unwrap();
    assert_eq!(tree_2_cache.state.cache.len(), 1);
    assert_eq!(
        tree_2_cache.state.cache.get::<sled::IVec>(&b"key_f".into()),
        Some(&b"val_f".into())
    );
    assert_eq!(tree_2_cache.state.removed.len(), 1);
    assert_eq!(
        tree_2_cache
            .state
            .removed
            .get::<sled::IVec>(&b"key_e".into()),
        Some(&b"key_e".into())
    );
    assert_eq!(sequence[2].dropped_tree_names, [TREE_4]);

    // Now we are going to apply each diff and check that sled
    // and the overlay have been mutated accordingly.
    // Don't forget to flush.
    assert_eq!(overlay.apply_diff(&mut sequence[0]), Ok(()));
    db.flush()?;

    // All trees should be present in sled
    let db_tree_names = db.tree_names();
    assert_eq!(db_tree_names.len(), 6);
    assert!(db_tree_names.contains(&TREE_1.into()));
    assert!(db_tree_names.contains(&TREE_2.into()));
    assert!(db_tree_names.contains(&TREE_3.into()));
    assert!(db_tree_names.contains(&TREE_4.into()));
    assert!(db_tree_names.contains(&TREE_5.into()));

    let tree_1 = db.open_tree(TREE_1)?;
    assert_eq!(tree_1.len(), 2);
    assert_eq!(tree_1.get(b"key_a")?, Some(b"val_a".into()));
    assert_eq!(tree_1.get(b"key_b")?, Some(b"val_b".into()));
    let tree_2 = db.open_tree(TREE_2)?;
    assert!(tree_2.is_empty());
    let tree_3 = db.open_tree(TREE_3)?;
    assert_eq!(tree_3.len(), 1);
    assert_eq!(tree_3.get(b"key_i")?, Some(b"val_i".into()));
    let tree_4 = db.open_tree(TREE_4)?;
    assert_eq!(tree_4.len(), 1);
    assert_eq!(tree_4.get(b"key_g")?, Some(b"val_g".into()));
    let tree_5 = db.open_tree(TREE_5)?;
    assert!(tree_5.is_empty());

    assert_eq!(overlay.state.initial_tree_names.len(), 4);
    assert!(overlay.state.initial_tree_names.contains(&TREE_1.into()));
    assert!(overlay.state.initial_tree_names.contains(&TREE_4.into()));
    assert!(overlay.state.initial_tree_names.contains(&TREE_3.into()));
    assert_eq!(overlay.state.new_tree_names, [TREE_2, TREE_5]);
    assert_eq!(overlay.state.trees.len(), 3);
    assert!(overlay.state.trees.contains_key(TREE_1));
    assert!(overlay.state.trees.contains_key(TREE_2));
    assert!(overlay.state.trees.contains_key(TREE_5));
    assert_eq!(overlay.state.caches.len(), 3);
    assert!(overlay.state.caches.contains_key(TREE_1));
    assert!(overlay.state.caches.contains_key(TREE_2));
    assert!(overlay.state.caches.contains_key(TREE_5));
    let tree_1_cache = overlay.state.caches.get(TREE_1).unwrap();
    assert_eq!(tree_1_cache.state.cache.len(), 2);
    assert_eq!(
        tree_1_cache.state.cache.get::<sled::IVec>(&b"key_a".into()),
        Some(&b"val_a".into())
    );
    assert_eq!(
        tree_1_cache.state.cache.get::<sled::IVec>(&b"key_c".into()),
        Some(&b"val_c".into())
    );
    assert_eq!(tree_1_cache.state.removed.len(), 1);
    assert_eq!(
        tree_1_cache
            .state
            .removed
            .get::<sled::IVec>(&b"key_b".into()),
        Some(&b"key_b".into())
    );
    let tree_2_cache = overlay.state.caches.get(TREE_2).unwrap();
    assert_eq!(tree_2_cache.state.cache.len(), 2);
    assert_eq!(
        tree_2_cache.state.cache.get::<sled::IVec>(&b"key_d".into()),
        Some(&b"val_d".into())
    );
    assert_eq!(
        tree_2_cache.state.cache.get::<sled::IVec>(&b"key_f".into()),
        Some(&b"val_f".into())
    );
    assert_eq!(tree_2_cache.state.removed.len(), 1);
    assert_eq!(
        tree_2_cache
            .state
            .removed
            .get::<sled::IVec>(&b"key_e".into()),
        Some(&b"key_e".into())
    );
    let tree_5_cache = overlay.state.caches.get(TREE_5).unwrap();
    assert_eq!(tree_5_cache.state.cache.len(), 1);
    assert_eq!(
        tree_5_cache.state.cache.get::<sled::IVec>(&b"key_h".into()),
        Some(&b"val_h".into())
    );
    assert!(tree_5_cache.state.removed.is_empty());
    assert_eq!(overlay.state.dropped_tree_names, [TREE_3, TREE_4]);

    assert_eq!(overlay.apply_diff(&mut sequence[1]), Ok(()));
    db.flush()?;

    // All trees should be present in sled
    let db_tree_names = db.tree_names();
    assert_eq!(db_tree_names.len(), 5);
    assert!(db_tree_names.contains(&TREE_1.into()));
    assert!(db_tree_names.contains(&TREE_2.into()));
    assert!(db_tree_names.contains(&TREE_4.into()));
    assert!(db_tree_names.contains(&TREE_5.into()));

    let tree_1 = db.open_tree(TREE_1)?;
    assert_eq!(tree_1.len(), 1);
    assert_eq!(tree_1.get(b"key_b")?, Some(b"val_bb".into()));
    let tree_2 = db.open_tree(TREE_2)?;
    assert_eq!(tree_2.len(), 2);
    assert_eq!(tree_2.get(b"key_d")?, Some(b"val_d".into()));
    assert_eq!(tree_2.get(b"key_e")?, Some(b"val_e".into()));
    let tree_4 = db.open_tree(TREE_4)?;
    assert_eq!(tree_4.len(), 1);
    assert_eq!(tree_4.get(b"key_g")?, Some(b"val_g".into()));
    let tree_5 = db.open_tree(TREE_5)?;
    assert_eq!(tree_5.len(), 1);
    assert_eq!(tree_5.get(b"key_h")?, Some(b"val_h".into()));

    assert_eq!(overlay.state.initial_tree_names.len(), 5);
    assert!(overlay.state.initial_tree_names.contains(&TREE_1.into()));
    assert!(overlay.state.initial_tree_names.contains(&TREE_4.into()));
    assert!(overlay.state.initial_tree_names.contains(&TREE_2.into()));
    assert!(overlay.state.initial_tree_names.contains(&TREE_5.into()));
    assert!(overlay.state.new_tree_names.is_empty());
    // Tree 5 was stale so it should have been closed
    assert_eq!(overlay.state.trees.len(), 2);
    assert!(overlay.state.trees.contains_key(TREE_1));
    assert!(overlay.state.trees.contains_key(TREE_2));
    assert_eq!(overlay.state.caches.len(), 2);
    assert!(overlay.state.caches.contains_key(TREE_1));
    assert!(overlay.state.caches.contains_key(TREE_2));
    let tree_1_cache = overlay.state.caches.get(TREE_1).unwrap();
    assert_eq!(tree_1_cache.state.cache.len(), 2);
    assert_eq!(
        tree_1_cache.state.cache.get::<sled::IVec>(&b"key_a".into()),
        Some(&b"val_a".into())
    );
    assert_eq!(
        tree_1_cache.state.cache.get::<sled::IVec>(&b"key_c".into()),
        Some(&b"val_c".into())
    );
    assert_eq!(tree_1_cache.state.removed.len(), 1);
    assert_eq!(
        tree_1_cache
            .state
            .removed
            .get::<sled::IVec>(&b"key_b".into()),
        Some(&b"key_b".into())
    );
    let tree_2_cache = overlay.state.caches.get(TREE_2).unwrap();
    assert_eq!(tree_2_cache.state.cache.len(), 1);
    assert_eq!(
        tree_2_cache.state.cache.get::<sled::IVec>(&b"key_f".into()),
        Some(&b"val_f".into())
    );
    assert_eq!(tree_2_cache.state.removed.len(), 1);
    assert_eq!(
        tree_2_cache
            .state
            .removed
            .get::<sled::IVec>(&b"key_e".into()),
        Some(&b"key_e".into())
    );
    assert_eq!(overlay.state.dropped_tree_names, [TREE_4]);

    assert_eq!(overlay.apply_diff(&mut sequence[2]), Ok(()));
    db.flush()?;

    // All trees should be present in sled
    let db_tree_names = db.tree_names();
    assert_eq!(db_tree_names.len(), 4);
    assert!(db_tree_names.contains(&TREE_1.into()));
    assert!(db_tree_names.contains(&TREE_2.into()));
    assert!(db_tree_names.contains(&TREE_5.into()));

    let tree_1 = db.open_tree(TREE_1)?;
    assert_eq!(tree_1.len(), 2);
    assert_eq!(tree_1.get(b"key_a")?, Some(b"val_a".into()));
    assert_eq!(tree_1.get(b"key_c")?, Some(b"val_c".into()));
    let tree_2 = db.open_tree(TREE_2)?;
    assert_eq!(tree_2.len(), 2);
    assert_eq!(tree_2.get(b"key_d")?, Some(b"val_d".into()));
    assert_eq!(tree_2.get(b"key_f")?, Some(b"val_f".into()));
    let tree_5 = db.open_tree(TREE_5)?;
    assert_eq!(tree_5.len(), 1);
    assert_eq!(tree_5.get(b"key_h")?, Some(b"val_h".into()));

    // Since we removed everything, current overlay must not have
    // diffs over the tree, therefore its safe to keep using it
    assert_eq!(overlay.state.initial_tree_names.len(), 4);
    assert!(overlay.state.initial_tree_names.contains(&TREE_1.into()));
    assert!(overlay.state.initial_tree_names.contains(&TREE_2.into()));
    assert!(overlay.state.initial_tree_names.contains(&TREE_5.into()));
    assert!(overlay.state.new_tree_names.is_empty());
    assert!(overlay.state.trees.is_empty());
    assert!(overlay.state.caches.is_empty());
    assert!(overlay.state.dropped_tree_names.is_empty());

    Ok(())
}

#[test]
fn sled_db_overlay_rebuild_state() -> Result<(), sled::Error> {
    // Initialize database
    let config = Config::new().temporary(true);
    let db = config.open()?;

    // Initialize trees with some values
    let tree_1 = db.open_tree(TREE_1)?;
    tree_1.insert(b"key_a", b"val_a")?;
    let tree_4 = db.open_tree(TREE_4)?;
    tree_4.insert(b"key_g", b"val_g")?;

    // Initialize overlay
    let mut overlay = SledDbOverlay::new(&db);

    // Open trees in the overlay
    overlay.open_tree(TREE_1)?;
    overlay.open_tree(TREE_3)?;

    // Make a vector to keep track of changes
    let mut sequence = vec![];

    // Perform some changes and grab their differences
    overlay.insert(TREE_1, b"key_b", b"val_b")?;
    overlay.insert(TREE_3, b"key_i", b"val_i")?;
    sequence.push(overlay.diff(&sequence));

    overlay.insert(TREE_1, b"key_b", b"val_bb")?;
    overlay.remove(TREE_1, b"key_a")?;
    overlay.open_tree(TREE_2)?;
    overlay.insert(TREE_2, b"key_d", b"val_d")?;
    overlay.insert(TREE_2, b"key_e", b"val_e")?;
    overlay.drop_tree(TREE_3)?;
    sequence.push(overlay.diff(&sequence));

    overlay.insert(TREE_1, b"key_a", b"val_a")?;
    overlay.remove(TREE_1, b"key_b")?;
    overlay.insert(TREE_1, b"key_c", b"val_c")?;
    overlay.remove(TREE_2, b"key_e")?;
    overlay.insert(TREE_2, b"key_f", b"val_f")?;
    overlay.drop_tree(TREE_4)?;
    overlay.open_tree(TREE_5)?;
    overlay.insert(TREE_5, b"key_h", b"val_h")?;
    sequence.push(overlay.diff(&sequence));

    // Create a different overlay to rebuild
    // the previous one using the changes sequence
    let mut overlay2 = SledDbOverlay::new(&db);
    // All trees should be present in sled
    assert_eq!(overlay2.state.initial_tree_names.len(), 6);
    assert!(overlay2.state.initial_tree_names.contains(&TREE_1.into()));
    assert!(overlay2.state.initial_tree_names.contains(&TREE_2.into()));
    assert!(overlay2.state.initial_tree_names.contains(&TREE_3.into()));
    assert!(overlay2.state.initial_tree_names.contains(&TREE_4.into()));
    assert!(overlay2.state.initial_tree_names.contains(&TREE_5.into()));
    assert!(overlay2.state.new_tree_names.is_empty());
    assert!(overlay2.state.trees.is_empty());
    assert!(overlay2.state.caches.is_empty());
    assert!(overlay2.state.dropped_tree_names.is_empty());

    // Add each diff from the sequence and verify
    // overlay has been mutated accordingly
    overlay2.add_diff(&sequence[0]);
    assert_eq!(overlay2.state.initial_tree_names.len(), 3);
    assert!(overlay2.state.initial_tree_names.contains(&TREE_1.into()));
    assert!(overlay2.state.initial_tree_names.contains(&TREE_4.into()));
    assert_eq!(overlay2.state.new_tree_names, [TREE_3]);
    assert_eq!(overlay2.state.trees.len(), 2);
    assert!(overlay2.state.trees.contains_key(TREE_1));
    assert!(overlay2.state.trees.contains_key(TREE_3));
    assert_eq!(overlay2.state.caches.len(), 2);
    assert!(overlay2.state.caches.contains_key(TREE_1));
    assert!(overlay2.state.caches.contains_key(TREE_3));
    let tree_1_cache = overlay2.state.caches.get(TREE_1).unwrap();
    assert_eq!(tree_1_cache.state.cache.len(), 1);
    assert_eq!(
        tree_1_cache.state.cache.get::<sled::IVec>(&b"key_b".into()),
        Some(&b"val_b".into())
    );
    assert!(tree_1_cache.state.removed.is_empty());
    let tree_3_cache = overlay2.state.caches.get(TREE_3).unwrap();
    assert_eq!(tree_3_cache.state.cache.len(), 1);
    assert_eq!(
        tree_3_cache.state.cache.get::<sled::IVec>(&b"key_i".into()),
        Some(&b"val_i".into())
    );
    assert!(tree_3_cache.state.removed.is_empty());
    assert!(overlay2.state.dropped_tree_names.is_empty());

    overlay2.add_diff(&sequence[1]);
    assert_eq!(overlay2.state.initial_tree_names.len(), 3);
    assert!(overlay2.state.initial_tree_names.contains(&TREE_1.into()));
    assert!(overlay2.state.initial_tree_names.contains(&TREE_4.into()));
    assert_eq!(overlay2.state.new_tree_names, [TREE_2]);
    assert_eq!(overlay2.state.trees.len(), 2);
    assert!(overlay2.state.trees.contains_key(TREE_1));
    assert!(overlay2.state.trees.contains_key(TREE_2));
    assert_eq!(overlay2.state.caches.len(), 2);
    assert!(overlay2.state.caches.contains_key(TREE_1));
    assert!(overlay2.state.caches.contains_key(TREE_2));
    let tree_1_cache = overlay2.state.caches.get(TREE_1).unwrap();
    assert_eq!(tree_1_cache.state.cache.len(), 1);
    assert_eq!(
        tree_1_cache.state.cache.get::<sled::IVec>(&b"key_b".into()),
        Some(&b"val_bb".into())
    );
    assert_eq!(tree_1_cache.state.removed.len(), 1);
    assert_eq!(
        tree_1_cache
            .state
            .removed
            .get::<sled::IVec>(&b"key_a".into()),
        Some(&b"key_a".into())
    );
    let tree_2_cache = overlay2.state.caches.get(TREE_2).unwrap();
    assert_eq!(tree_2_cache.state.cache.len(), 2);
    assert_eq!(
        tree_2_cache.state.cache.get::<sled::IVec>(&b"key_d".into()),
        Some(&b"val_d".into())
    );
    assert_eq!(
        tree_2_cache.state.cache.get::<sled::IVec>(&b"key_e".into()),
        Some(&b"val_e".into())
    );
    assert!(tree_2_cache.state.removed.is_empty());
    assert_eq!(overlay2.state.dropped_tree_names, [TREE_3]);

    // Deviate and create some new records in the overlay
    overlay2.insert(TREE_1, b"key_c", b"val_cc")?;
    overlay2.remove(TREE_2, b"key_e")?;
    overlay2.open_tree(TREE_4)?;
    overlay2.insert(TREE_4, b"key_f", b"val_f")?;

    // Verify overlay2 has the correct state
    assert_eq!(overlay2.state.initial_tree_names.len(), 3);
    assert!(overlay2.state.initial_tree_names.contains(&TREE_1.into()));
    assert!(overlay2.state.initial_tree_names.contains(&TREE_4.into()));
    assert_eq!(overlay2.state.new_tree_names, [TREE_2]);
    assert_eq!(overlay2.state.trees.len(), 3);
    assert!(overlay2.state.trees.contains_key(TREE_1));
    assert!(overlay2.state.trees.contains_key(TREE_2));
    assert!(overlay2.state.trees.contains_key(TREE_4));
    assert_eq!(overlay2.state.caches.len(), 3);
    assert!(overlay2.state.caches.contains_key(TREE_1));
    assert!(overlay2.state.caches.contains_key(TREE_2));
    assert!(overlay2.state.caches.contains_key(TREE_4));
    let tree_1_cache = overlay2.state.caches.get(TREE_1).unwrap();
    assert_eq!(tree_1_cache.state.cache.len(), 2);
    assert_eq!(
        tree_1_cache.state.cache.get::<sled::IVec>(&b"key_b".into()),
        Some(&b"val_bb".into())
    );
    assert_eq!(
        tree_1_cache.state.cache.get::<sled::IVec>(&b"key_c".into()),
        Some(&b"val_cc".into())
    );
    assert_eq!(tree_1_cache.state.removed.len(), 1);
    assert_eq!(
        tree_1_cache
            .state
            .removed
            .get::<sled::IVec>(&b"key_a".into()),
        Some(&b"key_a".into())
    );
    let tree_2_cache = overlay2.state.caches.get(TREE_2).unwrap();
    assert_eq!(tree_2_cache.state.cache.len(), 1);
    assert_eq!(
        tree_2_cache.state.cache.get::<sled::IVec>(&b"key_d".into()),
        Some(&b"val_d".into())
    );
    assert_eq!(tree_2_cache.state.removed.len(), 1);
    assert_eq!(
        tree_2_cache
            .state
            .removed
            .get::<sled::IVec>(&b"key_e".into()),
        Some(&b"key_e".into())
    );
    let tree_4_cache = overlay2.state.caches.get(TREE_4).unwrap();
    assert_eq!(tree_4_cache.state.cache.len(), 1);
    assert_eq!(
        tree_4_cache.state.cache.get::<sled::IVec>(&b"key_f".into()),
        Some(&b"val_f".into())
    );
    assert!(tree_4_cache.state.removed.is_empty());
    assert_eq!(overlay2.state.dropped_tree_names, [TREE_3]);

    // Now we are going to apply each diff and check that sled
    // and the overlays have been mutated accordingly.
    // Don't forget to flush.
    assert_eq!(overlay.apply_diff(&mut sequence[0]), Ok(()));
    overlay2.remove_diff(&mut sequence[0]);
    db.flush()?;

    // All trees should be present in sled
    let db_tree_names = db.tree_names();
    assert_eq!(db_tree_names.len(), 6);
    assert!(db_tree_names.contains(&TREE_1.into()));
    assert!(db_tree_names.contains(&TREE_2.into()));
    assert!(db_tree_names.contains(&TREE_3.into()));
    assert!(db_tree_names.contains(&TREE_4.into()));
    assert!(db_tree_names.contains(&TREE_5.into()));

    let tree_1 = db.open_tree(TREE_1)?;
    assert_eq!(tree_1.len(), 2);
    assert_eq!(tree_1.get(b"key_a")?, Some(b"val_a".into()));
    assert_eq!(tree_1.get(b"key_b")?, Some(b"val_b".into()));
    let tree_2 = db.open_tree(TREE_2)?;
    assert!(tree_2.is_empty());
    let tree_3 = db.open_tree(TREE_3)?;
    assert_eq!(tree_3.len(), 1);
    assert_eq!(tree_3.get(b"key_i")?, Some(b"val_i".into()));
    let tree_4 = db.open_tree(TREE_4)?;
    assert_eq!(tree_4.len(), 1);
    assert_eq!(tree_4.get(b"key_g")?, Some(b"val_g".into()));
    let tree_5 = db.open_tree(TREE_5)?;
    assert!(tree_5.is_empty());

    assert_eq!(overlay.state.initial_tree_names.len(), 4);
    assert!(overlay.state.initial_tree_names.contains(&TREE_1.into()));
    assert!(overlay.state.initial_tree_names.contains(&TREE_4.into()));
    assert!(overlay.state.initial_tree_names.contains(&TREE_3.into()));
    assert_eq!(overlay.state.new_tree_names, [TREE_2, TREE_5]);
    assert_eq!(overlay.state.trees.len(), 3);
    assert!(overlay.state.trees.contains_key(TREE_1));
    assert!(overlay.state.trees.contains_key(TREE_2));
    assert!(overlay.state.trees.contains_key(TREE_5));
    assert_eq!(overlay.state.caches.len(), 3);
    assert!(overlay.state.caches.contains_key(TREE_1));
    assert!(overlay.state.caches.contains_key(TREE_2));
    assert!(overlay.state.caches.contains_key(TREE_5));
    let tree_1_cache = overlay.state.caches.get(TREE_1).unwrap();
    assert_eq!(tree_1_cache.state.cache.len(), 2);
    assert_eq!(
        tree_1_cache.state.cache.get::<sled::IVec>(&b"key_a".into()),
        Some(&b"val_a".into())
    );
    assert_eq!(
        tree_1_cache.state.cache.get::<sled::IVec>(&b"key_c".into()),
        Some(&b"val_c".into())
    );
    assert_eq!(tree_1_cache.state.removed.len(), 1);
    assert_eq!(
        tree_1_cache
            .state
            .removed
            .get::<sled::IVec>(&b"key_b".into()),
        Some(&b"key_b".into())
    );
    let tree_2_cache = overlay.state.caches.get(TREE_2).unwrap();
    assert_eq!(tree_2_cache.state.cache.len(), 2);
    assert_eq!(
        tree_2_cache.state.cache.get::<sled::IVec>(&b"key_d".into()),
        Some(&b"val_d".into())
    );
    assert_eq!(
        tree_2_cache.state.cache.get::<sled::IVec>(&b"key_f".into()),
        Some(&b"val_f".into())
    );
    assert_eq!(tree_2_cache.state.removed.len(), 1);
    assert_eq!(
        tree_2_cache
            .state
            .removed
            .get::<sled::IVec>(&b"key_e".into()),
        Some(&b"key_e".into())
    );
    let tree_5_cache = overlay.state.caches.get(TREE_5).unwrap();
    assert_eq!(tree_5_cache.state.cache.len(), 1);
    assert_eq!(
        tree_5_cache.state.cache.get::<sled::IVec>(&b"key_h".into()),
        Some(&b"val_h".into())
    );
    assert!(tree_5_cache.state.removed.is_empty());
    assert_eq!(overlay.state.dropped_tree_names, [TREE_3, TREE_4]);

    assert_eq!(overlay2.state.initial_tree_names.len(), 4);
    assert!(overlay2.state.initial_tree_names.contains(&TREE_1.into()));
    assert!(overlay2.state.initial_tree_names.contains(&TREE_4.into()));
    assert!(overlay2.state.initial_tree_names.contains(&TREE_3.into()));
    assert_eq!(overlay2.state.new_tree_names, [TREE_2]);
    assert_eq!(overlay2.state.trees.len(), 3);
    assert!(overlay2.state.trees.contains_key(TREE_1));
    assert!(overlay2.state.trees.contains_key(TREE_2));
    assert!(overlay2.state.trees.contains_key(TREE_4));
    assert_eq!(overlay2.state.caches.len(), 3);
    assert!(overlay2.state.caches.contains_key(TREE_1));
    assert!(overlay2.state.caches.contains_key(TREE_2));
    assert!(overlay2.state.caches.contains_key(TREE_4));
    let tree_1_cache = overlay2.state.caches.get(TREE_1).unwrap();
    assert_eq!(tree_1_cache.state.cache.len(), 2);
    assert_eq!(
        tree_1_cache.state.cache.get::<sled::IVec>(&b"key_b".into()),
        Some(&b"val_bb".into())
    );
    assert_eq!(
        tree_1_cache.state.cache.get::<sled::IVec>(&b"key_c".into()),
        Some(&b"val_cc".into())
    );
    assert_eq!(tree_1_cache.state.removed.len(), 1);
    assert_eq!(
        tree_1_cache
            .state
            .removed
            .get::<sled::IVec>(&b"key_a".into()),
        Some(&b"key_a".into())
    );
    let tree_2_cache = overlay2.state.caches.get(TREE_2).unwrap();
    assert_eq!(tree_2_cache.state.cache.len(), 1);
    assert_eq!(
        tree_2_cache.state.cache.get::<sled::IVec>(&b"key_d".into()),
        Some(&b"val_d".into())
    );
    assert_eq!(tree_2_cache.state.removed.len(), 1);
    assert_eq!(
        tree_2_cache
            .state
            .removed
            .get::<sled::IVec>(&b"key_e".into()),
        Some(&b"key_e".into())
    );
    let tree_4_cache = overlay2.state.caches.get(TREE_4).unwrap();
    assert_eq!(tree_4_cache.state.cache.len(), 1);
    assert_eq!(
        tree_4_cache.state.cache.get::<sled::IVec>(&b"key_f".into()),
        Some(&b"val_f".into())
    );
    assert!(tree_4_cache.state.removed.is_empty());
    assert_eq!(overlay2.state.dropped_tree_names, [TREE_3]);

    assert_eq!(overlay.apply_diff(&mut sequence[1]), Ok(()));
    overlay2.remove_diff(&mut sequence[1]);
    db.flush()?;

    // All trees should be present in sled
    let db_tree_names = db.tree_names();
    assert_eq!(db_tree_names.len(), 5);
    assert!(db_tree_names.contains(&TREE_1.into()));
    assert!(db_tree_names.contains(&TREE_2.into()));
    assert!(db_tree_names.contains(&TREE_4.into()));
    assert!(db_tree_names.contains(&TREE_5.into()));

    let tree_1 = db.open_tree(TREE_1)?;
    assert_eq!(tree_1.len(), 1);
    assert_eq!(tree_1.get(b"key_b")?, Some(b"val_bb".into()));
    let tree_2 = db.open_tree(TREE_2)?;
    assert_eq!(tree_2.len(), 2);
    assert_eq!(tree_2.get(b"key_d")?, Some(b"val_d".into()));
    assert_eq!(tree_2.get(b"key_e")?, Some(b"val_e".into()));
    let tree_4 = db.open_tree(TREE_4)?;
    assert_eq!(tree_4.len(), 1);
    assert_eq!(tree_4.get(b"key_g")?, Some(b"val_g".into()));

    assert_eq!(overlay.state.initial_tree_names.len(), 4);
    assert!(overlay.state.initial_tree_names.contains(&TREE_1.into()));
    assert!(overlay.state.initial_tree_names.contains(&TREE_2.into()));
    assert!(overlay.state.initial_tree_names.contains(&TREE_4.into()));
    assert_eq!(overlay.state.new_tree_names, [TREE_5]);
    assert_eq!(overlay.state.trees.len(), 3);
    assert!(overlay.state.trees.contains_key(TREE_1));
    assert!(overlay.state.trees.contains_key(TREE_2));
    assert!(overlay.state.trees.contains_key(TREE_5));
    assert_eq!(overlay.state.caches.len(), 3);
    assert!(overlay.state.caches.contains_key(TREE_1));
    assert!(overlay.state.caches.contains_key(TREE_2));
    assert!(overlay.state.caches.contains_key(TREE_5));
    let tree_1_cache = overlay.state.caches.get(TREE_1).unwrap();
    assert_eq!(tree_1_cache.state.cache.len(), 2);
    assert_eq!(
        tree_1_cache.state.cache.get::<sled::IVec>(&b"key_a".into()),
        Some(&b"val_a".into())
    );
    assert_eq!(
        tree_1_cache.state.cache.get::<sled::IVec>(&b"key_c".into()),
        Some(&b"val_c".into())
    );
    assert_eq!(tree_1_cache.state.removed.len(), 1);
    assert_eq!(
        tree_1_cache
            .state
            .removed
            .get::<sled::IVec>(&b"key_b".into()),
        Some(&b"key_b".into())
    );
    let tree_2_cache = overlay.state.caches.get(TREE_2).unwrap();
    assert_eq!(tree_2_cache.state.cache.len(), 1);
    assert_eq!(
        tree_2_cache.state.cache.get::<sled::IVec>(&b"key_f".into()),
        Some(&b"val_f".into())
    );
    assert_eq!(tree_2_cache.state.removed.len(), 1);
    assert_eq!(
        tree_2_cache
            .state
            .removed
            .get::<sled::IVec>(&b"key_e".into()),
        Some(&b"key_e".into())
    );
    let tree_5_cache = overlay.state.caches.get(TREE_5).unwrap();
    assert_eq!(tree_5_cache.state.cache.len(), 1);
    assert_eq!(
        tree_5_cache.state.cache.get::<sled::IVec>(&b"key_h".into()),
        Some(&b"val_h".into())
    );
    assert!(tree_5_cache.state.removed.is_empty());
    assert_eq!(overlay.state.dropped_tree_names, [TREE_4]);

    assert_eq!(overlay2.state.initial_tree_names.len(), 4);
    assert!(overlay2.state.initial_tree_names.contains(&TREE_1.into()));
    assert!(overlay2.state.initial_tree_names.contains(&TREE_4.into()));
    assert!(overlay2.state.initial_tree_names.contains(&TREE_2.into()));
    assert!(overlay2.state.new_tree_names.is_empty());
    assert_eq!(overlay2.state.trees.len(), 3);
    assert!(overlay2.state.trees.contains_key(TREE_1));
    assert!(overlay2.state.trees.contains_key(TREE_2));
    assert!(overlay2.state.trees.contains_key(TREE_4));
    assert_eq!(overlay2.state.caches.len(), 3);
    assert!(overlay2.state.caches.contains_key(TREE_1));
    assert!(overlay2.state.caches.contains_key(TREE_2));
    assert!(overlay2.state.caches.contains_key(TREE_4));
    let tree_1_cache = overlay2.state.caches.get(TREE_1).unwrap();
    assert_eq!(tree_1_cache.state.cache.len(), 1);
    assert_eq!(
        tree_1_cache.state.cache.get::<sled::IVec>(&b"key_c".into()),
        Some(&b"val_cc".into())
    );
    assert!(tree_1_cache.state.removed.is_empty());
    let tree_2_cache = overlay2.state.caches.get(TREE_2).unwrap();
    assert!(tree_2_cache.state.cache.is_empty());
    assert_eq!(tree_2_cache.state.removed.len(), 1);
    assert_eq!(
        tree_2_cache
            .state
            .removed
            .get::<sled::IVec>(&b"key_e".into()),
        Some(&b"key_e".into())
    );
    let tree_4_cache = overlay2.state.caches.get(TREE_4).unwrap();
    assert_eq!(tree_4_cache.state.cache.len(), 1);
    assert_eq!(
        tree_4_cache.state.cache.get::<sled::IVec>(&b"key_f".into()),
        Some(&b"val_f".into())
    );
    assert!(tree_4_cache.state.removed.is_empty());
    assert!(overlay2.state.dropped_tree_names.is_empty());

    // We chose to follow the second overlay, so we apply its diff
    assert_eq!(overlay2.apply_diff(&mut overlay2.diff(&[])), Ok(()));
    db.flush()?;

    // All trees should be present in sled
    let db_tree_names = db.tree_names();
    assert_eq!(db_tree_names.len(), 5);
    assert!(db_tree_names.contains(&TREE_1.into()));
    assert!(db_tree_names.contains(&TREE_2.into()));
    assert!(db_tree_names.contains(&TREE_4.into()));
    assert!(db_tree_names.contains(&TREE_5.into()));

    let tree_1 = db.open_tree(TREE_1)?;
    assert_eq!(tree_1.len(), 2);
    assert_eq!(tree_1.get(b"key_b")?, Some(b"val_bb".into()));
    assert_eq!(tree_1.get(b"key_c")?, Some(b"val_cc".into()));
    let tree_2 = db.open_tree(TREE_2)?;
    assert_eq!(tree_2.len(), 1);
    assert_eq!(tree_2.get(b"key_d")?, Some(b"val_d".into()));
    let tree_4 = db.open_tree(TREE_4)?;
    assert_eq!(tree_4.len(), 2);
    assert_eq!(tree_4.get(b"key_g")?, Some(b"val_g".into()));
    assert_eq!(tree_4.get(b"key_f")?, Some(b"val_f".into()));

    // Since we removed everything, current overlay must not have
    // diffs over the tree, therefore its safe to keep using it
    assert_eq!(overlay2.state.initial_tree_names.len(), 4);
    assert!(overlay2.state.initial_tree_names.contains(&TREE_1.into()));
    assert!(overlay2.state.initial_tree_names.contains(&TREE_2.into()));
    assert!(overlay2.state.initial_tree_names.contains(&TREE_4.into()));
    assert!(overlay2.state.new_tree_names.is_empty());
    assert!(overlay2.state.trees.is_empty());
    assert!(overlay2.state.caches.is_empty());
    assert!(overlay2.state.dropped_tree_names.is_empty());

    // Since we used overlay2, we must drop the new trees from original
    // overlay that are not present now, and stop using it.
    db.drop_tree(&TREE_5)?;
    let db_tree_names = db.tree_names();
    assert_eq!(db_tree_names.len(), 4);
    assert!(db_tree_names.contains(&TREE_1.into()));
    assert!(db_tree_names.contains(&TREE_2.into()));
    assert!(db_tree_names.contains(&TREE_4.into()));

    Ok(())
}
