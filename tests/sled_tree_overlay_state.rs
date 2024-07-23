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
//! [`sled::Tree`] instance, and perform diffs and
//! writes to verify overlay's cache diff functionality.

use sled::Config;

use sled_overlay::SledTreeOverlay;

const TREE: &[u8] = b"_tree";

#[test]
fn sled_tree_overlay_state() -> Result<(), sled::Error> {
    // Initialize database
    let config = Config::new().temporary(true);
    let db = config.open()?;

    // Initialize tree with some values and its overlay
    let tree = db.open_tree(TREE)?;
    tree.insert(b"key_a", b"val_a")?;
    let mut overlay = SledTreeOverlay::new(&tree);
    assert!(!overlay.is_empty());

    // Make a vector to keep track of changes
    let mut sequence = vec![];

    // Perform some changes and grab their differences
    overlay.insert(b"key_b", b"val_b")?;
    sequence.push(overlay.diff2(&sequence)?);

    overlay.insert(b"key_b", b"val_bb")?;
    overlay.remove(b"key_a")?;
    sequence.push(overlay.diff2(&sequence)?);

    overlay.insert(b"key_a", b"val_a")?;
    overlay.remove(b"key_b")?;
    overlay.insert(b"key_c", b"val_c")?;
    sequence.push(overlay.diff2(&sequence)?);

    // Verify overlay has the correct state
    assert_eq!(overlay.state.cache.len(), 2);
    assert_eq!(
        overlay.state.cache.get::<sled::IVec>(&b"key_a".into()),
        Some(&b"val_a".into())
    );
    assert_eq!(
        overlay.state.cache.get::<sled::IVec>(&b"key_c".into()),
        Some(&b"val_c".into())
    );
    assert_eq!(overlay.state.removed.len(), 1);
    assert_eq!(
        overlay.state.removed.get::<sled::IVec>(&b"key_b".into()),
        Some(&b"key_b".into())
    );

    // Verify diffs sequence is correct
    assert_eq!(sequence.len(), 3);

    assert_eq!(sequence[0].cache.len(), 1);
    assert_eq!(
        sequence[0].cache.get::<sled::IVec>(&b"key_b".into()),
        Some(&(None, b"val_b".into()))
    );
    assert!(sequence[0].removed.is_empty());

    assert_eq!(sequence[1].cache.len(), 1);
    assert_eq!(
        sequence[1].cache.get::<sled::IVec>(&b"key_b".into()),
        Some(&(Some(b"val_b".into()), b"val_bb".into()))
    );
    assert_eq!(sequence[1].removed.len(), 1);
    assert_eq!(
        sequence[1].removed.get::<sled::IVec>(&b"key_a".into()),
        Some(&b"val_a".into())
    );

    assert_eq!(sequence[2].cache.len(), 2);
    assert_eq!(
        sequence[2].cache.get::<sled::IVec>(&b"key_a".into()),
        Some(&(None, b"val_a".into()))
    );
    assert_eq!(
        sequence[2].cache.get::<sled::IVec>(&b"key_c".into()),
        Some(&(None, b"val_c".into()))
    );
    assert_eq!(sequence[2].removed.len(), 1);
    assert_eq!(
        sequence[2].removed.get::<sled::IVec>(&b"key_b".into()),
        Some(&b"val_bb".into())
    );

    // Now we are going to apply each diff and check that sled
    // has been mutated accordingly
    let batch = sequence[0].aggregate().unwrap();
    tree.apply_batch(batch)?;
    db.flush()?;
    assert_eq!(tree.len(), 2);
    assert_eq!(tree.get(b"key_a")?, Some(b"val_a".into()));
    assert_eq!(tree.get(b"key_b")?, Some(b"val_b".into()));
    overlay.remove_diff2(&sequence[0]);

    let batch = sequence[1].aggregate().unwrap();
    tree.apply_batch(batch)?;
    db.flush()?;
    assert_eq!(tree.len(), 1);
    assert_eq!(tree.get(b"key_a")?, None);
    assert_eq!(tree.get(b"key_b")?, Some(b"val_bb".into()));
    overlay.remove_diff2(&sequence[1]);

    // Since we removed the diffs, current overlay diff must be
    // the same as the last diff in the sequence
    let diff = overlay.diff2(&[])?;
    assert_eq!(diff, sequence[2]);
    // Therefore we can safely use its batch
    let batch = overlay.aggregate().unwrap();
    tree.apply_batch(batch)?;
    db.flush()?;
    assert_eq!(tree.len(), 2);
    assert_eq!(tree.get(b"key_a")?, Some(b"val_a".into()));
    assert_eq!(tree.get(b"key_b")?, None);
    assert_eq!(tree.get(b"key_c")?, Some(b"val_c".into()));
    overlay.remove_diff2(&sequence[2]);

    // Since we removed everything, current overlay must not have
    // diffs over the tree, therefore its safe to keep using it
    let diff = overlay.diff(&[]);
    assert!(diff.cache.is_empty());
    assert!(diff.removed.is_empty());
    let diff = overlay.diff2(&[])?;
    assert!(diff.cache.is_empty());
    assert!(diff.removed.is_empty());

    // We are going to make some changes that we want to revert
    // using the corresponding diff
    overlay.insert(b"key_a", b"val_aa")?;
    overlay.insert(b"key_b", b"val_b")?;
    overlay.remove(b"key_c")?;

    // Grab the diff, apply it and verify tree state
    let diff = overlay.diff2(&[])?;
    let batch = diff.aggregate().unwrap();
    tree.apply_batch(batch)?;
    db.flush()?;
    assert_eq!(tree.len(), 2);
    assert_eq!(tree.get(b"key_a")?, Some(b"val_aa".into()));
    assert_eq!(tree.get(b"key_b")?, Some(b"val_b".into()));
    assert_eq!(tree.get(b"key_c")?, None);

    // Now we grab the diff revert batch, apply it and verity tree state
    let batch = diff.revert().unwrap();
    tree.apply_batch(batch)?;
    db.flush()?;
    assert_eq!(tree.len(), 2);
    assert_eq!(tree.get(b"key_a")?, Some(b"val_a".into()));
    assert_eq!(tree.get(b"key_b")?, None);
    assert_eq!(tree.get(b"key_c")?, Some(b"val_c".into()));

    // Now we are going to revert the diffs sequence going backwards
    // and verify tree state mutates accordingly
    let batch = sequence[2].revert().unwrap();
    tree.apply_batch(batch)?;
    db.flush()?;
    assert_eq!(tree.len(), 1);
    assert_eq!(tree.get(b"key_a")?, None);
    assert_eq!(tree.get(b"key_b")?, Some(b"val_bb".into()));

    let batch = sequence[1].revert().unwrap();
    tree.apply_batch(batch)?;
    db.flush()?;
    assert_eq!(tree.len(), 2);
    assert_eq!(tree.get(b"key_a")?, Some(b"val_a".into()));
    assert_eq!(tree.get(b"key_b")?, Some(b"val_b".into()));

    let batch = sequence[0].revert().unwrap();
    tree.apply_batch(batch)?;
    db.flush()?;

    // Tree has now reverted to its original state
    assert_eq!(tree.len(), 1);
    assert_eq!(tree.get(b"key_a")?, Some(b"val_a".into()));

    Ok(())
}

#[test]
fn sled_tree_overlay_rebuild_state() -> Result<(), sled::Error> {
    // Initialize database
    let config = Config::new().temporary(true);
    let db = config.open()?;

    // Initialize tree with some values and its overlay
    let tree = db.open_tree(TREE)?;
    tree.insert(b"key_a", b"val_a")?;
    let mut overlay = SledTreeOverlay::new(&tree);
    assert!(!overlay.is_empty());

    // Make two vectors to keep track of changes
    let mut sequence = vec![];
    let mut state_sequence = vec![];

    // Perform some changes and grab their differences
    overlay.insert(b"key_b", b"val_b")?;
    sequence.push(overlay.diff2(&sequence)?);
    state_sequence.push(overlay.clone());

    overlay.insert(b"key_b", b"val_bb")?;
    overlay.remove(b"key_a")?;
    sequence.push(overlay.diff2(&sequence)?);
    state_sequence.push(overlay.clone());

    overlay.insert(b"key_a", b"val_a")?;
    overlay.remove(b"key_b")?;
    overlay.insert(b"key_c", b"val_c")?;
    sequence.push(overlay.diff2(&sequence)?);

    // Create a different overlay to rebuild
    // the previous one using the changes sequence
    let mut overlay2 = SledTreeOverlay::new(&tree);
    assert!(!overlay2.is_empty());

    // Add each diff from the sequence and verify
    // overlay has been mutated accordingly
    overlay2.add_diff2(&sequence[0]);
    assert_eq!(overlay2.state.cache.len(), 1);
    assert_eq!(
        overlay2.state.cache.get::<sled::IVec>(&b"key_b".into()),
        Some(&b"val_b".into())
    );
    assert!(overlay2.state.removed.is_empty());
    assert_eq!(state_sequence[0].state, overlay2.state);

    overlay2.add_diff2(&sequence[1]);
    assert_eq!(overlay2.state.cache.len(), 1);
    assert_eq!(
        overlay2.state.cache.get::<sled::IVec>(&b"key_b".into()),
        Some(&b"val_bb".into())
    );
    assert_eq!(overlay2.state.removed.len(), 1);
    assert_eq!(
        overlay2.state.removed.get::<sled::IVec>(&b"key_a".into()),
        Some(&b"key_a".into())
    );
    assert_eq!(state_sequence[1].state, overlay2.state);

    overlay2.add_diff2(&sequence[2]);
    assert_eq!(overlay2.state.cache.len(), 2);
    assert_eq!(
        overlay2.state.cache.get::<sled::IVec>(&b"key_a".into()),
        Some(&b"val_a".into())
    );
    assert_eq!(
        overlay2.state.cache.get::<sled::IVec>(&b"key_c".into()),
        Some(&b"val_c".into())
    );
    assert_eq!(overlay2.state.removed.len(), 1);
    assert_eq!(
        overlay2.state.removed.get::<sled::IVec>(&b"key_b".into()),
        Some(&b"key_b".into())
    );
    assert_eq!(overlay.state, overlay2.state);

    Ok(())
}
