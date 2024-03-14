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
    sequence.push(overlay.diff(&sequence));

    overlay.insert(b"key_b", b"val_bb")?;
    overlay.remove(b"key_a")?;
    sequence.push(overlay.diff(&sequence));

    overlay.insert(b"key_a", b"val_a")?;
    overlay.remove(b"key_b")?;
    overlay.insert(b"key_c", b"val_c")?;
    sequence.push(overlay.diff(&sequence));

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
        Some(&b"val_b".into())
    );
    assert!(sequence[0].removed.is_empty());

    assert_eq!(sequence[1].cache.len(), 1);
    assert_eq!(
        sequence[1].cache.get::<sled::IVec>(&b"key_b".into()),
        Some(&b"val_bb".into())
    );
    assert_eq!(sequence[1].removed.len(), 1);
    assert_eq!(
        sequence[1].removed.get::<sled::IVec>(&b"key_a".into()),
        Some(&b"key_a".into())
    );

    assert_eq!(sequence[2].cache.len(), 2);
    assert_eq!(
        sequence[2].cache.get::<sled::IVec>(&b"key_a".into()),
        Some(&b"val_a".into())
    );
    assert_eq!(
        sequence[2].cache.get::<sled::IVec>(&b"key_c".into()),
        Some(&b"val_c".into())
    );
    assert_eq!(sequence[2].removed.len(), 1);
    assert_eq!(
        sequence[2].removed.get::<sled::IVec>(&b"key_b".into()),
        Some(&b"key_b".into())
    );

    // Now we are going to apply each diff and check that sled
    // has been mutated accordingly
    let batch = sequence[0].aggregate().unwrap();
    tree.apply_batch(batch)?;
    db.flush()?;
    assert_eq!(tree.len(), 2);
    assert_eq!(tree.get(b"key_a")?, Some(b"val_a".into()));
    assert_eq!(tree.get(b"key_b")?, Some(b"val_b".into()));
    overlay.remove_diff(&sequence[0]);

    let batch = sequence[1].aggregate().unwrap();
    tree.apply_batch(batch)?;
    db.flush()?;
    assert_eq!(tree.len(), 1);
    assert_eq!(tree.get(b"key_a")?, None);
    assert_eq!(tree.get(b"key_b")?, Some(b"val_bb".into()));
    overlay.remove_diff(&sequence[1]);

    // Since we removed the diffs, current overlay diff must be
    // the same as the last diff in the sequence
    let diff = overlay.diff(&[]);
    assert_eq!(diff, sequence[2]);
    // Therefore we can safely use its batch
    let batch = overlay.aggregate().unwrap();
    tree.apply_batch(batch)?;
    db.flush()?;
    assert_eq!(tree.len(), 2);
    assert_eq!(tree.get(b"key_a")?, Some(b"val_a".into()));
    assert_eq!(tree.get(b"key_b")?, None);
    assert_eq!(tree.get(b"key_c")?, Some(b"val_c".into()));
    overlay.remove_diff(&sequence[2]);

    // Since we removed everything, current overlay must not have
    // diffs over the tree, therefore its safe to keep using it
    let diff = overlay.diff(&[]);
    assert!(diff.cache.is_empty());
    assert!(diff.removed.is_empty());

    Ok(())
}
