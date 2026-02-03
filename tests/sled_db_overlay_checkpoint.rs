/* This file is part of sled-overlay
 *
 * Copyright (C) 2023-2026 Dyne.org foundation
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
//! [`sled::Db`] instance, and perform checkpoints and writes to verify
//! overlay's cache checkpoint functionality.

use sled::Config;

use sled_overlay::SledDbOverlay;

const TREE: &[u8] = b"_tree";
const NEW_TREE: &[u8] = b"_new_tree";

#[test]
fn sled_db_overlay_checkpoint() -> Result<(), sled::Error> {
    // Initialize database
    let config = Config::new().temporary(true);
    let db = config.open()?;

    // Initialize overlay
    let mut overlay = SledDbOverlay::new(&db, vec![]);

    // Open tree in the overlay
    overlay.open_tree(TREE, false)?;

    // We keep seperate trees for validation
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

    // Now we create an overlay checkpoint
    overlay.checkpoint();

    // We add some more values to the overlay
    overlay.insert(TREE, b"key_d", b"val_d")?;
    overlay.insert(TREE, b"key_e", b"val_e")?;
    overlay.insert(TREE, b"key_f", b"val_f")?;

    // Verify they are in the overlay
    assert_eq!(overlay.get(TREE, b"key_d")?, Some(b"val_d".into()));
    assert_eq!(overlay.get(TREE, b"key_e")?, Some(b"val_e".into()));
    assert_eq!(overlay.get(TREE, b"key_f")?, Some(b"val_f".into()));

    // Verify they are not in sled
    assert_eq!(tree.get(b"key_d")?, None);
    assert_eq!(tree.get(b"key_e")?, None);
    assert_eq!(tree.get(b"key_f")?, None);

    // We also create a new tree
    overlay.open_tree(NEW_TREE, false)?;

    // We assume something went wrong, so we revert to last checkpoint
    overlay.revert_to_checkpoint();

    // And drop the new tree we created
    db.drop_tree(NEW_TREE)?;

    // Now execute all tree batches in the overlay
    assert_eq!(overlay.apply(), Ok(()));

    // Don't forget to flush
    db.flush()?;

    // Verify sled contains pre-checkpoint keys
    assert_eq!(tree.get(b"key_a")?, Some(b"val_a".into()));
    assert_eq!(tree.get(b"key_b")?, Some(b"val_b".into()));
    assert_eq!(tree.get(b"key_c")?, Some(b"val_c".into()));

    // Verify sled doesn't contains keys after checkpoint
    assert_eq!(tree.get(b"key_d")?, None);
    assert_eq!(tree.get(b"key_e")?, None);
    assert_eq!(tree.get(b"key_f")?, None);

    // Verify sled doesn't contain the new tree we created
    // after checkpoint
    assert!(!db.tree_names().contains(&NEW_TREE.into()));

    Ok(())
}
