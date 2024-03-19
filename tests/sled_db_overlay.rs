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
//! [`sled::Db`] instance, and perform writes to verify overlay's cache
//! functionality.

use sled::Config;

use sled_overlay::SledDbOverlay;

const TREE_1: &[u8] = b"_tree1";
const TREE_2: &[u8] = b"_tree2";

#[test]
fn sled_db_overlay() -> Result<(), sled::Error> {
    // Initialize database
    let config = Config::new().temporary(true);
    let db = config.open()?;

    // Initialize overlay
    let mut overlay = SledDbOverlay::new(&db);

    // Open trees in the overlay
    overlay.open_tree(TREE_1)?;
    overlay.open_tree(TREE_2)?;

    // Check overlay trees are empty
    assert!(overlay.is_empty(TREE_1)?);
    assert!(overlay.is_empty(TREE_2)?);

    // Check last value is `None`
    assert_eq!(overlay.last(TREE_1)?, None);
    assert_eq!(overlay.last(TREE_2)?, None);

    // We keep seperate trees for validation
    let tree_1 = db.open_tree(TREE_1)?;
    let tree_2 = db.open_tree(TREE_2)?;

    // Insert some values to the overlay
    overlay.insert(TREE_1, b"key_a", b"val_a")?;
    overlay.insert(TREE_1, b"key_b", b"val_b")?;
    overlay.insert(TREE_1, b"key_c", b"val_c")?;

    overlay.insert(TREE_2, b"key_d", b"val_d")?;
    overlay.insert(TREE_2, b"key_e", b"val_e")?;
    overlay.insert(TREE_2, b"key_f", b"val_f")?;

    // Verify they are in the overlay
    assert_eq!(overlay.get(TREE_1, b"key_a")?, Some(b"val_a".into()));
    assert_eq!(overlay.get(TREE_1, b"key_b")?, Some(b"val_b".into()));
    assert_eq!(overlay.get(TREE_1, b"key_c")?, Some(b"val_c".into()));

    assert_eq!(overlay.get(TREE_2, b"key_d")?, Some(b"val_d".into()));
    assert_eq!(overlay.get(TREE_2, b"key_e")?, Some(b"val_e".into()));
    assert_eq!(overlay.get(TREE_2, b"key_f")?, Some(b"val_f".into()));

    // Check overlay trees are not empty
    assert!(!overlay.is_empty(TREE_1)?);
    assert!(!overlay.is_empty(TREE_2)?);

    // Check their last values
    assert_eq!(
        overlay.last(TREE_1)?,
        Some((b"key_c".into(), b"val_c".into()))
    );
    assert_eq!(
        overlay.last(TREE_2)?,
        Some((b"key_f".into(), b"val_f".into()))
    );

    // Verify they are not in sled
    assert_eq!(tree_1.get(b"key_a")?, None);
    assert_eq!(tree_1.get(b"key_b")?, None);
    assert_eq!(tree_1.get(b"key_c")?, None);

    assert_eq!(tree_2.get(b"key_d")?, None);
    assert_eq!(tree_2.get(b"key_e")?, None);
    assert_eq!(tree_2.get(b"key_f")?, None);

    // Now execute all tree batches in the overlay
    assert_eq!(overlay.apply(), Ok(()));

    // Don't forget to flush
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
