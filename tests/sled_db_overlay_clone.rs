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
//! [`sled::Db`] instance, and clone it to verify writes on the cloned
//! overlay's cache do not affect the original one.

use sled::Config;

use sled_overlay::SledDbOverlay;

const TREE: &[u8] = b"_tree";

#[test]
fn sled_db_overlay_clone() -> Result<(), sled::Error> {
    // Initialize database
    let config = Config::new().temporary(true);
    let db = config.open()?;

    // Initialize overlay
    let mut overlay = SledDbOverlay::new(&db, vec![]);

    // Open tree in the overlay
    overlay.open_tree(TREE, false)?;

    // Check overlay tree is empty
    assert!(overlay.is_empty(TREE)?);

    // Check last value is `None`
    assert_eq!(overlay.last(TREE)?, None);

    // We keep seperate tree for validation
    let tree = db.open_tree(TREE)?;

    // Insert some values to the overlay
    overlay.insert(TREE, b"key_a", b"val_a")?;
    overlay.insert(TREE, b"key_b", b"val_b")?;
    overlay.insert(TREE, b"key_c", b"val_c")?;

    // Verify they are in the overlay
    assert_eq!(overlay.get(TREE, b"key_a")?, Some(b"val_a".into()));
    assert_eq!(overlay.get(TREE, b"key_b")?, Some(b"val_b".into()));
    assert_eq!(overlay.get(TREE, b"key_c")?, Some(b"val_c".into()));

    // Check overlay tree is not empty
    assert!(!overlay.is_empty(TREE)?);

    // Check its last value
    assert_eq!(
        overlay.last(TREE)?,
        Some((b"key_c".into(), b"val_c".into()))
    );

    // Verify they are not in sled
    assert_eq!(tree.get(b"key_a")?, None);
    assert_eq!(tree.get(b"key_b")?, None);
    assert_eq!(tree.get(b"key_c")?, None);

    // Clone the overlay
    let mut overlay_clone = overlay.clone();

    // Check cloned overlay tree is not empty
    assert!(!overlay_clone.is_empty(TREE)?);

    // Check its last value
    assert_eq!(
        overlay_clone.last(TREE)?,
        Some((b"key_c".into(), b"val_c".into()))
    );

    // Insert some values to the cloned overlay
    overlay_clone.insert(TREE, b"key_d", b"val_d")?;
    overlay_clone.insert(TREE, b"key_e", b"val_e")?;
    overlay_clone.insert(TREE, b"key_f", b"val_f")?;

    // Verify all records are in the cloned overlay
    assert_eq!(overlay_clone.get(TREE, b"key_a")?, Some(b"val_a".into()));
    assert_eq!(overlay_clone.get(TREE, b"key_b")?, Some(b"val_b".into()));
    assert_eq!(overlay_clone.get(TREE, b"key_c")?, Some(b"val_c".into()));
    assert_eq!(overlay_clone.get(TREE, b"key_d")?, Some(b"val_d".into()));
    assert_eq!(overlay_clone.get(TREE, b"key_e")?, Some(b"val_e".into()));
    assert_eq!(overlay_clone.get(TREE, b"key_f")?, Some(b"val_f".into()));

    // Check its last values
    assert_eq!(
        overlay_clone.last(TREE)?,
        Some((b"key_f".into(), b"val_f".into()))
    );

    // Verify they are not in original overlay or sled
    assert_eq!(tree.get(b"key_d")?, None);
    assert_eq!(tree.get(b"key_e")?, None);
    assert_eq!(tree.get(b"key_f")?, None);
    assert_eq!(overlay.get(TREE, b"key_d")?, None);
    assert_eq!(overlay.get(TREE, b"key_e")?, None);
    assert_eq!(overlay.get(TREE, b"key_f")?, None);

    // We finished processing the cloned overlay, so we can
    // discard it and apply all tree baches of the original
    // overlay.
    assert_eq!(overlay.apply(), Ok(()));

    // Don't forget to flush
    db.flush()?;

    // Verify sled contains keys
    assert_eq!(tree.get(b"key_a")?, Some(b"val_a".into()));
    assert_eq!(tree.get(b"key_b")?, Some(b"val_b".into()));
    assert_eq!(tree.get(b"key_c")?, Some(b"val_c".into()));

    Ok(())
}
