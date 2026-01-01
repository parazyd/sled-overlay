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

//! sled-overlay is a small crate that serves as tooling to have intermediate
//! writes to some sled database. With it, we're able to write data into an
//! in-memory cache, and only flush to the actual sled trees after we decide
//! that everything in some batch was executed correctly.
//! This gives some minimal infrastructure to be able to transparently have
//! rollback-like functionality.

pub use sled;

pub mod tree;
pub use tree::{
    SledTreeOverlay, SledTreeOverlayIter, SledTreeOverlayState, SledTreeOverlayStateDiff,
};

pub mod database;
pub use database::{SledDbOverlay, SledDbOverlayStateDiff};
