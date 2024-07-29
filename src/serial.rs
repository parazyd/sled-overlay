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

use std::{
    collections::BTreeMap,
    io::{Read, Result, Write},
};

use darkfi_serial::{Decodable, Encodable, VarInt};

use crate::{SledDbOverlayStateDiff, SledTreeOverlayStateDiff};

impl Encodable for SledTreeOverlayStateDiff {
    fn encode<S: Write>(&self, s: &mut S) -> Result<usize> {
        let mut len = 0;

        len += VarInt(self.cache.len() as u64).encode(s)?;
        for (key, (previous, current)) in self.cache.iter() {
            len += key.to_vec().encode(s)?;
            let previous = previous.as_ref().map(|p| p.to_vec());
            len += previous.encode(s)?;
            len += current.to_vec().encode(s)?;
        }

        len += VarInt(self.removed.len() as u64).encode(s)?;
        for (key, value) in self.removed.iter() {
            len += key.to_vec().encode(s)?;
            len += value.to_vec().encode(s)?;
        }

        Ok(len)
    }
}

impl Decodable for SledTreeOverlayStateDiff {
    fn decode<D: Read>(d: &mut D) -> Result<Self> {
        let len = VarInt::decode(d)?.0;
        let mut cache = BTreeMap::new();
        for _ in 0..len {
            let key: Vec<u8> = Decodable::decode(d)?;
            let (previous, current): (Option<Vec<u8>>, Vec<u8>) = Decodable::decode(d)?;
            let previous = previous.as_ref().map(|p| p.clone().into());
            cache.insert(key.into(), (previous, current.into()));
        }

        let len = VarInt::decode(d)?.0;
        let mut removed = BTreeMap::new();
        for _ in 0..len {
            let key: Vec<u8> = Decodable::decode(d)?;
            let entry: Vec<u8> = Decodable::decode(d)?;
            removed.insert(key.into(), entry.into());
        }

        Ok(Self { cache, removed })
    }
}

impl Encodable for SledDbOverlayStateDiff {
    fn encode<S: Write>(&self, s: &mut S) -> Result<usize> {
        let mut len = 0;

        len += VarInt(self.initial_tree_names.len() as u64).encode(s)?;
        for tree_name in &self.initial_tree_names {
            len += tree_name.to_vec().encode(s)?;
        }

        len += VarInt(self.caches.len() as u64).encode(s)?;
        for (key, (cache, drop)) in self.caches.iter() {
            len += key.to_vec().encode(s)?;
            len += cache.encode(s)?;
            len += drop.encode(s)?;
        }

        len += VarInt(self.dropped_trees.len() as u64).encode(s)?;
        for (key, (cache, restore)) in self.dropped_trees.iter() {
            len += key.to_vec().encode(s)?;
            len += cache.encode(s)?;
            len += restore.encode(s)?;
        }

        Ok(len)
    }
}

impl Decodable for SledDbOverlayStateDiff {
    fn decode<D: Read>(d: &mut D) -> Result<Self> {
        let len = VarInt::decode(d)?.0;
        let mut initial_tree_names = vec![];
        for _ in 0..len {
            let initial_tree_name: Vec<u8> = Decodable::decode(d)?;
            initial_tree_names.push(initial_tree_name.into());
        }

        let len = VarInt::decode(d)?.0;
        let mut caches = BTreeMap::new();
        for _ in 0..len {
            let key: Vec<u8> = Decodable::decode(d)?;
            let cache = Decodable::decode(d)?;
            let drop = Decodable::decode(d)?;
            caches.insert(key.into(), (cache, drop));
        }

        let len = VarInt::decode(d)?.0;
        let mut dropped_trees = BTreeMap::new();
        for _ in 0..len {
            let key: Vec<u8> = Decodable::decode(d)?;
            let cache = Decodable::decode(d)?;
            let restore = Decodable::decode(d)?;
            dropped_trees.insert(key.into(), (cache, restore));
        }

        Ok(Self {
            initial_tree_names,
            caches,
            dropped_trees,
        })
    }
}
