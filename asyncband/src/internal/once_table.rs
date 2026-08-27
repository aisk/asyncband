// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::borrow::Borrow;
use std::fmt;
use std::hash::BuildHasher;
use std::hash::Hash;
use std::sync::Arc;
use std::sync::RwLockReadGuard;
use std::sync::RwLockWriteGuard;

use hashbrown::HashTable;

use crate::internal::rwlock::RwLock;
use crate::once::OnceCell;

const SHARD_COUNT: usize = 64;

type Entries<K, V> = HashTable<Arc<OnceTableEntry<K, V>>>;

pub struct OnceTableEntry<K, V> {
    hash: u64,
    key: K,
    cell: OnceCell<V>,
}

impl<K, V> OnceTableEntry<K, V> {
    pub fn initialized(&self) -> bool {
        self.cell.initialized()
    }

    pub fn get(&self) -> Option<&V> {
        self.cell.get()
    }

    pub async fn get_or_init<F>(&self, init: F) -> &V
    where
        F: AsyncFnOnce() -> V,
    {
        self.cell.get_or_init(init).await
    }

    pub async fn get_or_try_init<E, F>(&self, init: F) -> Result<&V, E>
    where
        F: AsyncFnOnce() -> Result<V, E>,
    {
        self.cell.get_or_try_init(init).await
    }
}

/// Outcome of looking a key up for compute: an initialized entry resolves to its value while the
/// shard lock is still held, so contended hits never touch the entry's shared reference count.
pub enum OnceTableLookup<K, V> {
    Hit(V),
    Pending(Arc<OnceTableEntry<K, V>>),
}

/// Shared keyed storage that lets once primitives clean up an exact entry without cloning its key.
pub struct OnceTable<K, V, S> {
    shards: Box<[RwLock<Entries<K, V>>]>,
    hasher: S,
}

impl<K, V, S> fmt::Debug for OnceTable<K, V, S>
where
    K: fmt::Debug,
    V: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug_map = f.debug_map();
        for shard in &self.shards {
            let entries = shard.read();
            debug_map.entries(entries.iter().map(|entry| (&entry.key, &entry.cell)));
        }
        debug_map.finish()
    }
}

impl<K, V, S> OnceTable<K, V, S> {
    pub fn with_hasher(hasher: S) -> Self {
        Self::with_capacity_and_hasher(0, hasher)
    }

    pub fn with_capacity_and_hasher(capacity: usize, hasher: S) -> Self {
        let shard_capacity = capacity.div_ceil(SHARD_COUNT);
        let shards = (0..SHARD_COUNT)
            .map(|_| RwLock::new(HashTable::with_capacity(shard_capacity)))
            .collect();
        Self { shards, hasher }
    }

    fn shard_read(&self, hash: u64) -> RwLockReadGuard<'_, Entries<K, V>> {
        self.shards[hash as usize & (SHARD_COUNT - 1)].read()
    }

    fn shard_write(&self, hash: u64) -> RwLockWriteGuard<'_, Entries<K, V>> {
        self.shards[hash as usize & (SHARD_COUNT - 1)].write()
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.shards.iter().map(|shard| shard.read().len()).sum()
    }

    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.shards.iter().all(|shard| shard.read().is_empty())
    }
}

impl<K, V, S> OnceTable<K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    pub fn get_or_insert(&self, key: K) -> Arc<OnceTableEntry<K, V>> {
        let hash = self.hasher.hash_one(&key);
        {
            let shard = self.shard_read(hash);
            if let Some(entry) = shard.find(hash, |entry| entry.key.eq(&key)) {
                return Arc::clone(entry);
            }
        }

        let mut shard = self.shard_write(hash);
        Arc::clone(
            shard
                .entry(hash, |entry| entry.key.eq(&key), |entry| entry.hash)
                .or_insert_with(|| {
                    Arc::new(OnceTableEntry {
                        hash,
                        key,
                        cell: OnceCell::new(),
                    })
                })
                .into_mut(),
        )
    }

    pub fn get<Q>(&self, key: &Q) -> Option<Arc<OnceTableEntry<K, V>>>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let hash = self.hasher.hash_one(key);
        self.shard_read(hash)
            .find(hash, |entry| entry.key.borrow() == key)
            .map(Arc::clone)
    }

    /// Clones the value of an initialized entry under the shard read lock, so hits never touch
    /// the entry's shared reference count.
    pub fn get_value<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
        V: Clone,
    {
        let hash = self.hasher.hash_one(key);
        self.shard_read(hash)
            .find(hash, |entry| entry.key.borrow() == key)?
            .get()
            .cloned()
    }

    /// Looks the key up under the shard read lock: an initialized hit resolves to its value
    /// without cloning the entry, a pending entry is returned to wait on, and only an absent key
    /// takes the shard write lock to insert.
    pub fn lookup_or_insert(&self, key: K) -> OnceTableLookup<K, V>
    where
        V: Clone,
    {
        let hash = self.hasher.hash_one(&key);
        {
            let shard = self.shard_read(hash);
            if let Some(entry) = shard.find(hash, |entry| entry.key.eq(&key)) {
                if let Some(value) = entry.get() {
                    return OnceTableLookup::Hit(value.clone());
                }
                return OnceTableLookup::Pending(Arc::clone(entry));
            }
        }

        let mut shard = self.shard_write(hash);
        let entry = shard
            .entry(hash, |entry| entry.key.eq(&key), |entry| entry.hash)
            .or_insert_with(|| {
                Arc::new(OnceTableEntry {
                    hash,
                    key,
                    cell: OnceCell::new(),
                })
            })
            .into_mut();
        if let Some(value) = entry.get() {
            return OnceTableLookup::Hit(value.clone());
        }
        OnceTableLookup::Pending(Arc::clone(entry))
    }

    pub fn remove<Q>(&self, key: &Q) -> Option<Arc<OnceTableEntry<K, V>>>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let hash = self.hasher.hash_one(key);
        let mut shard = self.shard_write(hash);
        let entry = shard
            .find_entry(hash, |entry| entry.key.borrow() == key)
            .ok()?;
        let (entry, _) = entry.remove();
        Some(entry)
    }

    /// Removes the entry if the table still contains the same allocation.
    pub fn remove_entry(&self, entry: &Arc<OnceTableEntry<K, V>>) {
        let mut shard = self.shard_write(entry.hash);
        let Ok(occupied) = shard.find_entry(entry.hash, |stored| Arc::ptr_eq(stored, entry)) else {
            return;
        };

        drop(occupied.remove());
    }

    pub fn cleanup_abandoned_entry(&self, entry: Arc<OnceTableEntry<K, V>>) {
        let mut shard = self.shard_write(entry.hash);
        // If the table still owns this entry, a count of two means the current call is its only
        // owner outside the table: entries are only cloned out of their shard while holding the
        // shard lock, so the write lock excludes new owners while the count is checked, and
        // owners that release outside the lock do so only after the cell is initialized or the
        // entry was detached. The ptr_eq probe rejects an entry that was detached or replaced.
        if Arc::strong_count(&entry) == 2 && !entry.initialized() {
            if let Ok(occupied) = shard.find_entry(entry.hash, |stored| Arc::ptr_eq(stored, &entry))
            {
                drop(occupied.remove());
            }
        }

        // Drop this call's reference before unlocking so a waiting cleanup observes the updated
        // reference count.
        drop(entry);
    }

    pub fn insert(&self, key: K, value: V) {
        self.remove(&key);

        let hash = self.hasher.hash_one(&key);
        let entry = Arc::new(OnceTableEntry {
            hash,
            key,
            cell: OnceCell::from_value(value),
        });
        self.shard_write(hash)
            .insert_unique(hash, entry, |entry| entry.hash);
    }
}
