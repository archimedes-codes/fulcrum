use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, VecDeque};
use std::sync::{Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

pub const MAX_ITEMS: usize = 50;
pub const MAX_TOTAL_BYTES: usize = 8 * 1024 * 1024; // 8 MB

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StoredKind {
    Search,
    Fetch,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredItem {
    pub id: String,
    #[serde(rename = "type")]
    pub kind: StoredKind,
    pub content: String,
    pub metadata: Value,
    pub timestamp: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct CacheStats {
    pub count: usize,
    pub total_bytes: usize,
}

struct CacheInner {
    items: HashMap<String, StoredItem>,
    order: VecDeque<String>,
    total_bytes: usize,
}

pub struct ResponseCache {
    inner: Mutex<CacheInner>,
}

impl Default for ResponseCache {
    fn default() -> Self {
        Self::new()
    }
}

impl ResponseCache {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(CacheInner {
                items: HashMap::new(),
                order: VecDeque::new(),
                total_bytes: 0,
            }),
        }
    }

    pub fn store(&self, kind: StoredKind, content: String, metadata: Value) -> String {
        let id = generate_id();
        self.store_with_id(id, kind, content, metadata)
    }

    pub fn store_with_id(
        &self,
        id: String,
        kind: StoredKind,
        content: String,
        metadata: Value,
    ) -> String {
        let mut inner = self.inner.lock().unwrap();

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let meta_bytes = serde_json::to_vec(&metadata).map(|v| v.len()).unwrap_or(0);
        let item_bytes = content.len() + meta_bytes;
        let item = StoredItem {
            id: id.clone(),
            kind,
            content,
            metadata,
            timestamp,
        };

        inner.total_bytes += item_bytes;
        inner.items.insert(id.clone(), item);
        inner.order.push_back(id.clone());

        // Evict LRU items if over count or byte limits
        while inner.order.len() > MAX_ITEMS
            || (inner.total_bytes > MAX_TOTAL_BYTES && !inner.order.is_empty())
        {
            if let Some(oldest_id) = inner.order.pop_front() {
                if let Some(removed) = inner.items.remove(&oldest_id) {
                    let removed_meta_bytes = serde_json::to_vec(&removed.metadata)
                        .map(|v| v.len())
                        .unwrap_or(0);
                    let removed_bytes = removed.content.len() + removed_meta_bytes;
                    inner.total_bytes = inner.total_bytes.saturating_sub(removed_bytes);
                }
            }
        }

        id
    }

    pub fn get(&self, id: &str) -> Option<StoredItem> {
        let mut inner = self.inner.lock().unwrap();
        if let Some(item) = inner.items.get(id).cloned() {
            // Move id to back (most recently used)
            if let Some(pos) = inner.order.iter().position(|x| x == id) {
                inner.order.remove(pos);
                inner.order.push_back(id.to_string());
            }
            Some(item)
        } else {
            None
        }
    }

    pub fn clear(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.items.clear();
        inner.order.clear();
        inner.total_bytes = 0;
    }

    pub fn stats(&self) -> CacheStats {
        let inner = self.inner.lock().unwrap();
        CacheStats {
            count: inner.items.len(),
            total_bytes: inner.total_bytes,
        }
    }
}

pub fn generate_id() -> String {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();

    const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let suffix: String = (0..6)
        .map(|_| {
            let idx = (rand::random::<u8>() as usize) % CHARSET.len();
            CHARSET[idx] as char
        })
        .collect();

    format!("web_{now_ms}_{suffix}")
}

pub fn global_cache() -> &'static ResponseCache {
    static CACHE: OnceLock<ResponseCache> = OnceLock::new();
    CACHE.get_or_init(ResponseCache::new)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_store_and_get() {
        let cache = ResponseCache::new();
        let id = cache.store(
            StoredKind::Search,
            "test content".into(),
            serde_json::json!({"meta": 1}),
        );
        let item = cache.get(&id).expect("item should exist");
        assert_eq!(item.content, "test content");
        assert_eq!(item.kind, StoredKind::Search);
        assert_eq!(item.metadata["meta"], 1);

        assert!(cache.get("nonexistent").is_none());
    }

    #[test]
    fn test_lru_eviction_by_count() {
        let cache = ResponseCache::new();
        let mut ids = Vec::new();
        for i in 0..=MAX_ITEMS {
            let id = cache.store(
                StoredKind::Search,
                format!("item {i}"),
                serde_json::json!({}),
            );
            ids.push(id);
        }

        assert_eq!(cache.stats().count, MAX_ITEMS);
        // First item (ids[0]) should have been evicted
        assert!(cache.get(&ids[0]).is_none());
        // Remaining items should exist
        for id in &ids[1..] {
            assert!(cache.get(id).is_some());
        }
    }

    #[test]
    fn test_lru_access_prevents_eviction() {
        let cache = ResponseCache::new();
        let id0 = cache.store(StoredKind::Search, "item 0".into(), serde_json::json!({}));
        let mut other_ids = Vec::new();
        for i in 1..MAX_ITEMS {
            let id = cache.store(
                StoredKind::Search,
                format!("item {i}"),
                serde_json::json!({}),
            );
            other_ids.push(id);
        }

        // Cache now has MAX_ITEMS (0..49).
        // Access id0 to make it most recently used:
        assert!(cache.get(&id0).is_some());

        // Insert item 50:
        let id_new = cache.store(StoredKind::Search, "item 50".into(), serde_json::json!({}));

        // id0 should still exist, because it was accessed!
        assert!(cache.get(&id0).is_some());
        assert!(cache.get(&id_new).is_some());
        // other_ids[0] (item 1) should have been evicted as the LRU
        assert!(cache.get(&other_ids[0]).is_none());
    }

    #[test]
    fn test_lru_eviction_by_bytes() {
        let cache = ResponseCache::new();
        // Insert a 5 MB item
        let id1 = cache.store(
            StoredKind::Fetch,
            "a".repeat(5 * 1024 * 1024),
            serde_json::json!({}),
        );
        let empty_meta_len = serde_json::to_vec(&serde_json::json!({})).unwrap().len();
        assert_eq!(cache.stats().total_bytes, 5 * 1024 * 1024 + empty_meta_len);

        // Insert another 4 MB item (total 9 MB > 8 MB)
        let id2 = cache.store(
            StoredKind::Fetch,
            "b".repeat(4 * 1024 * 1024),
            serde_json::json!({}),
        );

        // id1 should have been evicted to bring total bytes under 8 MB
        assert!(cache.get(&id1).is_none());
        assert!(cache.get(&id2).is_some());
        assert_eq!(cache.stats().count, 1);
        assert_eq!(cache.stats().total_bytes, 4 * 1024 * 1024 + empty_meta_len);
    }

    #[test]
    fn test_metadata_bytes_accounting() {
        let cache = ResponseCache::new();
        let meta = serde_json::json!({"key": "value"});
        let meta_len = serde_json::to_vec(&meta).unwrap().len();
        let content = "hello";
        let _id = cache.store(StoredKind::Search, content.into(), meta);
        assert_eq!(cache.stats().total_bytes, content.len() + meta_len);
        cache.clear();
        assert_eq!(cache.stats().total_bytes, 0);
    }

    #[test]
    fn test_clear() {
        let cache = ResponseCache::new();
        let id = cache.store(StoredKind::Search, "content".into(), serde_json::json!({}));
        assert_eq!(cache.stats().count, 1);
        cache.clear();
        assert_eq!(cache.stats().count, 0);
        assert_eq!(cache.stats().total_bytes, 0);
        assert!(cache.get(&id).is_none());
    }
}
