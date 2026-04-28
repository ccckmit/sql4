//! 儲存後端抽象
//!
//! `Storage` trait 定義兩個操作：
//!   - `read_node(page_id)`  → Node
//!   - `write_node(page_id, &Node)`
//!   - `alloc_page()`        → 新頁號
//!   - `flush()`             → 強制寫入（磁碟後端才有作用）
//!
//! 目前有兩個實作：
//!   - `MemoryStorage`：全在記憶體，關掉就不見
//!   - `DiskStorage`  ：每個節點存成一個固定大小頁面，寫入 .sql4db 檔

use crate::btree::node::Node;
use super::codec::{decode_node, encode_node, PAGE_SIZE};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

// ------------------------------------------------------------------ //
//  Storage trait                                                       //
// ------------------------------------------------------------------ //

pub trait Storage {
    /// 讀取指定頁號的節點
    fn read_node(&mut self, page_id: usize) -> Node;

    /// 將節點寫入指定頁號
    fn write_node(&mut self, page_id: usize, node: &Node);

    /// 配置一個新的空白頁，回傳頁號
    fn alloc_page(&mut self) -> usize;

    /// 回傳目前最大頁數（頁號從 0 開始）
    fn page_count(&self) -> usize;

    /// 將所有待寫資料強制刷入底層（記憶體後端為 no-op）
    fn flush(&mut self);
}

// ------------------------------------------------------------------ //
//  MemoryStorage                                                       //
// ------------------------------------------------------------------ //

/// 純記憶體後端：以 HashMap 儲存頁面，程式結束後資料消失
pub struct MemoryStorage {
    pages: HashMap<usize, Node>,
    next_page: usize,
}

impl MemoryStorage {
    pub fn new() -> Self {
        MemoryStorage {
            pages: HashMap::new(),
            next_page: 0,
        }
    }
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl Storage for MemoryStorage {
    fn read_node(&mut self, page_id: usize) -> Node {
        self.pages
            .get(&page_id)
            .cloned()
            .expect("MemoryStorage: page not found")
    }

    fn write_node(&mut self, page_id: usize, node: &Node) {
        self.pages.insert(page_id, node.clone());
    }

    fn alloc_page(&mut self) -> usize {
        let id = self.next_page;
        self.next_page += 1;
        id
    }

    fn page_count(&self) -> usize {
        self.next_page
    }

    fn flush(&mut self) {
        // no-op
    }
}

// ------------------------------------------------------------------ //
//  DiskStorage                                                         //
// ------------------------------------------------------------------ //

/// 磁碟後端：資料寫入單一 `.sql4db` 檔案
///
/// 檔案格式：
/// ```text
/// [0..8]   magic:      b"SQL4DB\0\0"
/// [8..12]  version:    u32 = 1
/// [12..16] page_count: u32
/// [16..20] root_page:  u32
/// [20..PAGE_SIZE] 填充至第一頁結束（header 佔一整頁）
/// [PAGE_SIZE..]   資料頁，每頁 PAGE_SIZE bytes
/// ```
pub struct DiskStorage {
    file: File,
    page_count: usize,
}

const MAGIC: &[u8; 8] = b"SQL4DB\0\0";
const VERSION: u32 = 1;
const HEADER_OFFSET: u64 = PAGE_SIZE as u64; // 第 0 頁是 header，資料從第 1 頁起

impl DiskStorage {
    /// 開啟或建立資料庫檔案
    pub fn open<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        let path = path.as_ref();
        let exists = path.exists();

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        let mut storage = DiskStorage { file, page_count: 0 };

        if exists {
            storage.read_header()?;
        } else {
            storage.write_header()?;
        }

        Ok(storage)
    }

    fn write_header(&mut self) -> std::io::Result<()> {
        let mut header = vec![0u8; PAGE_SIZE];
        header[0..8].copy_from_slice(MAGIC);
        header[8..12].copy_from_slice(&VERSION.to_le_bytes());
        header[12..16].copy_from_slice(&(self.page_count as u32).to_le_bytes());
        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(&header)?;
        self.file.flush()
    }

    fn read_header(&mut self) -> std::io::Result<()> {
        let mut header = vec![0u8; PAGE_SIZE];
        self.file.seek(SeekFrom::Start(0))?;
        self.file.read_exact(&mut header)?;

        if &header[0..8] != MAGIC {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid sql4db magic",
            ));
        }
        self.page_count =
            u32::from_le_bytes(header[12..16].try_into().unwrap()) as usize;
        Ok(())
    }

    /// 資料頁的檔案偏移（page_id 從 0 計數，但 0 保留給 header）
    fn page_offset(page_id: usize) -> u64 {
        HEADER_OFFSET + (page_id as u64) * PAGE_SIZE as u64
    }
}

impl Storage for DiskStorage {
    fn read_node(&mut self, page_id: usize) -> Node {
        let offset = Self::page_offset(page_id);
        let mut buf = vec![0u8; PAGE_SIZE];
        self.file.seek(SeekFrom::Start(offset)).unwrap();
        self.file.read_exact(&mut buf).unwrap();
        decode_node(&buf)
    }

    fn write_node(&mut self, page_id: usize, node: &Node) {
        let offset = Self::page_offset(page_id);
        let buf = encode_node(node);
        self.file.seek(SeekFrom::Start(offset)).unwrap();
        self.file.write_all(&buf).unwrap();
    }

    fn alloc_page(&mut self) -> usize {
        let id = self.page_count;
        self.page_count += 1;
        // 先寫一個空白頁佔位
        let blank = vec![0u8; PAGE_SIZE];
        let offset = Self::page_offset(id);
        self.file.seek(SeekFrom::Start(offset)).unwrap();
        self.file.write_all(&blank).unwrap();
        id
    }

    fn page_count(&self) -> usize {
        self.page_count
    }

    fn flush(&mut self) {
        // 更新 header 中的 page_count，然後 fsync
        self.write_header().unwrap();
        self.file.flush().unwrap();
    }
}

// ------------------------------------------------------------------ //
//  測試                                                                //
// ------------------------------------------------------------------ //

#[cfg(test)]
mod tests {
    use super::*;
    use crate::btree::node::{Key, Node, Record};

    fn leaf_with(key: i64, val: &str) -> Node {
        let mut node = Node::new_leaf();
        node.keys.push(Key::Integer(key));
        node.records.push(Record {
            key: Key::Integer(key),
            value: val.as_bytes().to_vec(),
        });
        node
    }

    // --- MemoryStorage ---

    #[test]
    fn memory_alloc_write_read() {
        let mut store = MemoryStorage::new();
        let id = store.alloc_page();
        let node = leaf_with(42, "hello");
        store.write_node(id, &node);
        let back = store.read_node(id);
        assert_eq!(back.keys, node.keys);
        assert_eq!(back.records[0].value, b"hello");
    }

    #[test]
    fn memory_multiple_pages() {
        let mut store = MemoryStorage::new();
        for i in 0..5usize {
            let id = store.alloc_page();
            assert_eq!(id, i);
            store.write_node(id, &leaf_with(i as i64, "v"));
        }
        assert_eq!(store.page_count(), 5);
        let node = store.read_node(3);
        assert_eq!(node.keys[0], Key::Integer(3));
    }

    // --- DiskStorage ---

    #[test]
    fn disk_write_and_read() {
        let path = "/tmp/sql4_test_disk.sql4db";
        let _ = std::fs::remove_file(path);

        {
            let mut store = DiskStorage::open(path).unwrap();
            let id = store.alloc_page();
            store.write_node(id, &leaf_with(99, "world"));
            store.flush();
        }

        // 重新開啟，驗證持久化
        {
            let mut store = DiskStorage::open(path).unwrap();
            assert_eq!(store.page_count(), 1);
            let node = store.read_node(0);
            assert_eq!(node.keys[0], Key::Integer(99));
            assert_eq!(node.records[0].value, b"world");
        }

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn disk_multiple_pages_persist() {
        let path = "/tmp/sql4_test_multi.sql4db";
        let _ = std::fs::remove_file(path);

        {
            let mut store = DiskStorage::open(path).unwrap();
            for i in 0..3usize {
                let id = store.alloc_page();
                store.write_node(id, &leaf_with(i as i64 * 10, "v"));
            }
            store.flush();
        }

        {
            let mut store = DiskStorage::open(path).unwrap();
            assert_eq!(store.page_count(), 3);
            let n = store.read_node(2);
            assert_eq!(n.keys[0], Key::Integer(20));
        }

        let _ = std::fs::remove_file(path);
    }
}
