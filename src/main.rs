use std::{
    borrow::BorrowMut,
    fs::File,
    io::{Seek, SeekFrom, Write},
    os::unix::fs::FileExt,
    ptr::swap,
};

use rand::Rng;

const PageSize: usize = 128;
const Empty: usize = 999999999;
type PageId = usize;

#[derive(Debug)]
enum MemPageType {
    Inner,
    Leaf,
}

#[derive(Debug)]
struct MemPage {
    t: MemPageType,
    keys: Vec<usize>,
    pointers: Vec<usize>,
    values: Vec<usize>,
    parent: PageId,
}

impl MemPage {
    pub fn new() -> Self {
        Self {
            t: MemPageType::Inner,
            keys: Vec::new(),
            pointers: Vec::new(),
            values: Vec::new(),
            parent: Empty,
        }
    }

    pub fn as_buf(&self) -> Vec<u8> {
        let mut buf = vec![0; PageSize];
        buf[0] = match self.t {
            MemPageType::Inner => 0,
            MemPageType::Leaf => 1,
        };
        buf[1..9].copy_from_slice(&self.keys.len().to_le_bytes());
        buf[10..18].copy_from_slice(&self.pointers.len().to_le_bytes());
        buf[18..26].copy_from_slice(&self.values.len().to_le_bytes());
        let mut start = 26;
        for (i, v) in self.keys.iter().enumerate() {
            buf[start..start + 8].copy_from_slice(&v.to_le_bytes());
            start += 8;
        }
        for (i, v) in self.values.iter().enumerate() {
            buf[start..start + 8].copy_from_slice(&v.to_le_bytes());
            start += 8;
        }
        for (i, v) in self.pointers.iter().enumerate() {
            buf[start..start + 8].copy_from_slice(&v.to_le_bytes());
            start += 8;
        }
        buf[start..start + 8].copy_from_slice(&usize::to_le_bytes(self.parent));

        buf
    }

    pub fn form_buf(buf: &Vec<u8>) -> MemPage {
        let mut mem_page = MemPage::new();
        if buf[0] == 0 {
            mem_page.t = MemPageType::Inner;
        } else {
            mem_page.t = MemPageType::Leaf;
        }
        let nkeys = usize::from_le_bytes(buf[1..9].try_into().unwrap());
        let npointers = usize::from_le_bytes(buf[10..18].try_into().unwrap());
        let nvalues = usize::from_le_bytes(buf[18..26].try_into().unwrap());

        let mut start = 26;
        for i in 0..nkeys {
            mem_page.keys.push(usize::from_le_bytes(
                buf[start..start + 8].try_into().unwrap(),
            ));
            start += 8;
        }

        for i in 0..npointers {
            mem_page.pointers.push(usize::from_le_bytes(
                buf[start..start + 8].try_into().unwrap(),
            ));
            start += 8;
        }

        for i in 0..nvalues {
            mem_page.values.push(usize::from_le_bytes(
                buf[start..start + 8].try_into().unwrap(),
            ));
            start += 8;
        }

        mem_page.parent = usize::from_le_bytes(buf[start..start + 8].try_into().unwrap());

        mem_page
    }

    pub fn is_leaf(&self) -> bool {
        match self.t {
            MemPageType::Leaf => true,
            _ => false,
        }
    }
}

struct Pager {
    file: File,
    free_list: Vec<PageId>,
    file_size: usize,
}

impl Pager {
    pub fn new() -> Self {
        Self {
            file: File::options()
                .create(true)
                .read(true)
                .write(true)
                .truncate(true)
                .open("pere.db")
                .unwrap(),
            free_list: Vec::new(),
            file_size: 0,
        }
    }

    pub fn allocate_page(&mut self) -> PageId {
        if !self.free_list.is_empty() {
            self.free_list.pop().unwrap()
        } else {
            self.file.seek(SeekFrom::End(0));
            let page_id = self.file_size as usize / PageSize;
            self.file.write(&vec![0; PageSize]).unwrap();
            self.file_size += PageSize;
            page_id
        }
    }

    pub fn read(&self, page_id: PageId) -> MemPage {
        let mut buf = vec![0; PageSize];
        self.file
            .read_at(buf.as_mut_slice(), (page_id * PageSize) as u64)
            .unwrap();
        MemPage::form_buf(&buf)
    }

    pub fn write(&mut self, page_id: PageId, page: &MemPage) {
        self.file
            .write_at(page.as_buf().as_slice(), (page_id * PageSize) as u64)
            .unwrap();
        self.file.flush().unwrap();
    }

    pub fn close(&mut self) {
        self.file.flush().unwrap();
        self.file.set_len(0).unwrap();
        self.file.flush().unwrap();
    }
}

struct Btree {
    root: PageId,
    min_degree: usize,
    pager: Pager,
}

impl Btree {
    pub fn new(min_degree: usize) -> Self {
        Self {
            root: Empty,
            min_degree,
            pager: Pager::new(),
        }
    }

    pub fn insert(&mut self, key: usize, data: usize) {
        println!("\ninserting {} {}", key, data);
        if self.root == Empty {
            let page_id = self.pager.allocate_page();
            let mut page = MemPage::new();
            page.t = MemPageType::Leaf;
            page.keys.push(key);
            page.values.push(data);
            self.pager.write(page_id, &page);
            self.root = page_id;
            return;
        }

        let mut cur = self.root;
        let mut page = self.pager.read(cur);
        loop {
            println!("trying {:?}", page);
            if page.is_leaf() {
                break;
            }
            let mut i = 0;
            for page_key in &page.keys {
                if key < *page_key {
                    break;
                }
                i += 1;
            }
            cur = page.pointers[i];
            page = self.pager.read(cur);
        }
        println!("found leaf {:?}", page);

        let mut move_key = key;
        let mut move_value = data;
        let mut overwrite = false;
        for i in 0..page.keys.len() {
            if (page.keys[i] == key) {
                overwrite = true;
                page.values[i] = data;
                break;
            }
        }
        if overwrite {
            self.pager.write(cur, &page);
            return;
        }
        for i in 0..page.keys.len() {
            if move_key < page.keys[i] {
                (page.keys[i], move_key) = (move_key, page.keys[i]);
                (page.values[i], move_value) = (move_value, page.values[i]);
            }
        }
        page.keys.push(move_key);
        page.values.push(move_value);
        if page.keys.len() > self.max_keys() {
            // split
            let mut right_id = self.pager.allocate_page();
            let mut right = MemPage::new();
            right.t = MemPageType::Leaf;
            let m = page.keys.len() / 2;
            let m_key = page.keys[m];
            // push m to end in right
            for i in m..page.keys.len() {
                right.keys.push(page.keys[i]);
                right.values.push(page.values[i]);
            }

            // remove keys from left page
            page.keys.truncate(m);
            page.values.truncate(m);
            if self.root == cur {
                let new_root = self.pager.allocate_page();
                self.root = new_root;
                let mut root_page = MemPage::new();
                root_page.t = MemPageType::Inner;
                right.parent = new_root;
                page.parent = new_root;
                root_page.keys.push(m_key);
                root_page.pointers.push(cur);
                root_page.pointers.push(right_id);
                self.pager.write(cur, &page);
                self.pager.write(right_id, &right);
                self.pager.write(new_root, &root_page);
                println!("------------------------ root {:?}", root_page);
            } else {
                right.parent = page.parent;
                self.pager.write(cur, &page);
                self.pager.write(right_id, &right);
                self.insert_internal(m_key, page.parent, right_id);
            }
            println!("splitting leaf page page {:?}", page);
            println!("                    right {:?}", right);
            println!("                    m_key {:?}", m_key);
        } else {
            self.pager.write(cur, &page);
        }

        println!("done {:?}", page);
    }

    pub fn insert_internal(&mut self, key: usize, page_id: usize, child_id: usize) {
        let mut page = self.pager.read(page_id);
        println!("insert {} internal page {:?}", key, page);
        if page.keys.len() < self.max_keys() {
            let mut i = 0;
            while i < page.keys.len() && key > page.keys[i] {
                i += 1;
            }
            page.keys.insert(i, key);
            // insert and shift to right other pointers
            page.pointers.insert(i + 1, child_id);
            println!("after insert internal {:?}", page);
            self.pager.write(page_id, &page);
        } else {
            let mut i = 0;
            while i < page.keys.len() && key > page.keys[i] {
                i += 1;
            }
            page.keys.insert(i, key);
            // insert and shift to right other pointers
            page.pointers.insert(i + 1, child_id);
            // now propagate
            let m = page.keys.len() / 2;
            let m_key = page.keys[m];
            let right = self.pager.allocate_page();
            let mut right_page = MemPage::new();
            right_page.parent = page.parent;

            // move keys, here m is not included like in leaf
            for i in m + 1..page.keys.len() {
                right_page.keys.push(page.keys[i]);
            }
            // move pointers
            for i in m + 1..page.pointers.len() {
                right_page.pointers.push(page.pointers[i]);
            }
            page.keys.truncate(m);
            page.pointers.truncate(m + 1);

            // update parent of right
            for p in &right_page.pointers {
                let mut child_page = self.pager.read(*p);
                child_page.parent = right;
                self.pager.write(*p, &child_page);
            }
            println!("after insert split internal {:?}", page);
            println!("        right {:?}", right_page);
            println!("        m {:?}", m_key);
            if page_id == self.root {
                // if page was root before splitting, then create a new root
                let new_root = self.pager.allocate_page();
                let mut page_root = MemPage::new();
                page_root.t = MemPageType::Inner;
                page_root.keys.push(m_key);
                page_root.pointers.push(page_id);
                page_root.pointers.push(right);
                self.root = new_root;
                page.parent = new_root;
                right_page.parent = new_root;

                self.pager.write(right, &right_page);
                self.pager.write(page_id, &page);
                self.pager.write(new_root, &page_root);
            } else {
                self.pager.write(right, &right_page);
                self.pager.write(page_id, &page);
                self.insert_internal(m_key, page.parent, right);
            }
        }
    }

    pub fn get(&self, key: usize) -> usize {
        println!("get {}", key);
        let mut cur = self.root;
        let mut cur_page = self.pager.read(cur);
        while !cur_page.is_leaf() {
            println!("cur page {:?}", cur_page);
            let mut i = 0;
            while i < cur_page.keys.len() {
                if key < cur_page.keys[i] {
                    break;
                }
                i += 1;
            }
            cur = cur_page.pointers[i];
            cur_page = self.pager.read(cur);
        }
        println!("cur page {:?}", cur_page);
        let mut i = 0;
        while i < cur_page.keys.len() {
            if key == cur_page.keys[i] {
                break;
            }
            i += 1;
        }
        if i == cur_page.keys.len() {
            println!("not found");
            0
        } else {
            println!("got values {}", cur_page.values[i]);
            cur_page.values[i]
        }
    }

    pub fn delete(&mut self, key: usize) {}

    pub fn list(&self) -> Vec<usize> {
        Vec::new()
    }

    fn min_keys(&self) -> usize {
        self.min_degree - 1
    }

    fn max_keys(&self) -> usize {
        (self.min_degree * 2) - 1
    }
    pub fn close(&mut self) {
        self.pager.close();
    }
}

fn main() {
    let mut b = Btree::new(2);
    b.insert(1, 1);
    b.insert(2, 2);
    b.insert(3, 3);
    b.insert(4, 4);
}

// #[test]
// fn test_mem_page() {
//     let mut m = MemPage::new();
//     m.keys.push(2);
//     m.values.push(3);
//     m.values.push(4);
//     m.pointers.push(5);
//     for (i, v) in m.as_buf().iter().enumerate() {
//         if i % 8 == 0 {
//             println!();
//         }
//         print!("{:?} ", v);
//     }
// }

// #[test]
// fn test_btree() {
//     let mut b = Btree::new(2);
//     b.insert(1, 1);
//     b.insert(2, 2);
//     b.insert(3, 3);
//     b.insert(4, 4);
//     debug_assert!(b.get(4) == 4);
//     debug_assert!(b.get(3) == 3);
//     debug_assert!(b.get(2) == 2);
//     debug_assert!(b.get(1) == 1);
//     b.close();
// }

#[test]
fn test_btree_hard() {
    let mut b = Btree::new(2);
    let l = 50;
    let mut rng = rand::thread_rng();
    let mut values = Vec::new();
    for i in 0..l {
        let v: usize = rng.gen();
        let v = v % 200;
        dbg!(v);
        values.push(v);
        b.insert(v, v);
    }
    for value in values {
        debug_assert!(b.get(value) == value);
    }
    b.close();
}

// #[test]
// fn test_btree_twice() {
//     let mut b = Btree::new(2);
//     b.insert(1, 1);
//     b.insert(1, 2);
//     debug_assert!(b.get(1) == 2);
//     b.close()
// }
