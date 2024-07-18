use std::{
    fs::File,
    io::{Seek, SeekFrom, Write},
    os::unix::fs::FileExt,
};

const PageSize: usize = 128;
const Empty: usize = 999999999;
type PageId = usize;

enum MemPageType {
    Inner,
    Leaf,
}

struct MemPage {
    t: MemPageType,
    keys: Vec<usize>,
    pointers: Vec<usize>,
    values: Vec<usize>,
}

impl MemPage {
    pub fn new() -> Self {
        Self {
            t: MemPageType::Inner,
            keys: Vec::new(),
            pointers: Vec::new(),
            values: Vec::new(),
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

        mem_page
    }
}

struct Pager {
    file: File,
    free_list: Vec<PageId>,
}
impl Pager {
    pub fn new() -> Self {
        Self {
            file: File::open("pere.db").unwrap(),
            free_list: Vec::new(),
        }
    }

    pub fn allocate_page(&mut self) -> PageId {
        if !self.free_list.is_empty() {
            self.free_list.pop().unwrap()
        } else {
            self.file.seek(SeekFrom::End(0));
            let page_id = self.file.metadata().unwrap().len() as usize / PageSize;
            self.file.write(&vec![0; PageSize]).unwrap();
            page_id
        }
    }

    pub fn write(&mut self, page_id: PageId, page: &MemPage) {
        self.file
            .write_at(page.as_buf().as_slice(), (page_id * PageSize) as u64)
            .unwrap();
    }
}

struct Btree {
    root: PageId,
    degree: usize,
    pager: Pager,
}

impl Btree {
    pub fn new() -> Self {
        Self {
            root: Empty,
            degree: 3,
            pager: Pager::new(),
        }
    }
    pub fn insert(&mut self, key: usize, data: usize) {
        if self.root == Empty {
            let page_id = self.pager.allocate_page();
            let page_id_child = self.pager.allocate_page();
            let mut page = MemPage::new();
            let mut page_child = MemPage::new();
            page.keys.push(key);
            page.pointers.push(page_id_child);

            page_child.t = MemPageType::Leaf;
            page_child.keys.push(key);
            page_child.values.push(data);
            self.pager.write(page_id, &page);
            self.pager.write(page_id_child, &page_child);
        }
    }

    pub fn get(&self, key: usize) -> usize {
        0
    }

    pub fn delete(&mut self, key: usize) {}

    pub fn list(&self) -> Vec<usize> {
        Vec::new()
    }
}

fn main() {}

#[test]
fn test_mem_page() {
    let mut m = MemPage::new();
    m.keys.push(2);
    m.values.push(3);
    m.values.push(4);
    m.pointers.push(5);
    for (i, v) in m.as_buf().iter().enumerate() {
        if i % 8 == 0 {
            println!();
        }
        print!("{:?} ", v);
    }
}
