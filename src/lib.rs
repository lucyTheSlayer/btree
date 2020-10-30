use std::fs::{File, OpenOptions};
use crate::page::{Page, PAGE_SIZE, PageType, Pos, PageError};
pub use crate::byte::*;
use std::marker::PhantomData;
use anyhow::Result;
use std::fmt::Debug;
use std::rc::Rc;
use std::cell::RefCell;

mod page;
mod byte;

pub struct BTree<K, V>
{
    path: &'static str,
    fd: Rc<RefCell<File>>,
    meta_page: Option<Page<K, V>>,
    root_page: Option<Page<K, V>>
}

impl<K, V> BTree<K, V>
    where
        K: Encodable + Decodable + BinSizer + PartialEq + PartialOrd + Debug + Clone,
        V: Encodable + Decodable + BinSizer + Debug + Clone
{
    pub fn new(path: &'static str) -> Self {
        let fd = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path).expect("could not open btree file");
        let mut btree = BTree::<K, V> {
            path,
            fd: Rc::new(RefCell::new(fd)),
            meta_page: None,
            root_page: None,
        };
        let file_len = btree.fd.as_ref().borrow().metadata().unwrap().len();
        if file_len == 0 {
            btree.init_as_empty()
        } else {
            btree.init_load()
        }
        btree
    }

    fn sync(&mut self) -> Result<()>{
        if let Some(p) = self.meta_page.as_mut() {
            p.sync()?;
        }
        if let Some(p) = self.root_page.as_mut() {
            p.sync()?;
        }
        Ok(())
    }

    fn init_as_empty(&mut self) {
        println!("init empty btree");
        let mut meta_page = Page::<K, V>::new(self.fd.clone(), 0, PageType::META).unwrap();
        meta_page.set_total_page(2);
        meta_page.set_root_index(1);
        let mut root_page = Page::<K, V>::new(self.fd.clone(), 1, PageType::LEAF).unwrap();
        root_page.set_item_count(0).unwrap();

        self.meta_page = Some(meta_page);
        self.root_page = Some(root_page);
        self.sync().unwrap();
    }

    fn init_load(&mut self) {
        let meta_page = Page::<K, V>::load(self.fd.clone(), 0).unwrap();
        assert_eq!(meta_page.page_type, PageType::META);

        let root_page = Page::<K, V>::load(self.fd.clone(), meta_page.root_index()).unwrap();
        println!("root page index: {}; total pages:{}; root page keys: {};", meta_page.root_index(), meta_page.total_pages(), root_page.item_count());
        self.meta_page = Some(meta_page);
        self.root_page = Some(root_page);
    }

    pub fn set(&mut self, key: &K, value: &V) -> Result<()> {
        let mut p = self.root_page.as_mut().unwrap();
        let mut pages = Vec::new();
        loop {
            match p.page_type {
                PageType::INTERNAL => {
                    match p.find(key) {
                        Some((i, pos)) => {
                            let ptr_index = match pos {
                                Pos::Left => {
                                    i
                                }
                                _ => {
                                   i + 1
                                }
                            };
                            let child_page_index = p.ptr_at(ptr_index).unwrap();
                            pages.push(Page::<K, V>::load(self.fd.clone(), child_page_index).unwrap());
                            let len = pages.len();
                            p = &mut pages[len - 1];
                        }
                        None => {
                            panic!("impossible for an empty internal page")
                        }
                    }
                }
                PageType::LEAF => {
                    match p.insert(key, value) {
                        Ok(_) => {
                            // inserted, done!
                            return Ok(());
                        },
                        Err(err) => {
                            match err.downcast_ref::<PageError>() {
                                Some(PageError::Full) => {
                                    // eh..., the page is full, we need to split it
                                    break;
                                }
                                _ => {
                                    return Err(err);
                                }
                            }
                        }
                    }
                }
                _ => {
                    panic!("impossible a meta page")
                }
            }
        }
        // page is full, split it!
        // println!("page is full");
        let mut kp = None;
        for p in pages.iter_mut().rev() {
            match p.page_type {
                PageType::LEAF => {
                    // leaf page must be full in this case
                    kp = Some(self.split_leaf_page(p, key, value)?);
                }
                PageType::INTERNAL => {
                    let (k, ptr) = kp.unwrap();
                    if p.is_full() {
                        kp = Some(self.split_internal_page(p, &k, ptr)?);
                    } else {
                        p.insert_ptr(&k, ptr)?;
                        return Ok(());
                    }
                }
                _ => {
                    panic!("impossible a meta page")
                }
            }
        }

        // so root page must be changed
        match kp {
            Some((k, ptr)) => {
                let is_root_full;
                {
                    let mut root_page = self.root_page.as_mut().unwrap();
                    assert_eq!(root_page.page_type, PageType::INTERNAL);
                    is_root_full = root_page.is_full();
                }

                if is_root_full {
                    let mut root_page = self.root_page.take().unwrap();
                    let (k2, ptr2) = self.split_internal_page(&mut root_page, &k, ptr)?;
                    let mut new_root_page = self.new_page(PageType::INTERNAL)?;
                    new_root_page.set_item_count(1)?;
                    new_root_page.set_ptr_at(0, root_page.index)?;
                    new_root_page.set_key_at(0, &k2)?;
                    new_root_page.set_ptr_at(1, ptr2)?;

                    let meta_page = self.meta_page.as_mut().unwrap();
                    meta_page.set_root_index(new_root_page.index);
                    self.root_page = Some(new_root_page);
                } else {
                    let mut root_page = self.root_page.as_mut().unwrap();
                    root_page.insert_ptr(&k, ptr)?;
                }
            }
            None => {
                // root page is full, do split !!!
                let mut root_page = self.root_page.take().unwrap();
                assert!(root_page.is_full() && root_page.page_type == PageType::LEAF);
                let (k, ptr) = self.split_leaf_page(&mut root_page, key, value)?;
                let mut new_root_page = self.new_page(PageType::INTERNAL)?;
                new_root_page.set_item_count(1)?;
                new_root_page.set_ptr_at(0, root_page.index)?;
                new_root_page.set_key_at(0, &k)?;
                new_root_page.set_ptr_at(1, ptr)?;

                let meta_page = self.meta_page.as_mut().unwrap();
                meta_page.set_root_index(new_root_page.index);

                self.root_page = Some(new_root_page);
            }
        }
        self.sync();
        Ok(())
    }

    pub fn get(&mut self, key: &K) -> Option<V> {
        let mut p = self.root_page.as_ref().unwrap();
        let mut pages = Vec::new();
        loop {
            // println!("{:?} {}", p.page_type, p.item_count());
            match p.find(key) {
                Some((i, pos)) => {
                    match p.page_type {
                        PageType::LEAF => {
                            // println!("i: {}, pos: {:?}", i, pos);
                            // println!("{:?}", p);
                            return if pos == Pos::Current {
                                p.value_at(i)
                            } else {
                                None
                            }
                        }
                        PageType::INTERNAL => {
                            match pos {
                                Pos::Left => {
                                    pages.push(Page::<K, V>::load(self.fd.clone(), p.ptr_at(i).unwrap()).unwrap());
                                    p = &pages[pages.len() - 1];
                                }
                                _ => {
                                    pages.push(Page::<K, V>::load(self.fd.clone(), p.ptr_at(i + 1).unwrap()).unwrap());
                                    p = &pages[pages.len() - 1];
                                }
                            }
                        }
                        _ => {
                            // impossible
                            return None
                        }
                    }
                },
                None => {
                    return None;
                }
            }
        }
    }

    fn new_page(&mut self, pt: PageType) -> Result<Page<K, V>> {
        let meta_page = self.meta_page.as_mut().unwrap();
        let max_index = meta_page.total_pages();
        meta_page.set_total_page(max_index + 1);
        Ok(Page::<K, V>::new(self.fd.clone(), max_index, pt)?)
    }

    fn split_leaf_page(&mut self, p: &mut Page<K, V>, key: &K, value: &V) -> Result<(K, u32)> {
        assert_eq!(p.page_type, PageType::LEAF);
        let mut new_page = self.new_page(PageType::LEAF)?;
        let mut keys = Vec::new();
        let mut values = Vec::new();
        let mut inserted = false;
        for i in 0..p.item_count() {
            let k = p.key_at(i).unwrap();
            if !inserted && k > *key {
                keys.push(key.clone());
                values.push(value.clone());
                inserted = true;
            }
            keys.push(k);
            values.push(p.value_at(i).unwrap())
        }
        if !inserted {
            keys.push(key.clone());
            values.push(value.clone());
            inserted = true;
        }
        let cut_i =  (keys.len() + 1) / 2;
        p.set_item_count(cut_i);
        new_page.set_item_count(keys.len() - cut_i);

        for i in 0..cut_i {
            p.set_key_at(i, &keys[i])?;
            p.set_value_at(i, &values[i])?;
        }

        for i in cut_i..keys.len() {
            new_page.set_key_at(i - cut_i, &keys[i])?;
            new_page.set_value_at(i - cut_i, &values[i])?;
        }

        Ok((keys[cut_i].clone(), new_page.index))
    }

    fn split_internal_page(&mut self, p: &mut Page<K, V>, key: &K, ptr: u32) -> Result<(K, u32)> {
        assert_eq!(p.page_type, PageType::INTERNAL);
        let mut new_page = self.new_page(PageType::INTERNAL)?;
        let mut keys = Vec::new();
        let mut ptrs = Vec::new();
        let mut inserted = false;
        ptrs.push(p.ptr_at(0).unwrap());
        for i in 0..p.item_count() {
            let k = p.key_at(i).unwrap();
            if !inserted && k > *key {
                keys.push(key.clone());
                ptrs.push(ptr);
                inserted = true;
            }
            keys.push(k);
            ptrs.push(p.ptr_at(i + 1).unwrap());
        }

        if !inserted {
            keys.push(key.clone());
            ptrs.push(ptr);
            inserted = true;
        }

        let up_i =  (keys.len() - 1) / 2;
        p.set_item_count(up_i);
        new_page.set_item_count(keys.len() - up_i - 1);

        for i in 0..up_i {
            p.set_key_at(i, &keys[i])?;
            p.set_ptr_at(i + 1, ptrs[i + 1])?;
        }

        new_page.set_ptr_at(0, ptrs[up_i + 1])?;
        for i in (up_i + 1)..keys.len() {
            new_page.set_key_at(i - up_i - 1, &keys[i])?;
            new_page.set_ptr_at(i - up_i, ptrs[i + 1])?;
        }
        Ok((keys[up_i].clone(), new_page.index))
    }
}
