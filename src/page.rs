use std::fs::File;
use anyhow::{Result, anyhow};
use std::io::{Read, Seek, SeekFrom, Write};
use std::borrow::{BorrowMut, Borrow};
use crate::byte::{Encodable, Decodable, BinSizer};
use std::marker::PhantomData;
use thiserror::Error;
use std::fmt::{Display, Debug, Formatter};

pub const PAGE_SIZE: usize = 4096;
pub const MAX_KEY_SIZE: usize = 128;
pub const MAX_VALUE_SIZE: usize = 1024;
const PTR_SIZE: usize = 4;

#[derive(Error, Debug)]
pub enum PageError {
    #[error("page is full, need split")]
    Full
}

pub(crate) struct Page<K, V>
{
    pub index: u32,
    buf: [u8; PAGE_SIZE],
    pub page_type: PageType,
    keys_pos: usize,
    values_pos: usize,
    ptrs_pos: usize,
    max_item_count: usize,
    _k: PhantomData<K>,
    _v: PhantomData<V>,
}

#[derive(Debug, PartialOrd, PartialEq)]
pub(crate) enum PageType {
    META,
    INTERNAL,
    LEAF,
}

#[derive(Debug, PartialOrd, PartialEq)]
pub(crate) enum Pos {
    Current,
    Left,
    Right
}

impl<K, V> Default for Page<K, V> {
    fn default() -> Self {
        Page::<K, V> {
            index: 0,
            buf: [0; PAGE_SIZE],
            page_type: PageType::LEAF,
            keys_pos: 0,
            values_pos: 0,
            ptrs_pos: 0,
            max_item_count: 0,
            _k: PhantomData,
            _v: PhantomData,
        }
    }
}

impl<K, V> Page<K, V> where
    K: Encodable + Decodable + BinSizer + PartialEq + PartialOrd + Debug + Clone,
    V: Encodable + Decodable + BinSizer + Debug + Clone
{
    pub fn new(index: u32, pt: PageType) -> Result<Self> {
        let mut page = Self::default();
        page.page_type = pt;
        page.index = index;
        match page.page_type{
            PageType::META => {
                page.buf[0] = 0x01;
                page.set_root_index(0);
                page.set_total_page(0);
            }
            PageType::INTERNAL => {
                page.buf[0] = 0x02;
                page.set_item_count(0).unwrap();
            }
            PageType::LEAF => {
                page.buf[0] = 0;
                page.set_item_count(0).unwrap();
            }
        }
        page.init_layout();
        Ok(page)
    }

    fn init_layout(&mut self) {
        match self.page_type{
            PageType::META => {
            }
            PageType::INTERNAL => {
                self.max_item_count = (PAGE_SIZE - 8 - PTR_SIZE) / (K::bin_size() + PTR_SIZE);
                self.keys_pos = 8;
                self.ptrs_pos = self.keys_pos + self.max_item_count * K::bin_size()
            }
            PageType::LEAF => {
                self.max_item_count = (PAGE_SIZE - 8) / (K::bin_size() + V::bin_size());
                self.keys_pos = 8;
                self.values_pos = self.keys_pos + self.max_item_count * K::bin_size();
            }
        };
        // at least we should have two items in one page
        assert!(self.page_type == PageType::META || self.max_item_count >= 2)
    }

    pub fn load(fd: &mut File, index: u32) -> Result<Self> {
        let mut page = Self::default();
        page.index = index;
        fd.seek(SeekFrom::Start((index as usize * PAGE_SIZE) as u64))?;
        fd.read_exact(page.buf.borrow_mut())?;
        page.page_type = page.get_page_type();
        page.init_layout();
        Ok(page)
    }

    pub fn sync(&mut self, fd: &mut File) -> Result<()> {
        fd.seek(SeekFrom::Start((self.index as usize * PAGE_SIZE) as u64))?;
        fd.write_all(self.buf.borrow())?;
        Ok(())
    }

    fn get_page_type(&self) -> PageType {
        let u = self.buf[0];
        if u & 0x01 == 1 {
            PageType::META
        } else {
            if u & 0x02 > 0 {
                PageType::INTERNAL
            } else {
                PageType::LEAF
            }
        }
    }

    pub fn root_index(&self) -> u32 {
        match self.page_type {
            PageType::META => u32::decode(&self.buf[4..]).unwrap().0,
            _ => panic!("not a meta page")
        }
    }

    pub fn total_pages(&self) -> u32 {
        match self.page_type {
            PageType::META => u32::decode(&self.buf[8..]).unwrap().0,
            _ => panic!("not a meta page")
        }
    }

    pub fn set_root_index(&mut self, root_index: u32) {
        match self.page_type {
            PageType::META => {
                root_index.encode(&mut self.buf[4..]).unwrap();
            }
            _ => panic!("not a meta page")
        }
    }

    pub fn set_total_page(&mut self, total_page: u32) {
        match self.page_type {
            PageType::META => {
                total_page.encode(&mut self.buf[8..]).unwrap();
            },
            _ => panic!("not a meta page")
        }
    }

    pub fn item_count(&self) -> usize {
        match self.page_type {
            PageType::INTERNAL | PageType::LEAF => u32::decode(&self.buf[4..]).unwrap().0 as usize,
            _ => panic!("not a meta page")
        }
    }

    pub fn is_full(&self) -> bool {
        assert_ne!(self.page_type, PageType::META);
        self.item_count() >= self.max_item_count
    }

    pub fn set_item_count(&mut self, item_count: usize) -> Result<()>{
        match self.page_type {
            PageType::INTERNAL | PageType::LEAF=> {
                if item_count > self.max_item_count {
                    Err(PageError::Full.into())
                } else {
                    (item_count as u32).encode(&mut self.buf[4..]).unwrap();
                    Ok(())
                }
            },
            _ => panic!("not a meta page")
        }
    }

    pub fn key_at(&self, i: usize) -> Option<K> {
        match self.page_type {
            PageType::INTERNAL | PageType::LEAF=> {
                if i >= self.item_count() {
                    None
                } else {
                    K::decode(&self.buf[(self.keys_pos + i * K::bin_size())..]).map(|t| t.0).ok()
                }
            }
            _ => panic!("not a internal / leaf page")
        }
    }

    pub fn value_at(&self, i: usize) -> Option<V> {
        match self.page_type {
            PageType::LEAF => {
                if i >= self.item_count() {
                    None
                } else {
                    V::decode(&self.buf[(self.values_pos + i * V::bin_size())..]).map(|t| t.0).ok()
                }
            }
            _ => panic!("not a leaf page")
        }
    }

    pub fn ptr_at(&self, i: usize) -> Option<u32> {
        match self.page_type {
            PageType::INTERNAL=> {
                if i >= self.item_count() + 1 {
                    None
                } else {
                    u32::decode(&self.buf[(self.ptrs_pos + i * PTR_SIZE)..]).map(|t| t.0).ok()
                }
            }
            _ => panic!("not a internal page")
        }
    }

    pub fn set_key_at(&mut self, i: usize, key: &K) -> Result<()> {
        match self.page_type {
            PageType::INTERNAL | PageType::LEAF => {
                if i >= self.item_count() {
                    return Err(anyhow!("over size"))
                }
                key.encode(&mut self.buf[(self.keys_pos + i * K::bin_size())..])?;
                Ok(())
            }
            _ => panic!("not a internal / leaf page")
        }
    }

    pub fn set_value_at(&mut self, i: usize, value: &V) -> Result<()> {
        match self.page_type {
            PageType::LEAF => {
                if i >= self.item_count() {
                    return Err(anyhow!("over size"))
                }
                value.encode(&mut self.buf[(self.values_pos + i * V::bin_size())..])?;
                Ok(())
            }
            _ => panic!("not a leaf page")
        }
    }

    pub fn set_ptr_at(&mut self, i: usize, ptr: u32) -> Result<()> {
        match self.page_type {
            PageType::INTERNAL => {
                if i >= self.item_count() + 1 {
                    return Err(anyhow!("over size"))
                }
                ptr.encode(&mut self.buf[(self.ptrs_pos + i * PTR_SIZE)..])?;
                Ok(())
            }
            _ => panic!("not a internal page")
        }
    }

    pub fn find(&self, k: &K) -> Option<(usize, Pos)> {
        let item_count = self.item_count();
        if item_count == 0 {
            return None;
        }
        let mut min = 0;
        let mut max = item_count - 1;
        let mut mid;
        while min <= max {
            mid = (min + max) / 2;
            let mid_key = self.key_at(mid).unwrap();
            if mid_key == *k {
                return Some((mid, Pos::Current));
            } else if *k > mid_key {
                if mid == item_count - 1 || self.key_at(mid + 1).unwrap() > *k {
                    return Some((mid, Pos::Right));
                }
                min = mid + 1
            } else if *k < mid_key {
                if mid == 0 {
                    return Some((mid, Pos::Left));
                }
                max = mid - 1
            }
        }

        None
    }

    pub fn insert(&mut self, k: &K, v: &V) -> Result<()> {
        assert_eq!(self.page_type, PageType::LEAF);
        let old_item_count = self.item_count();
        match self.find(k) {
            None => {
                // empty node
                self.set_item_count(1)?;
                self.set_key_at(0, k)?;
                self.set_value_at(0, v)?;
            },
            Some((i, pos)) => {
                match pos {
                    Pos::Current => {
                        self.set_key_at(i, k)?;
                        self.set_value_at(i, v)?;
                    }
                    Pos::Left => {
                        self.set_item_count(old_item_count + 1)?;
                        for j in (i..old_item_count).rev() {
                            self.set_key_at(j + 1, &self.key_at(j).unwrap())?;
                            self.set_value_at(j + 1, &self.value_at(j).unwrap())?;
                        }
                        self.set_key_at(i, k)?;
                        self.set_value_at(i, v)?;
                    }
                    Pos::Right => {
                        self.set_item_count(old_item_count + 1)?;
                        for j in ((i + 1)..old_item_count).rev() {
                            self.set_key_at(j + 1, &self.key_at(j).unwrap())?;
                            self.set_value_at(j + 1, &self.value_at(j).unwrap())?;
                        }
                        self.set_key_at(i + 1, k)?;
                        self.set_value_at(i + 1, v)?;
                    }
                }
            }
        }
        Ok(())
    }

    pub fn insert_ptr(&mut self, k: &K, ptr: u32) -> Result<()> {
        assert_eq!(self.page_type, PageType::INTERNAL);
        let old_item_count = self.item_count();
        match self.find(k) {
            None => {
                // empty node
                // must first set ptrs[0] !!!
                assert!(self.ptr_at(0).unwrap() > 0);
                self.set_item_count(1)?;
                self.set_key_at(0, k)?;
                self.set_ptr_at(1, ptr)?;
            },
            Some((i, pos)) => {
                match pos {
                    Pos::Current => {
                        self.set_key_at(i, k)?;
                        self.set_ptr_at(i + 1, ptr)?;
                    }
                    Pos::Left => {
                        self.set_item_count(old_item_count + 1)?;
                        for j in (i..old_item_count).rev() {
                            self.set_key_at(j + 1, &self.key_at(j).unwrap())?;
                            self.set_ptr_at(j + 2, self.ptr_at(j + 1).unwrap())?;
                        }
                        self.set_key_at(i, k)?;
                        self.set_ptr_at(i + 1, ptr)?;
                    }
                    Pos::Right => {
                        self.set_item_count(old_item_count + 1)?;
                        for j in ((i + 1)..old_item_count).rev() {
                            self.set_key_at(j + 1, &self.key_at(j).unwrap())?;
                            self.set_ptr_at(j + 2, self.ptr_at(j + 1).unwrap())?;
                        }
                        self.set_key_at(i + 1, k)?;
                        self.set_ptr_at(i + 2, ptr)?;
                    }
                }
            }
        }
        Ok(())
    }
}

impl<K,V> Debug for Page<K, V> where
    K: Encodable + Decodable + BinSizer + PartialEq + PartialOrd + Debug + Clone,
    V: Encodable + Decodable + BinSizer + Debug + Clone
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.page_type {
            PageType::META => {
                f.write_fmt(format_args!("{:?}; root index:{}; total pages: {}", self.page_type, self.root_index(), self.total_pages()))?;
            }
            PageType::LEAF => {
                f.write_fmt(format_args!("{:?}; item count:{};\n", self.page_type, self.item_count()))?;
                for i in 0..self.item_count() {
                    f.write_fmt(format_args!("#{} {:?}: {:?}\n", i, self.key_at(i).unwrap(), self.value_at(i).unwrap()))?;
                }
            }
            PageType::INTERNAL => {
                f.write_fmt(format_args!("{:?}; item count:{};\n", self.page_type, self.item_count()))?;
                f.write_fmt(format_args!("#_ _: {}\n", self.ptr_at(0).unwrap()))?;
                for i in 0..self.item_count() {
                    f.write_fmt(format_args!("#{} {:?}: {}\n", i, self.key_at(i).unwrap(), self.ptr_at(i + 1).unwrap()))?;
                }
            }
        }
        Ok(())
    }
}