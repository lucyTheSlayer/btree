use anyhow::{anyhow, Result};
use core::mem;

pub trait BinSizer {
    fn bin_size() -> usize;
}

pub trait Encodable {
    fn encode(&self, buf: &mut [u8]) -> Result<usize>;
}

pub trait Decodable where Self: Sized{
    fn decode(buf: &[u8]) -> Result<(Self, usize)>;
}

pub fn check_len(buf: &[u8], size: usize) -> Result<()>{
    if buf.len() < size {
        Err(anyhow!("buf too short {} {}", buf.len(), size))
    } else {
        Ok(())
    }
}

macro_rules! num_impl {
    ($ty: ty, $size: tt) => {
        impl BinSizer for $ty {
            #[inline]
            fn bin_size() -> usize {
                $size
            }
        }
        impl Encodable for $ty {
            fn encode(&self, buf: &mut [u8]) -> Result<usize> {
                check_len(buf, $size)?;
                unsafe { *(&mut buf[0] as *mut _ as *mut _) = self.to_be() };
                Ok($size)
            }
        }
        impl Decodable for $ty {
            fn decode(buf: &[u8]) -> Result<(Self, usize)> {
                check_len(buf, $size)?;
                let val: $ty = unsafe { *(&buf[0] as *const _ as *const _) };
                Ok((val.to_be(), $size))
            }
        }
    }
}

num_impl!(u8, 1);
num_impl!(u16, 2);
num_impl!(u32, 4);
num_impl!(u64, 8);
num_impl!(i8, 1);
num_impl!(i16, 2);
num_impl!(i32, 4);
num_impl!(i64, 8);
num_impl!(usize, (mem::size_of::<usize>()));
num_impl!(isize, (mem::size_of::<isize>()));

macro_rules! float_impl {
    ($ty: ty, $base: ty) => {
        impl BinSizer for $ty {
            #[inline]
            fn bin_size() -> usize {
                mem::size_of::<$base>()
            }
        }
        impl Encodable for $ty {
            fn encode(&self, buf: &mut [u8]) -> Result<usize> {
                check_len(buf, mem::size_of::<$base>())?;
                let val: $base = unsafe { mem::transmute(*self) };
                val.encode(buf)
            }
        }
        impl Decodable for $ty {
            fn decode(buf: &[u8]) -> Result<(Self, usize)> {
                check_len(buf, mem::size_of::<$base>())?;
                let (val, size) = <$base>::decode(buf)?;
                Ok((unsafe {mem::transmute(val)}, size))
            }
        }
    };
}

float_impl!(f32, u32);
float_impl!(f64, u64);

#[macro_export]
macro_rules! define_fixed_len_str {
    ($name: ident, $capacity: expr) => {
        #[derive(Debug, Clone, PartialEq, PartialOrd)]
        pub struct $name(String);

        impl BinSizer for $name {
             #[inline]
            fn bin_size() -> usize {
                $capacity
            }
        }

        impl From<String> for $name {
            fn from(s: String) -> Self {
                assert!(s.len() <= $capacity);
                Self(s)
            }
        }

        impl From<&str> for $name {
            fn from(s: &str) -> Self {
                assert!(s.len() <= $capacity);
                Self(s.to_owned())
            }
        }

        impl Encodable for $name {
            fn encode(&self, buf: &mut [u8]) -> anyhow::Result<usize> {
                check_len(buf, $capacity)?;
                let bytes = self.0.as_bytes();
                unsafe {
                    std::ptr::copy_nonoverlapping(bytes.as_ptr(), &mut buf[0], bytes.len());
                }
                // std::ptr::copy_nonoverlapping(bytes, buf, bytes.len());
                if bytes.len() < $capacity - 1 {
                    buf[bytes.len()] = 0;
                }
                // std::io::Write::write(buf, self.0.as_bytes())?;
                Ok($capacity)
            }
        }
        impl Decodable for $name {
            fn decode(buf: &[u8]) -> anyhow::Result<(Self, usize)> {
                let mut str_end_i = $capacity;
                for i in 0..$capacity {
                    if buf[i] == 0 {
                        str_end_i = i;
                        break;
                    }
                }
                let s = std::str::from_utf8(&buf[..str_end_i])?;
                Ok((Self(s.to_owned()), $capacity))
            }
        }

        impl $name {
            pub fn new(s: &str) -> Self{
                Self(s.to_owned())
            }
        }
    }
}