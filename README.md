# btree
a disk-persistence b+ tree implemented in rust

I wrote this to study both rust & btree. So this is not intended for a production usage.

```rust
use btree::BTree;

fn main() {
    let mut btree = BTree::<u64, f64>::new("./testfloat.btree");
    btree.set(&1, &10.23f64).unwrap();
    btree.set(&2, &99.9f64).unwrap();
    
    println!("{}", btree.get(&1).unwrap()); //10.23
}
```
