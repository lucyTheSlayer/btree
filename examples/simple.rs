use btree::BTree;
use rand::prelude::*;

fn main() {
    let mut btree = BTree::<u32, u32>::new("./testbtree.btree");

    let mut rng = thread_rng();
    let mut nums = Vec::<u32>::new();
    for i in 0..1000000 {
        nums.push(i);
    }
    nums.shuffle(&mut rng);
    for i in nums {
        btree.set(&i, &(i + 1)).unwrap();
        println!("i {}", i);
    }

    // for i in 0..1000000 {
    //     println!("{} {}", i, btree.get(&i).unwrap());
    // }
}