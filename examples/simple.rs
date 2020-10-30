use btree::BTree;
use rand::prelude::*;
use chrono::Local;

fn main() {
    let mut btree = BTree::<u32, u32>::new("./testbtree.btree");

    // let mut rng = thread_rng();
    // let mut nums = Vec::<u32>::new();
    // for i in 0..100000 {
    //     nums.push(i);
    // }
    // nums.shuffle(&mut rng);
    // let t0 = Local::now().timestamp_millis();
    // for i in nums {
    //     btree.set(&i, &(i + 1)).unwrap();
    //     // println!("i {}", i);
    // }
    // println!("{}", Local::now().timestamp_millis() - t0);
    for i in 0..100000 {
        println!("{} {}", i, btree.get(&i).unwrap());
    }
}