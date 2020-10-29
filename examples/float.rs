use btree::BTree;

fn main() {
    let mut btree = BTree::<u64, f64>::new("./testfloat.btree");

    // for i in 0..10000 {
    //     btree.set(&i, &(i as f64).sqrt()).unwrap();
    // }

    for i in 0..10000 {
        println!("{} {}", i, btree.get(&i).unwrap());
    }
}