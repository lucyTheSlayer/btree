use btree::BTree;

fn main() {
    let mut btree = BTree::<u64, f64>::new("./testfloat.btree");
    // btree.set(&1, &10.23f64).unwrap();
    // btree.set(&2, &99.9f64).unwrap();
    //
    // println!("{}", btree.get(&1).unwrap());
    //
    // for i in 0..10000 {
    //     btree.set(&i, &(i as f64).sqrt()).unwrap();
    // }

    for i in 0..10000 {
        println!("{} {}", i, btree.get(&i).unwrap());
    }
}