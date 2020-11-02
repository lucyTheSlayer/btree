use btree::*;

define_fixed_len_str!(FixedLenStrKey, 50);
define_fixed_len_str!(FixedLenStrValue, 50);


fn main() {
    let mut btree = BTree::<FixedLenStrKey, FixedLenStrValue>::new("./teststr.btree");
    // btree.set(&("haha".into()), &("hehe".into())).unwrap();
    // btree.set(&FixedLenStrKey::new("haha2"), &FixedLenStrValue::new("hehe2")).unwrap();
    btree.set(&("金庸".into()), &("飞雪连天射白鹿，笑书神侠倚碧鸳".into())).unwrap();
    btree.set(&("古龙".into()), &("小李飞刀，多情剑客无情剑".into())).unwrap();
    println!("{}", btree.get(&FixedLenStrKey::new("金庸")).unwrap().0);
}