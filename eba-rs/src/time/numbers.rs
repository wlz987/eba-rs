pub fn positive_int(value: usize, name: &str) -> usize {
    if value < 1 {
        panic!("{} must be a positive integer", name);
    }
    value
}

pub fn nonnegative_int(value: u64, name: &str) -> u64 {
    let _ = name;
    value
}
