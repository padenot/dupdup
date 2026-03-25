pub(crate) fn format_bytes_binary(n: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut val = n as f64;
    let mut idx = 0usize;
    while val >= 1024.0 && idx + 1 < UNITS.len() {
        val /= 1024.0;
        idx += 1;
    }
    if idx == 0 {
        format!("{} {}", n, UNITS[idx])
    } else {
        format!("{:.0} {}", val, UNITS[idx])
    }
}

pub(crate) fn format_bytes_binary_u128(n: u128) -> String {
    format_bytes_binary((n.min(u64::MAX as u128)) as u64)
}
