//! Decimal string <-> i64 fixed point (1_000_000 = 1 USDC = 1 contract).
//! No floats anywhere: Kalshi sends prices as 4-decimal strings and sizes as
//! 2-decimal strings, and the template wants exact integers.

/// One unit (1 USDC or 1 contract) in fixed point.
pub const ONE: i64 = 1_000_000;

/// Number of fractional digits carried by the fixed-point representation.
pub const DECIMALS: u32 = 6;

/// Parse a decimal string such as `"0.0800"`, `"300.00"`, `"-54.00"`, `"1"`
/// or `".5"` into fixed point. Rejects more than 6 fractional digits, any
/// non-digit character, empty input and overflow.
pub fn parse_fixed(s: &str) -> anyhow::Result<i64> {
    let (negative, body) = match s.as_bytes().first() {
        None => anyhow::bail!("empty decimal string"),
        Some(b'-') => (true, &s[1..]),
        Some(b'+') => (false, &s[1..]),
        _ => (false, s),
    };

    let (int_part, frac_part) = match body.split_once('.') {
        Some((i, f)) => (i, f),
        None => (body, ""),
    };

    if int_part.is_empty() && frac_part.is_empty() {
        anyhow::bail!("decimal string {s:?} has no digits");
    }
    if frac_part.len() > DECIMALS as usize {
        anyhow::bail!(
            "decimal string {s:?} has more than {DECIMALS} fractional digits"
        );
    }
    if !int_part.bytes().all(|b| b.is_ascii_digit()) {
        anyhow::bail!("decimal string {s:?} has a non-digit in its integer part");
    }
    if !frac_part.bytes().all(|b| b.is_ascii_digit()) {
        anyhow::bail!("decimal string {s:?} has a non-digit in its fractional part");
    }

    let mut value: i64 = 0;
    for b in int_part.bytes() {
        value = value
            .checked_mul(10)
            .and_then(|v| v.checked_add(i64::from(b - b'0')))
            .ok_or_else(|| anyhow::anyhow!("decimal string {s:?} overflows i64"))?;
    }
    value = value
        .checked_mul(ONE)
        .ok_or_else(|| anyhow::anyhow!("decimal string {s:?} overflows i64"))?;

    let mut frac: i64 = 0;
    for b in frac_part.bytes() {
        frac = frac * 10 + i64::from(b - b'0');
    }
    for _ in frac_part.len()..DECIMALS as usize {
        frac *= 10;
    }
    value = value
        .checked_add(frac)
        .ok_or_else(|| anyhow::anyhow!("decimal string {s:?} overflows i64"))?;

    Ok(if negative { -value } else { value })
}

/// Convert a Kalshi `no` price into the equivalent `yes` ask price:
/// `ONE - p_no`. Errors if `p_no` is outside `[0, ONE]`.
pub fn no_to_yes_price(p_no: i64) -> anyhow::Result<i64> {
    anyhow::ensure!(
        (0..=ONE).contains(&p_no),
        "no price {p_no} is outside [0, {ONE}]"
    );
    Ok(ONE - p_no)
}

/// Human-readable rendering with 6 decimals, e.g. `80_000 -> "0.080000"`.
pub fn format_fixed(v: i64) -> String {
    let sign = if v < 0 { "-" } else { "" };
    let magnitude = v.unsigned_abs();
    let int_part = magnitude / ONE as u64;
    let frac_part = magnitude % ONE as u64;
    format!("{sign}{int_part}.{frac_part:0>6}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_architecture_doc_cases() {
        assert_eq!(parse_fixed("0.0800").unwrap(), 80_000);
        assert_eq!(parse_fixed("300.00").unwrap(), 300_000_000);
        assert_eq!(parse_fixed("-54.00").unwrap(), -54_000_000);
    }

    #[test]
    fn parses_short_forms() {
        assert_eq!(parse_fixed("1").unwrap(), ONE);
        assert_eq!(parse_fixed(".5").unwrap(), 500_000);
        assert_eq!(parse_fixed("0.123456").unwrap(), 123_456);
        assert_eq!(parse_fixed("+1.5").unwrap(), 1_500_000);
        assert_eq!(parse_fixed("0").unwrap(), 0);
        assert_eq!(parse_fixed("0.0000").unwrap(), 0);
        assert_eq!(parse_fixed("5.").unwrap(), 5 * ONE);
        assert_eq!(parse_fixed("-0.0001").unwrap(), -100);
        assert_eq!(parse_fixed("0.9999").unwrap(), 999_900);
    }

    #[test]
    fn rejects_bad_input() {
        for bad in ["", "-", "+", ".", "abc", "1.2.3", "1,5", " 1", "1 ", "0.1234567", "1e3", "--1"] {
            assert!(parse_fixed(bad).is_err(), "{bad:?} should be rejected");
        }
    }

    #[test]
    fn rejects_overflow() {
        assert!(parse_fixed("9223372036854.775808").is_err());
        assert!(parse_fixed("99999999999999999999").is_err());
        // Largest representable: i64::MAX / ONE = 9_223_372_036_854.
        assert_eq!(
            parse_fixed("9223372036854.775807").unwrap(),
            i64::MAX
        );
    }

    #[test]
    fn no_to_yes() {
        assert_eq!(no_to_yes_price(540_000).unwrap(), 460_000);
        assert_eq!(no_to_yes_price(0).unwrap(), ONE);
        assert_eq!(no_to_yes_price(ONE).unwrap(), 0);
        assert!(no_to_yes_price(-1).is_err());
        assert!(no_to_yes_price(ONE + 1).is_err());
    }

    #[test]
    fn formats() {
        assert_eq!(format_fixed(80_000), "0.080000");
        assert_eq!(format_fixed(-54_000_000), "-54.000000");
        assert_eq!(format_fixed(0), "0.000000");
        assert_eq!(format_fixed(ONE), "1.000000");
        assert_eq!(format_fixed(-1), "-0.000001");
        assert_eq!(format_fixed(i64::MAX), "9223372036854.775807");
    }

    #[test]
    fn parse_format_round_trip() {
        for v in [0, 1, -1, 80_000, 300_000_000, -54_000_000, ONE, i64::MAX, i64::MIN + 1] {
            assert_eq!(parse_fixed(&format_fixed(v)).unwrap(), v, "{v}");
        }
    }
}
