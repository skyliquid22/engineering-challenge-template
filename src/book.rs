//! A single yes-denominated order book and the SBE DTOs derived from it.
//!
//! Bids are Kalshi `yes` levels as received; asks are Kalshi `no` levels
//! converted to yes prices (`ONE - p_no`) by the decoder before they get
//! here. The book never sees "yes" or "no".

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};

use crate::Side;
use crate::util::dtos::challenge_orderbook::{
    PriceLevelUpdateDomain, SnapshotAsksEntryDomain, SnapshotBidsEntryDomain, SnapshotDomain,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BookState {
    /// Waiting for an exchange snapshot; deltas must be dropped.
    #[default]
    Syncing,
    /// Snapshot applied; deltas are applied and emitted.
    Live,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApplyError {
    /// `old + delta < 0`; the book is left unchanged and must be resynced.
    NegativeQuantity {
        side: Side,
        price: i64,
        old: i64,
        delta: i64,
    },
    /// The book is `Syncing`; the delta was ignored.
    NotLive,
}

impl std::fmt::Display for ApplyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NegativeQuantity { side, price, old, delta } => write!(
                f,
                "delta would drive {side} level {price} negative: {old} + {delta}"
            ),
            Self::NotLive => f.write_str("book is syncing"),
        }
    }
}

impl std::error::Error for ApplyError {}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Book {
    /// price -> absolute quantity, both fixed point. Ascending by price.
    pub bids: BTreeMap<i64, i64>,
    /// price (yes terms) -> absolute quantity. Ascending by price.
    pub asks: BTreeMap<i64, i64>,
    /// Exchange clock of the last change applied (0 if none).
    pub last_clock: u64,
    /// Exchange timestamp of the last change applied, if the exchange gave one.
    pub last_exchange_ts: Option<DateTime<Utc>>,
    pub state: BookState,
}

impl Book {
    /// A new book in the `Syncing` state.
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_live(&self) -> bool {
        self.state == BookState::Live
    }

    /// Forget the current levels' authority: deltas are dropped until the
    /// next `apply_snapshot`. Levels are kept so `to_snapshot` still works.
    pub fn mark_syncing(&mut self) {
        self.state = BookState::Syncing;
    }

    /// Replace both sides entirely and go `Live`. Levels with quantity <= 0
    /// are dropped. Updates `last_clock` and `last_exchange_ts`.
    pub fn apply_snapshot(
        &mut self,
        bids: &[(i64, i64)],
        asks: &[(i64, i64)],
        clock: u64,
        exchange_ts: Option<DateTime<Utc>>,
    ) {
        self.bids = bids
            .iter()
            .filter(|(_, qty)| *qty > 0)
            .map(|&(price, qty)| (price, qty))
            .collect();
        self.asks = asks
            .iter()
            .filter(|(_, qty)| *qty > 0)
            .map(|&(price, qty)| (price, qty))
            .collect();
        self.last_clock = clock;
        if exchange_ts.is_some() {
            self.last_exchange_ts = exchange_ts;
        }
        self.state = BookState::Live;
    }

    /// Apply `delta` to the level at `price` on `side` and return the new
    /// absolute quantity (0 when the level was removed). A positive delta on
    /// a missing level creates it. `Err(NegativeQuantity)` leaves the book
    /// unchanged; `Err(NotLive)` if the book is `Syncing`.
    pub fn apply_delta(
        &mut self,
        side: Side,
        price: i64,
        delta: i64,
        clock: u64,
        exchange_ts: Option<DateTime<Utc>>,
    ) -> Result<i64, ApplyError> {
        if !self.is_live() {
            return Err(ApplyError::NotLive);
        }

        let levels = match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };
        let old = levels.get(&price).copied().unwrap_or(0);
        let new = match old.checked_add(delta) {
            Some(new) if new >= 0 => new,
            _ => {
                return Err(ApplyError::NegativeQuantity {
                    side,
                    price,
                    old,
                    delta,
                });
            }
        };

        if new == 0 {
            levels.remove(&price);
        } else {
            levels.insert(price, new);
        }
        self.last_clock = self.last_clock.max(clock);
        if exchange_ts.is_some() {
            self.last_exchange_ts = exchange_ts;
        }
        Ok(new)
    }

    /// Build the template `Snapshot` DTO. Bids are listed best first
    /// (descending price), asks best first (ascending price).
    /// `exchange_timestamp` is `last_exchange_ts`, falling back to `received_at`.
    pub fn to_snapshot(&self, symbol: &str, received_at: DateTime<Utc>) -> SnapshotDomain {
        SnapshotDomain {
            received_at,
            exchange_timestamp: self.last_exchange_ts.unwrap_or(received_at),
            bids: self
                .bids
                .iter()
                .rev()
                .map(|(&price_level, &quantity)| SnapshotBidsEntryDomain {
                    price_level,
                    quantity,
                })
                .collect(),
            asks: self
                .asks
                .iter()
                .map(|(&price_level, &quantity)| SnapshotAsksEntryDomain {
                    price_level,
                    quantity,
                })
                .collect(),
            symbol: symbol.to_owned(),
        }
    }
}

/// Build the template `PriceLevelUpdate` DTO for an applied delta.
/// `quantity` is the absolute size after the update (0 = level gone).
pub fn level_update(
    symbol: &str,
    side: Side,
    price: i64,
    quantity: i64,
    received_at: DateTime<Utc>,
    exchange_ts: Option<DateTime<Utc>>,
) -> PriceLevelUpdateDomain {
    PriceLevelUpdateDomain {
        backtest_timestamp: received_at,
        exchange_timestamp: exchange_ts.unwrap_or(received_at),
        price_level: price,
        quantity,
        side,
        symbol: symbol.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(micros: i64) -> DateTime<Utc> {
        DateTime::from_timestamp_micros(micros).unwrap()
    }

    fn live_book() -> Book {
        let mut book = Book::new();
        book.apply_snapshot(
            &[(80_000, 300_000_000), (220_000, 333_000_000)],
            &[(440_000, 146_000_000), (460_000, 20_000_000)],
            4_182_057,
            Some(ts(1_700_000_000_000_000)),
        );
        book
    }

    #[test]
    fn new_book_is_syncing_and_empty() {
        let book = Book::new();
        assert_eq!(book.state, BookState::Syncing);
        assert!(!book.is_live());
        assert!(book.bids.is_empty());
        assert!(book.asks.is_empty());
        assert_eq!(book.last_clock, 0);
        assert_eq!(book.last_exchange_ts, None);
    }

    #[test]
    fn snapshot_makes_book_live_with_levels() {
        let book = live_book();
        assert!(book.is_live());
        assert_eq!(book.bids.len(), 2);
        assert_eq!(book.asks.len(), 2);
        assert_eq!(book.bids[&80_000], 300_000_000);
        assert_eq!(book.asks[&460_000], 20_000_000);
        assert_eq!(book.last_clock, 4_182_057);
        assert_eq!(book.last_exchange_ts, Some(ts(1_700_000_000_000_000)));
    }

    #[test]
    fn snapshot_drops_non_positive_levels() {
        let mut book = Book::new();
        book.apply_snapshot(&[(10, 0), (20, -5), (30, 7)], &[(40, 0), (50, 1)], 1, None);
        assert_eq!(book.bids.len(), 1);
        assert_eq!(book.bids[&30], 7);
        assert_eq!(book.asks.len(), 1);
        assert_eq!(book.asks[&50], 1);
    }

    #[test]
    fn snapshot_replaces_everything_including_vanished_levels() {
        let mut book = live_book();
        book.apply_snapshot(&[(90_000, 1_000_000)], &[], 5_000_000, None);
        assert_eq!(book.bids.len(), 1);
        assert!(book.bids.contains_key(&90_000));
        assert!(!book.bids.contains_key(&80_000));
        assert!(book.asks.is_empty());
        assert_eq!(book.last_clock, 5_000_000);
        // None does not clobber the previous exchange timestamp.
        assert_eq!(book.last_exchange_ts, Some(ts(1_700_000_000_000_000)));
    }

    #[test]
    fn snapshot_after_mark_syncing_goes_live_again() {
        let mut book = live_book();
        book.mark_syncing();
        assert!(!book.is_live());
        // Levels retained for to_snapshot while syncing.
        assert_eq!(book.bids.len(), 2);
        book.apply_snapshot(&[(1, 1)], &[(2, 2)], 9, None);
        assert!(book.is_live());
        assert_eq!(book.bids.len(), 1);
    }

    #[test]
    fn delta_on_syncing_book_is_rejected_and_leaves_book_unchanged() {
        let mut book = Book::new();
        let before = book.clone();
        assert_eq!(
            book.apply_delta(Side::Buy, 10, 5, 1, None),
            Err(ApplyError::NotLive)
        );
        assert_eq!(book, before);
    }

    #[test]
    fn positive_delta_updates_existing_level() {
        let mut book = live_book();
        let new = book
            .apply_delta(Side::Buy, 80_000, 1_000_000, 4_182_060, Some(ts(5)))
            .unwrap();
        assert_eq!(new, 301_000_000);
        assert_eq!(book.bids[&80_000], 301_000_000);
        assert_eq!(book.last_clock, 4_182_060);
        assert_eq!(book.last_exchange_ts, Some(ts(5)));
    }

    #[test]
    fn positive_delta_creates_missing_level() {
        let mut book = live_book();
        let new = book
            .apply_delta(Side::Sell, 500_000, 7_000_000, 4_182_061, None)
            .unwrap();
        assert_eq!(new, 7_000_000);
        assert_eq!(book.asks[&500_000], 7_000_000);
        assert_eq!(book.asks.len(), 3);
        // bids untouched
        assert_eq!(book.bids.len(), 2);
    }

    #[test]
    fn negative_delta_reduces_level() {
        let mut book = live_book();
        let new = book
            .apply_delta(Side::Sell, 460_000, -5_000_000, 4_182_062, None)
            .unwrap();
        assert_eq!(new, 15_000_000);
        assert_eq!(book.asks[&460_000], 15_000_000);
    }

    #[test]
    fn delta_to_zero_removes_level_and_returns_zero() {
        let mut book = live_book();
        let new = book
            .apply_delta(Side::Sell, 460_000, -20_000_000, 4_182_063, None)
            .unwrap();
        assert_eq!(new, 0);
        assert!(!book.asks.contains_key(&460_000));
        assert_eq!(book.asks.len(), 1);
    }

    #[test]
    fn delta_below_zero_is_error_and_leaves_book_unchanged() {
        let mut book = live_book();
        let before = book.clone();
        let err = book
            .apply_delta(Side::Sell, 460_000, -20_000_001, 4_182_064, Some(ts(99)))
            .unwrap_err();
        assert_eq!(
            err,
            ApplyError::NegativeQuantity {
                side: Side::Sell,
                price: 460_000,
                old: 20_000_000,
                delta: -20_000_001,
            }
        );
        assert_eq!(book, before, "book must be unchanged, including clock and ts");
    }

    #[test]
    fn negative_delta_on_missing_level_is_error() {
        let mut book = live_book();
        let before = book.clone();
        let err = book
            .apply_delta(Side::Buy, 123_456, -1, 4_182_065, None)
            .unwrap_err();
        assert_eq!(
            err,
            ApplyError::NegativeQuantity {
                side: Side::Buy,
                price: 123_456,
                old: 0,
                delta: -1,
            }
        );
        assert_eq!(book, before);
    }

    #[test]
    fn overflowing_delta_is_error_not_panic() {
        let mut book = live_book();
        let before = book.clone();
        assert!(matches!(
            book.apply_delta(Side::Buy, 80_000, i64::MAX, 1, None),
            Err(ApplyError::NegativeQuantity { .. })
        ));
        assert_eq!(book, before);
    }

    #[test]
    fn zero_delta_on_missing_level_is_noop_returning_zero() {
        let mut book = live_book();
        let new = book.apply_delta(Side::Buy, 1, 0, 4_182_066, None).unwrap();
        assert_eq!(new, 0);
        assert!(!book.bids.contains_key(&1));
        assert_eq!(book.last_clock, 4_182_066);
    }

    #[test]
    fn clock_never_goes_backwards_on_delta() {
        let mut book = live_book();
        book.apply_delta(Side::Buy, 80_000, 1, 10, None).unwrap();
        assert_eq!(book.last_clock, 4_182_057);
        book.apply_delta(Side::Buy, 80_000, 1, 4_182_070, None).unwrap();
        assert_eq!(book.last_clock, 4_182_070);
    }

    #[test]
    fn delta_without_exchange_ts_keeps_previous_ts() {
        let mut book = live_book();
        book.apply_delta(Side::Buy, 80_000, 1, 4_182_070, None).unwrap();
        assert_eq!(book.last_exchange_ts, Some(ts(1_700_000_000_000_000)));
    }

    #[test]
    fn to_snapshot_orders_best_first_and_uses_exchange_ts() {
        let mut book = live_book();
        book.apply_delta(Side::Buy, 150_000, 1_000_000, 4_182_080, None)
            .unwrap();
        let snap = book.to_snapshot("FED-23DEC-T3.00", ts(42));
        assert_eq!(snap.symbol, "FED-23DEC-T3.00");
        assert_eq!(snap.received_at, ts(42));
        assert_eq!(snap.exchange_timestamp, ts(1_700_000_000_000_000));
        let bid_prices: Vec<i64> = snap.bids.iter().map(|b| b.price_level).collect();
        assert_eq!(bid_prices, vec![220_000, 150_000, 80_000]);
        let bid_qtys: Vec<i64> = snap.bids.iter().map(|b| b.quantity).collect();
        assert_eq!(bid_qtys, vec![333_000_000, 1_000_000, 300_000_000]);
        let ask_prices: Vec<i64> = snap.asks.iter().map(|a| a.price_level).collect();
        assert_eq!(ask_prices, vec![440_000, 460_000]);
        let ask_qtys: Vec<i64> = snap.asks.iter().map(|a| a.quantity).collect();
        assert_eq!(ask_qtys, vec![146_000_000, 20_000_000]);
    }

    #[test]
    fn to_snapshot_falls_back_to_received_at_without_exchange_ts() {
        let mut book = Book::new();
        book.apply_snapshot(&[(1, 1)], &[(2, 2)], 1, None);
        let snap = book.to_snapshot("X", ts(7));
        assert_eq!(snap.exchange_timestamp, ts(7));
        assert_eq!(snap.received_at, ts(7));
    }

    #[test]
    fn to_snapshot_of_empty_book_has_empty_sides() {
        let snap = Book::new().to_snapshot("EMPTY", ts(1));
        assert!(snap.bids.is_empty());
        assert!(snap.asks.is_empty());
    }

    #[test]
    fn level_update_dto() {
        let upd = level_update("SYM", Side::Sell, 460_000, 0, ts(10), Some(ts(9)));
        assert_eq!(upd.symbol, "SYM");
        assert_eq!(upd.side, Side::Sell);
        assert_eq!(upd.price_level, 460_000);
        assert_eq!(upd.quantity, 0);
        assert_eq!(upd.backtest_timestamp, ts(10));
        assert_eq!(upd.exchange_timestamp, ts(9));

        let upd = level_update("SYM", Side::Buy, 1, 2, ts(10), None);
        assert_eq!(upd.exchange_timestamp, ts(10));
    }

    #[test]
    fn apply_error_display() {
        let err = ApplyError::NegativeQuantity {
            side: Side::Buy,
            price: 5,
            old: 1,
            delta: -2,
        };
        assert_eq!(
            err.to_string(),
            "delta would drive Buy level 5 negative: 1 + -2"
        );
        assert_eq!(ApplyError::NotLive.to_string(), "book is syncing");
    }
}
