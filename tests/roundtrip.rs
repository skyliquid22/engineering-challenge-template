//! Book -> template DTOs -> AlignedSbeWriter -> file -> AlignedSbeReader.
//!
//! Proves that what the collector derives from a book is exactly what a
//! reader of the output file sees, including across a chunk boundary.

use chrono::{DateTime, Utc};
use engineering_challenge::Side;
use engineering_challenge::book::{Book, level_update};
use engineering_challenge::util::dtos::challenge_orderbook::{
    AnyMessage, PriceLevelUpdateDomain, SnapshotDomain,
};
use engineering_challenge::util::{AlignedSbeReader, AlignedSbeWriter, OrderbookSchema};

const CHUNK_SIZE_64_MEBIBYTES: usize = 64 << 20;

/// What the collector would hand the writer: either DTO.
#[derive(Debug, Clone, PartialEq)]
enum Out {
    Snapshot(SnapshotDomain),
    Update(PriceLevelUpdateDomain),
}

fn ts(micros: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_micros(micros).unwrap()
}

/// Drive two books through a snapshot and several deltas (one removing a
/// level) and collect the DTO sequence the engine would emit.
fn fixture_stream() -> Vec<Out> {
    let mut out = Vec::new();
    let base = 1_700_000_000_000_000_i64;

    // Book A: the docs' FED example, yes bids + no asks already converted.
    let mut a = Book::new();
    let sym_a = "FED-23DEC-T3.00";
    a.apply_snapshot(
        &[(80_000, 300_000_000), (220_000, 333_000_000)],
        &[(440_000, 146_000_000), (460_000, 20_000_000)],
        4_182_057,
        Some(ts(base)),
    );
    out.push(Out::Snapshot(a.to_snapshot(sym_a, ts(base + 10))));

    // Book B: a quieter market with a single level each side.
    let mut b = Book::new();
    let sym_b = "HIGHNY-22DEC23-B53.5";
    b.apply_snapshot(
        &[(500_000, 1_000_000)],
        &[(520_000, 2_500_000)],
        77,
        None,
    );
    out.push(Out::Snapshot(b.to_snapshot(sym_b, ts(base + 20))));

    // Deltas on A: reduce, add a new level, remove a level entirely.
    let mut books = [(sym_a, a), (sym_b, b)];
    let steps: [(usize, Side, i64, i64, u64); 5] = [
        (0, Side::Buy, 80_000, -54_000_000, 4_182_061),
        (0, Side::Sell, 500_000, 7_000_000, 4_182_062),
        (0, Side::Sell, 460_000, -20_000_000, 4_182_063),
        (1, Side::Buy, 500_000, 500_000, 78),
        (1, Side::Buy, 510_000, 1_000_000, 79),
    ];
    for (i, (idx, side, price, delta, clock)) in steps.into_iter().enumerate() {
        let received_at = ts(base + 1_000 * (i as i64 + 1));
        let exchange_ts = Some(ts(base + 1_000 * (i as i64 + 1) - 300));
        let (sym, book) = &mut books[idx];
        let qty = book
            .apply_delta(side, price, delta, clock, exchange_ts)
            .expect("fixture deltas are valid");
        out.push(Out::Update(level_update(
            sym,
            side,
            price,
            qty,
            received_at,
            exchange_ts,
        )));
    }

    // Minute snapshot of both books from local state.
    for (sym, book) in &books {
        out.push(Out::Snapshot(book.to_snapshot(sym, ts(base + 60_000_000))));
    }
    out
}

fn write_all(writer: &mut AlignedSbeWriter, msgs: &[Out]) -> anyhow::Result<()> {
    for m in msgs {
        match m {
            Out::Snapshot(s) => writer.write_msg(s)?,
            Out::Update(u) => writer.write_msg(u)?,
        }
    }
    Ok(())
}

fn read_all(path: &std::path::Path) -> anyhow::Result<Vec<Out>> {
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(path)?;
    let mut out = Vec::new();
    while let Some(msg) = reader.read_msg()? {
        match msg {
            AnyMessage::Snapshot(d) => out.push(Out::Snapshot(SnapshotDomain::try_from_decoder(d)?)),
            AnyMessage::PriceLevelUpdate(d) => {
                out.push(Out::Update(PriceLevelUpdateDomain::try_from_decoder(d)?))
            }
            _ => anyhow::bail!("unknown message type in file"),
        }
    }
    Ok(out)
}

#[test]
fn fixture_removal_is_emitted_as_zero_quantity() {
    let stream = fixture_stream();
    let removal = stream.iter().find(|m| {
        matches!(m, Out::Update(u) if u.price_level == 460_000 && u.side == Side::Sell)
    });
    match removal {
        Some(Out::Update(u)) => assert_eq!(u.quantity, 0),
        other => panic!("expected the removal update, got {other:?}"),
    }
    // And the final snapshot of A no longer carries that level.
    let Some(Out::Snapshot(last_a)) = stream
        .iter()
        .rev()
        .find(|m| matches!(m, Out::Snapshot(s) if s.symbol == "FED-23DEC-T3.00"))
    else {
        panic!("missing final snapshot");
    };
    assert!(last_a.asks.iter().all(|l| l.price_level != 460_000));
    assert_eq!(last_a.asks.len(), 2);
}

#[test]
fn write_then_read_back_is_identical() -> anyhow::Result<()> {
    let stream = fixture_stream();
    assert_eq!(stream.len(), 9);

    let file = tempfile::NamedTempFile::new()?;
    let mut writer = AlignedSbeWriter::create(file.path(), CHUNK_SIZE_64_MEBIBYTES)?;
    write_all(&mut writer, &stream)?;
    writer.flush()?;
    drop(writer);

    // One chunk, zero padded to exactly the chunk size.
    assert_eq!(
        std::fs::metadata(file.path())?.len(),
        CHUNK_SIZE_64_MEBIBYTES as u64
    );

    let decoded = read_all(file.path())?;
    assert_eq!(decoded, stream);
    Ok(())
}

#[test]
fn messages_survive_a_chunk_boundary() -> anyhow::Result<()> {
    // Two explicit flushes = two chunks; the reader must skip the padding of
    // the first chunk and continue into the second.
    let stream = fixture_stream();
    let (first, second) = stream.split_at(4);

    let file = tempfile::NamedTempFile::new()?;
    let mut writer = AlignedSbeWriter::create(file.path(), CHUNK_SIZE_64_MEBIBYTES)?;
    write_all(&mut writer, first)?;
    writer.flush()?;
    write_all(&mut writer, second)?;
    writer.flush()?;
    drop(writer);

    assert_eq!(
        std::fs::metadata(file.path())?.len(),
        2 * CHUNK_SIZE_64_MEBIBYTES as u64
    );

    let decoded = read_all(file.path())?;
    assert_eq!(decoded.len(), stream.len());
    assert_eq!(decoded, stream);
    Ok(())
}

#[test]
fn empty_sides_round_trip() -> anyhow::Result<()> {
    // A snapshot of an empty book (no levels either side) must encode and
    // decode cleanly: Kalshi omits a side entirely when it has no offers.
    let snap = Book::new().to_snapshot("EMPTY-MARKET", ts(1_700_000_000_000_000));
    assert!(snap.bids.is_empty() && snap.asks.is_empty());

    let file = tempfile::NamedTempFile::new()?;
    let mut writer = AlignedSbeWriter::create(file.path(), CHUNK_SIZE_64_MEBIBYTES)?;
    writer.write_msg(&snap)?;
    writer.flush()?;
    drop(writer);

    let decoded = read_all(file.path())?;
    assert_eq!(decoded, vec![Out::Snapshot(snap)]);
    Ok(())
}
