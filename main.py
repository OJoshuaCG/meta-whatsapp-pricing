"""
WhatsApp Business Platform Pricing – CSV Ingestion Tool

Usage examples
--------------
Initialise the database schema (first run only):
    python main.py --init-db

Load a base-rate file:
    python main.py --file "Pricing.csv" --valid-from 2026-01-01

Load a tier-rate file, closing the previous period explicitly:
    python main.py --file "Tier Pricing.csv" --valid-from 2026-04-01

Load with optional metadata:
    python main.py --file "Pricing.csv" --valid-from 2026-04-01 \\
        --uploaded-by "john.doe" --notes "Q2 2026 update"

Dry-run (parse only, no DB writes):
    python main.py --file "Pricing.csv" --valid-from 2026-01-01 --dry-run

Bulk load all CSVs in a directory:
    python main.py --dir "./csv/" --valid-from 2026-01-01
    python main.py --dir "./csv/" --valid-from 2026-01-01 --dry-run
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from config import DB_CONFIG
from db.connection import get_connection
from db.initializer import init_schema
from loaders import load_base_rates, load_tier_rates
from models import BaseRateRecord, FileType, TierRateRecord
from utils.date_utils import day_before, parse_date

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s – %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# File-type detection
# ---------------------------------------------------------------------------

# Meta includes "volume tiers" in the first line of the Tier Pricing CSV.
_TIER_MARKER = "volume tier"

# Currency token that Meta uses ("$US") for all USD-denominated files.
# Extend this dict whenever Meta introduces a new non-ISO token or symbol.
_CURRENCY_ALIASES: dict[str, str] = {
    "$US": "USD",   # Meta's non-standard token for US Dollar
    "A$":  "AUD",   # Australian Dollar symbol
    "GBD": "GBP",   # Typo found in Meta CSV (should be GBP)
    "£":   "GBP",   # British Pound Sterling symbol
    "₹":   "INR",   # Indian Rupee symbol
    "RP":  "IDR",   # Indonesian Rupiah abbreviation
}


def _detect_file_type(filepath: Path) -> FileType:
    """Infer whether *filepath* is a BASE or TIER pricing file."""
    with filepath.open(encoding="utf-8") as fh:
        first_line = fh.readline().lower()
    return FileType.TIER if _TIER_MARKER in first_line else FileType.BASE


def _detect_currency(filepath: Path, file_type: FileType) -> str:
    """Read the currency from the first data row of the CSV.

    Both file formats have the currency in column index 1 of the first
    non-metadata row.

    BASE files: 5 metadata rows + 1 header row → data starts at logical row 6.
    TIER files: 5 combined metadata/header rows → data starts at logical row 5.
    """
    import pandas as pd  # local import to keep top-level imports light

    skip = 6 if file_type is FileType.BASE else 5

    df = pd.read_csv(
        filepath,
        skiprows=skip,
        header=None,
        nrows=1,
        dtype=str,
        keep_default_na=False,
    )
    raw = str(df.iloc[0, 1]).strip()
    return _CURRENCY_ALIASES.get(raw, raw.upper())


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def _get_or_create_market(cursor, name: str) -> int:
    """Return the id of *name* in waba_market, inserting it if absent."""
    cursor.execute("SELECT id FROM waba_market WHERE name = %s", (name,))
    row = cursor.fetchone()
    if row:
        return row[0]
    cursor.execute("INSERT INTO waba_market (name) VALUES (%s)", (name,))
    return cursor.lastrowid


def _get_message_type_id(cursor, code: str) -> int:
    """Return the id for *code* in waba_message_type."""
    cursor.execute(
        "SELECT id FROM waba_message_type WHERE code = %s", (code,)
    )
    row = cursor.fetchone()
    if row is None:
        raise ValueError(f"Unknown message type code: '{code}'")
    return row[0]


def _close_previous_load(cursor, currency: str, file_type: FileType, valid_from) -> None:
    """Set valid_to on the currently-active load for this currency + file_type.

    The previous period closes the day before the new one starts.
    """
    closing_date = day_before(valid_from)
    cursor.execute(
        """
        UPDATE waba_pricing_load
           SET valid_to = %s
         WHERE currency  = %s
           AND file_type = %s
           AND valid_to IS NULL
        """,
        (closing_date, currency, file_type.value),
    )
    if cursor.rowcount:
        logger.info(
            "Closed %d previous '%s' load(s) for currency %s (valid_to → %s).",
            cursor.rowcount, file_type.value, currency, closing_date,
        )


def _create_load(
    cursor,
    currency: str,
    file_type: FileType,
    file_name: str,
    valid_from,
    valid_to,
    uploaded_by: str | None,
    notes: str | None,
) -> int:
    """Insert a new waba_pricing_load row and return its id."""
    cursor.execute(
        """
        INSERT INTO waba_pricing_load
            (currency, file_type, file_name, valid_from, valid_to,
             uploaded_by, notes)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (currency, file_type.value, file_name, valid_from, valid_to,
         uploaded_by, notes),
    )
    load_id: int = cursor.lastrowid
    logger.info(
        "Created pricing load id=%d  type=%s  currency=%s  valid=%s → %s",
        load_id, file_type.value, currency, valid_from, valid_to or "open",
    )
    return load_id


# ---------------------------------------------------------------------------
# Insertion routines
# ---------------------------------------------------------------------------

def _insert_base_rates(
    cursor, load_id: int, records: list[BaseRateRecord]
) -> None:
    # Pre-load market cache and message-type cache to minimise round-trips
    market_cache: dict[str, int] = {}
    msg_type_cache: dict[str, int] = {}

    rows: list[tuple] = []
    for rec in records:
        if rec.market not in market_cache:
            market_cache[rec.market] = _get_or_create_market(cursor, rec.market)
        if rec.message_type_code not in msg_type_cache:
            msg_type_cache[rec.message_type_code] = _get_message_type_id(
                cursor, rec.message_type_code
            )
        rows.append((
            load_id,
            market_cache[rec.market],
            msg_type_cache[rec.message_type_code],
            rec.rate,
        ))

    cursor.executemany(
        """
        INSERT INTO waba_base_rate
            (load_id, market_id, message_type_id, rate)
        VALUES (%s, %s, %s, %s)
        """,
        rows,
    )
    logger.info("Inserted %d base-rate rows.", len(rows))


def _insert_tier_rates(
    cursor, load_id: int, records: list[TierRateRecord]
) -> None:
    market_cache: dict[str, int] = {}
    msg_type_cache: dict[str, int] = {}

    rows: list[tuple] = []
    for rec in records:
        if rec.market not in market_cache:
            market_cache[rec.market] = _get_or_create_market(cursor, rec.market)
        if rec.message_type_code not in msg_type_cache:
            msg_type_cache[rec.message_type_code] = _get_message_type_id(
                cursor, rec.message_type_code
            )
        rows.append((
            load_id,
            market_cache[rec.market],
            msg_type_cache[rec.message_type_code],
            rec.volume_from,
            rec.volume_to,
            rec.rate_type.value,
            rec.rate,
            rec.discount_pct,
        ))

    cursor.executemany(
        """
        INSERT INTO waba_tier_rate
            (load_id, market_id, message_type_id,
             volume_from, volume_to, rate_type, rate, discount_pct)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        rows,
    )
    logger.info("Inserted %d tier-rate rows.", len(rows))


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def run_load(
    filepath: Path,
    valid_from_str: str,
    valid_to_str: str | None,
    uploaded_by: str | None,
    notes: str | None,
    dry_run: bool,
) -> None:
    """Full pipeline: detect → parse → validate → write to DB."""
    valid_from = parse_date(valid_from_str)
    valid_to   = parse_date(valid_to_str) if valid_to_str else None

    if valid_to and valid_to < valid_from:
        raise ValueError(
            f"--valid-to ({valid_to}) must be on or after --valid-from ({valid_from})."
        )

    file_type = _detect_file_type(filepath)
    currency  = _detect_currency(filepath, file_type)

    logger.info(
        "File: '%s'  |  type=%s  currency=%s  valid=%s → %s",
        filepath.name, file_type.value, currency, valid_from, valid_to or "open",
    )

    # Parse CSV into in-memory records
    if file_type is FileType.BASE:
        records = load_base_rates(filepath)
    else:
        records = load_tier_rates(filepath)

    if not records:
        logger.warning("No records parsed – nothing to insert.")
        return

    if dry_run:
        logger.info(
            "Dry-run mode: %d records parsed successfully. No DB writes performed.",
            len(records),
        )
        return

    # Write to database inside a single transaction
    with get_connection() as conn:
        cursor = conn.cursor()
        try:
            _close_previous_load(cursor, currency, file_type, valid_from)
            load_id = _create_load(
                cursor,
                currency=currency,
                file_type=file_type,
                file_name=filepath.name,
                valid_from=valid_from,
                valid_to=valid_to,
                uploaded_by=uploaded_by,
                notes=notes,
            )
            if file_type is FileType.BASE:
                _insert_base_rates(cursor, load_id, records)  # type: ignore[arg-type]
            else:
                _insert_tier_rates(cursor, load_id, records)  # type: ignore[arg-type]
        finally:
            cursor.close()

    logger.info("Load completed successfully.")


def run_load_directory(
    dirpath: Path,
    valid_from_str: str,
    valid_to_str: str | None,
    uploaded_by: str | None,
    notes: str | None,
    dry_run: bool,
) -> int:
    """Process every *.csv file found in *dirpath*.

    Files are sorted alphabetically before processing so execution order is
    deterministic.  Each file is handled independently: a failure in one file
    is logged and counted, but processing continues for the remaining files.

    Returns the number of files that failed.
    """
    csv_files = sorted(dirpath.glob("*.csv"))
    if not csv_files:
        logger.warning("No .csv files found in '%s'. Nothing to do.", dirpath)
        return 0

    logger.info(
        "Bulk load: %d CSV file(s) found in '%s'.", len(csv_files), dirpath
    )

    failed: list[Path] = []
    for idx, filepath in enumerate(csv_files, start=1):
        logger.info("--- [%d/%d] Processing '%s' ---", idx, len(csv_files), filepath.name)
        try:
            run_load(
                filepath=filepath,
                valid_from_str=valid_from_str,
                valid_to_str=valid_to_str,
                uploaded_by=uploaded_by,
                notes=notes,
                dry_run=dry_run,
            )
        except Exception as exc:
            logger.error("Failed to process '%s': %s", filepath.name, exc)
            failed.append(filepath)

    total = len(csv_files)
    ok = total - len(failed)
    logger.info("Bulk load finished: %d/%d file(s) loaded successfully.", ok, total)
    if failed:
        logger.error(
            "Failed files (%d): %s",
            len(failed),
            ", ".join(f.name for f in failed),
        )

    return len(failed)


def run_init_db() -> None:
    """Initialise the database schema (idempotent)."""
    logger.info(
        "Initialising schema on %s:%s/%s …",
        DB_CONFIG.host, DB_CONFIG.port, DB_CONFIG.database,
    )
    with get_connection() as conn:
        init_schema(conn)
    logger.info("Schema ready.")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pricing-meta",
        description="Ingest Meta WhatsApp pricing CSVs into MariaDB.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    # Mutually exclusive top-level actions
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument(
        "--init-db",
        action="store_true",
        help="Create tables and seed reference data (safe to re-run).",
    )
    action.add_argument(
        "--file",
        metavar="PATH",
        type=Path,
        help="Path to a Pricing.csv or Tier Pricing.csv file to ingest.",
    )
    action.add_argument(
        "--dir",
        metavar="DIR",
        type=Path,
        help="Directory containing .csv files to ingest in bulk (sorted alphabetically).",
    )

    # Load options (only relevant when --file is provided)
    parser.add_argument(
        "--valid-from",
        metavar="YYYY-MM-DD",
        help="Start date for this pricing period (required with --file).",
    )
    parser.add_argument(
        "--valid-to",
        metavar="YYYY-MM-DD",
        default=None,
        help=(
            "End date for this pricing period.  "
            "If omitted, the period is left open (NULL in DB).  "
            "The previous active period is closed automatically."
        ),
    )
    parser.add_argument(
        "--uploaded-by",
        metavar="NAME",
        default=None,
        help="Name or identifier of the person loading the file.",
    )
    parser.add_argument(
        "--notes",
        metavar="TEXT",
        default=None,
        help="Free-text annotation stored with the load record.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse the CSV and report record counts without writing to the DB.",
    )

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    try:
        if args.init_db:
            run_init_db()
        elif args.file:
            if not args.valid_from:
                parser.error("--valid-from is required when using --file.")
            if not args.file.exists():
                parser.error(f"File not found: {args.file}")
            run_load(
                filepath=args.file,
                valid_from_str=args.valid_from,
                valid_to_str=args.valid_to,
                uploaded_by=args.uploaded_by,
                notes=args.notes,
                dry_run=args.dry_run,
            )
        else:
            # --dir was provided
            if not args.valid_from:
                parser.error("--valid-from is required when using --dir.")
            if not args.dir.is_dir():
                parser.error(f"Directory not found: {args.dir}")
            failed_count = run_load_directory(
                dirpath=args.dir,
                valid_from_str=args.valid_from,
                valid_to_str=args.valid_to,
                uploaded_by=args.uploaded_by,
                notes=args.notes,
                dry_run=args.dry_run,
            )
            if failed_count:
                return 1
    except (ValueError, FileNotFoundError) as exc:
        logger.error("%s", exc)
        return 1
    except Exception as exc:
        logger.exception("Unexpected error: %s", exc)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
