"""
Parser for Pricing.csv (flat / non-tiered base rates).

CSV layout
----------
    Rows 0-4 : metadata / notes (skipped)
    Row  5   : column headers  ← may span two physical lines due to a quoted
                                  newline in "Authentication-\\nInternational"
    Rows 6+  : data

Each data row has one column per message type.  This loader normalises the
wide format into one BaseRateRecord per (market, message_type) pair.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from models import BaseRateRecord

logger = logging.getLogger(__name__)

# Maps the raw CSV column header to the canonical message-type code stored in
# waba_message_type.  The newline inside the quoted header is stripped first.
_COLUMN_TO_MSG_TYPE: dict[str, str] = {
    "Marketing":                   "MARKETING",
    "Utility":                     "UTILITY",
    "Authentication":              "AUTHENTICATION",
    "Authentication-International": "AUTH_INTL",
    "Service":                     "SERVICE",
}

# Meta publishes currency as "$US" rather than the ISO-4217 "USD".
# Add further aliases here as new currency files are introduced.
_CURRENCY_ALIASES: dict[str, str] = {
    "$US": "USD",   # Meta's non-standard token for US Dollar
    "A$":  "AUD",   # Australian Dollar symbol
    "GBD": "GBP",   # Typo found in Meta CSV (should be GBP)
    "£":   "GBP",   # British Pound Sterling symbol
    "₹":   "INR",   # Indian Rupee symbol
    "RP":  "IDR",   # Indonesian Rupiah abbreviation
}


def _normalise_currency(raw: str) -> str:
    value = str(raw).strip()
    return _CURRENCY_ALIASES.get(value, value.upper())


def _normalise_rate(raw) -> float | None:
    """Return None for n/a values, otherwise a float."""
    if pd.isna(raw):
        return None
    text = str(raw).strip().lower()
    if text in ("n/a", "na", "", "--"):
        return None
    return float(raw)


def load_base_rates(filepath: str | Path) -> list[BaseRateRecord]:
    """Parse *Pricing.csv* and return a list of normalised :class:`BaseRateRecord`.

    Args:
        filepath: Path to the Pricing.csv file.

    Returns:
        A list of :class:`BaseRateRecord`, one entry per
        (market, message_type) combination.

    Raises:
        FileNotFoundError: If *filepath* does not exist.
        ValueError: If required columns are missing from the file.
    """
    filepath = Path(filepath)
    if not filepath.exists():
        raise FileNotFoundError(f"File not found: {filepath}")

    logger.info("Reading base rates from '%s'", filepath.name)

    df = pd.read_csv(
        filepath,
        skiprows=5,      # skip 5 logical header/metadata rows
        header=0,
        dtype=str,       # read everything as string; we parse manually
        keep_default_na=False,
    )

    # The quoted newline in "Authentication-\nInternational" may appear as-is
    # in the column name – strip and normalise it.
    df.columns = [col.strip().replace("\n", "") for col in df.columns]

    # Validate that the expected columns are present
    required = {"Market", "Currency"} | set(_COLUMN_TO_MSG_TYPE.keys())
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"Missing expected columns in {filepath.name}: {missing}"
        )

    records: list[BaseRateRecord] = []

    for _, row in df.iterrows():
        market: str = str(row["Market"]).strip()
        currency: str = _normalise_currency(row["Currency"])

        if not market:
            logger.debug("Skipping empty market row.")
            continue

        for csv_col, msg_type_code in _COLUMN_TO_MSG_TYPE.items():
            rate = _normalise_rate(row.get(csv_col))
            records.append(
                BaseRateRecord(
                    market=market,
                    currency=currency,
                    message_type_code=msg_type_code,
                    rate=rate,
                )
            )

    logger.info(
        "Parsed %d base-rate records from '%s'.", len(records), filepath.name
    )
    return records
