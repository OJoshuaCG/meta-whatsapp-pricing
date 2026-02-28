"""
Parser for Tier Pricing.csv (volume-tiered rates).

CSV layout
----------
    Row 0   : title line
    Row 1   : URL note
    Row 2   : section headers  (,,Utility,,,,,Authentication,,,,,Auth-Intl,,,,)
    Row 3   : sub-headers      (,,Messages per month,,What we charge,, …)
    Row 4   : column headers   (Market, Currency, From, To, Rate type, Rate,
                                vs. List rate  × 3 groups)
              NOTE: the first cell "Market\\n(per rate card)" spans two physical
              lines because of a quoted newline – pandas handles this correctly.
    Rows 5+ : data

The file has three side-by-side groups: Utility, Authentication, and
Authentication-International.  This loader unpacks each group and returns a
flat list of TierRateRecord, one per volume band per message type per market.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from models import RateType, TierRateRecord

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Column layout constants
# ---------------------------------------------------------------------------

# Custom column names assigned when reading (header=None).
# 17 columns: market, currency + 5 cols × 3 message-type groups.
_RAW_COLUMNS: list[str] = [
    "market", "currency",
    # Utility
    "util_from", "util_to", "util_rate_type", "util_rate", "util_discount",
    # Authentication
    "auth_from", "auth_to", "auth_rate_type", "auth_rate", "auth_discount",
    # Authentication-International
    "aintl_from", "aintl_to", "aintl_rate_type", "aintl_rate", "aintl_discount",
]

# Each group maps a canonical message-type code to its set of raw column names.
_GROUPS: dict[str, dict[str, str]] = {
    "UTILITY": {
        "from":      "util_from",
        "to":        "util_to",
        "rate_type": "util_rate_type",
        "rate":      "util_rate",
        "discount":  "util_discount",
    },
    "AUTHENTICATION": {
        "from":      "auth_from",
        "to":        "auth_to",
        "rate_type": "auth_rate_type",
        "rate":      "auth_rate",
        "discount":  "auth_discount",
    },
    "AUTH_INTL": {
        "from":      "aintl_from",
        "to":        "aintl_to",
        "rate_type": "aintl_rate_type",
        "rate":      "aintl_rate",
        "discount":  "aintl_discount",
    },
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


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def _normalise_currency(raw: str) -> str:
    value = str(raw).strip()
    return _CURRENCY_ALIASES.get(value, value.upper())


def _parse_volume(raw) -> int | None:
    """Parse a volume string such as '100,000' → 100000 or '--' → None."""
    text = str(raw).strip().replace(",", "")
    if text in ("--", "n/a", ""):
        return None
    return int(text)


def _parse_rate(raw) -> float:
    """Parse a rate string, handling thousands-separator commas (e.g. '1,940.13' → 1940.13)."""
    text = str(raw).strip().replace(",", "")
    return float(text)


def _parse_discount(raw) -> int:
    """Parse a discount string such as '-5%' → -5 or '0%' → 0."""
    text = str(raw).strip().replace("%", "")
    return int(text)


def _parse_rate_type(raw: str) -> RateType:
    text = str(raw).strip().upper()
    if "TIER" in text:
        return RateType.TIER
    return RateType.LIST


def _is_na_row_for_group(row: pd.Series, group: dict[str, str]) -> bool:
    """Return True when every cell in the group equals 'n/a' (not applicable)."""
    return all(
        str(row[col]).strip().lower() in ("n/a", "na", "")
        for col in group.values()
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def load_tier_rates(filepath: str | Path) -> list[TierRateRecord]:
    """Parse *Tier Pricing.csv* and return a flat list of :class:`TierRateRecord`.

    Args:
        filepath: Path to the Tier Pricing.csv file.

    Returns:
        One :class:`TierRateRecord` per volume band per message type per market.

    Raises:
        FileNotFoundError: If *filepath* does not exist.
        ValueError:        If the file does not have exactly 17 columns.
    """
    filepath = Path(filepath)
    if not filepath.exists():
        raise FileNotFoundError(f"File not found: {filepath}")

    logger.info("Reading tier rates from '%s'", filepath.name)

    df = pd.read_csv(
        filepath,
        skiprows=5,          # skip 5 logical rows (title, URL, 3 header rows)
        header=None,
        names=_RAW_COLUMNS,
        dtype=str,
        keep_default_na=False,
    )

    if df.shape[1] != len(_RAW_COLUMNS):
        raise ValueError(
            f"Expected {len(_RAW_COLUMNS)} columns but found {df.shape[1]} "
            f"in '{filepath.name}'."
        )

    # The market name only appears on the first row of each group; subsequent
    # rows for the same market have an empty market cell.  Forward-fill to
    # propagate the market name down to continuation rows.
    df["market"] = df["market"].replace("", pd.NA).ffill()

    records: list[TierRateRecord] = []

    for _, row in df.iterrows():
        market: str = str(row["market"]).strip()
        currency: str = _normalise_currency(row["currency"])

        if not market or market.lower() == "nan":
            continue

        for msg_type_code, cols in _GROUPS.items():
            if _is_na_row_for_group(row, cols):
                continue  # this message type is not applicable for this row

            try:
                volume_from = _parse_volume(row[cols["from"]])
                volume_to   = _parse_volume(row[cols["to"]])

                if volume_from is None:
                    # Unexpected: 'from' should never be n/a when 'to' is not
                    logger.debug(
                        "Skipping row with unparseable volume_from for "
                        "%s / %s.", market, msg_type_code
                    )
                    continue

                records.append(
                    TierRateRecord(
                        market=market,
                        currency=currency,
                        message_type_code=msg_type_code,
                        volume_from=volume_from,
                        volume_to=volume_to,
                        rate_type=_parse_rate_type(row[cols["rate_type"]]),
                        rate=_parse_rate(row[cols["rate"]]),
                        discount_pct=_parse_discount(row[cols["discount"]]),
                    )
                )
            except (ValueError, KeyError) as exc:
                logger.warning(
                    "Could not parse row for market='%s', type='%s': %s",
                    market, msg_type_code, exc,
                )

    logger.info(
        "Parsed %d tier-rate records from '%s'.", len(records), filepath.name
    )
    return records
