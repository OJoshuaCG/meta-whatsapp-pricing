"""
Dataclasses representing parsed pricing records before database insertion.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class FileType(str, Enum):
    BASE = "BASE"
    TIER = "TIER"


class RateType(str, Enum):
    LIST = "LIST"
    TIER = "TIER"


@dataclass(frozen=True)
class BaseRateRecord:
    """One row from Pricing.csv, normalised to a single message type per record."""

    market: str
    currency: str
    message_type_code: str
    rate: float | None  # None represents 'n/a'


@dataclass(frozen=True)
class TierRateRecord:
    """One volume-band row from Tier Pricing.csv for a specific message type."""

    market: str
    currency: str
    message_type_code: str
    volume_from: int
    volume_to: int | None  # None means unlimited ('--')
    rate_type: RateType
    rate: float
    discount_pct: int
