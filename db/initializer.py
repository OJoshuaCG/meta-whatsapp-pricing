"""
Database schema initialisation.

Creates all tables and seeds reference data (waba_message_type).
Safe to run multiple times – uses IF NOT EXISTS / INSERT IGNORE.

Usage
-----
    python main.py --init-db
"""

from __future__ import annotations

import logging

from mysql.connector import MySQLConnection

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DDL statements (order matters due to foreign keys)
# ---------------------------------------------------------------------------

_CREATE_STATEMENTS: list[str] = [
    """
    CREATE TABLE IF NOT EXISTS waba_market (
        id   SMALLINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(100) NOT NULL
             COMMENT 'Market name as used in Meta pricing CSVs',
        UNIQUE KEY uq_market_name (name)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
      COMMENT='Meta WhatsApp billing market catalogue'
    """,
    """
    CREATE TABLE IF NOT EXISTS waba_message_type (
        id   TINYINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
        code VARCHAR(30)  NOT NULL,
        name VARCHAR(100) NOT NULL,
        UNIQUE KEY uq_msg_type_code (code)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
      COMMENT='WhatsApp message category catalogue'
    """,
    """
    CREATE TABLE IF NOT EXISTS waba_pricing_load (
        id          INT UNSIGNED        AUTO_INCREMENT PRIMARY KEY,
        currency    CHAR(3)             NOT NULL
                    COMMENT 'ISO 4217 code (e.g. USD)',
        file_type   ENUM('BASE','TIER') NOT NULL
                    COMMENT 'BASE = Pricing.csv | TIER = Tier Pricing.csv',
        file_name   VARCHAR(255)        NULL,
        valid_from  DATE                NOT NULL,
        valid_to    DATE                NULL
                    COMMENT 'NULL = currently active',
        uploaded_at DATETIME            NOT NULL DEFAULT CURRENT_TIMESTAMP,
        uploaded_by VARCHAR(100)        NULL,
        notes       TEXT                NULL,
        UNIQUE KEY uq_load (currency, file_type, valid_from)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
      COMMENT='Audit log of every CSV file ingested'
    """,
    """
    CREATE TABLE IF NOT EXISTS waba_base_rate (
        id              BIGINT UNSIGNED   AUTO_INCREMENT PRIMARY KEY,
        load_id         INT UNSIGNED      NOT NULL,
        market_id       SMALLINT UNSIGNED NOT NULL,
        message_type_id TINYINT UNSIGNED  NOT NULL,
        rate            DECIMAL(10,6)     NULL
                        COMMENT 'NULL = n/a for this market/type',
        KEY idx_base_load   (load_id),
        KEY idx_base_market (market_id, message_type_id),
        CONSTRAINT fk_base_load    FOREIGN KEY (load_id)
            REFERENCES waba_pricing_load (id),
        CONSTRAINT fk_base_market  FOREIGN KEY (market_id)
            REFERENCES waba_market (id),
        CONSTRAINT fk_base_msgtype FOREIGN KEY (message_type_id)
            REFERENCES waba_message_type (id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
      COMMENT='Flat (non-tiered) rates per market and message type'
    """,
    """
    CREATE TABLE IF NOT EXISTS waba_tier_rate (
        id              BIGINT UNSIGNED     AUTO_INCREMENT PRIMARY KEY,
        load_id         INT UNSIGNED        NOT NULL,
        market_id       SMALLINT UNSIGNED   NOT NULL,
        message_type_id TINYINT UNSIGNED    NOT NULL,
        volume_from     INT UNSIGNED        NOT NULL,
        volume_to       INT UNSIGNED        NULL
                        COMMENT 'NULL = unlimited',
        rate_type       ENUM('LIST','TIER') NOT NULL,
        rate            DECIMAL(10,6)       NOT NULL,
        discount_pct    TINYINT             NOT NULL DEFAULT 0
                        COMMENT 'Discount vs. list rate: 0, -5, -10, -15, -20, -25',
        KEY idx_tier_load   (load_id),
        KEY idx_tier_market (market_id, message_type_id),
        CONSTRAINT fk_tier_load    FOREIGN KEY (load_id)
            REFERENCES waba_pricing_load (id),
        CONSTRAINT fk_tier_market  FOREIGN KEY (market_id)
            REFERENCES waba_market (id),
        CONSTRAINT fk_tier_msgtype FOREIGN KEY (message_type_id)
            REFERENCES waba_message_type (id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
      COMMENT='Volume-tiered rates per market and message type'
    """,
]

_SEED_MESSAGE_TYPES: list[tuple[str, str]] = [
    ("MARKETING",      "Marketing"),
    ("UTILITY",        "Utility"),
    ("AUTHENTICATION", "Authentication"),
    ("AUTH_INTL",      "Authentication-International"),
    ("SERVICE",        "Service"),
]

# Markets derived from Meta_Countries.csv.
_SEED_MARKETS: list[str] = [
    # ── Individual country markets ─────────────────────────────────────────
    "Argentina",
    "Brazil",
    "Chile",
    "Colombia",
    "Egypt",
    "France",
    "Germany",
    "India",
    "Indonesia",
    "Israel",
    "Italy",
    "Malaysia",
    "Mexico",
    "Netherlands",
    "Nigeria",
    "Pakistan",
    "Peru",
    "Russia",
    "Saudi Arabia",
    "South Africa",
    "Spain",
    "Turkey",
    "United Arab Emirates",
    "United Kingdom",
    "United States",
    # ── Regional group markets ─────────────────────────────────────────────
    "North America",
    "Rest of Africa",
    "Rest of Asia Pacific",
    "Rest of Central & Eastern Europe",
    "Rest of Latin America",
    "Rest of Middle East",
    "Rest of Western Europe",
    "Other",
]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def init_schema(conn: MySQLConnection) -> None:
    """Create all tables and seed reference data.

    Safe to call multiple times (idempotent).

    Args:
        conn: An open MariaDB connection.  The caller is responsible for
              committing or rolling back.
    """
    cursor = conn.cursor()
    try:
        for statement in _CREATE_STATEMENTS:
            cursor.execute(statement)
            logger.debug("Executed DDL: %s…", statement.strip()[:60])

        cursor.executemany(
            "INSERT IGNORE INTO waba_message_type (code, name) VALUES (%s, %s)",
            _SEED_MESSAGE_TYPES,
        )
        cursor.executemany(
            "INSERT IGNORE INTO waba_market (name) VALUES (%s,)",
            [(m,) for m in _SEED_MARKETS],
        )
        logger.info(
            "Schema initialised. %d message types and %d markets seeded.",
            len(_SEED_MESSAGE_TYPES),
            len(_SEED_MARKETS),
        )
    finally:
        cursor.close()
