"""
MariaDB connection management.

Usage
-----
    from db.connection import get_connection

    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        # connection is committed on exit; rolled back on exception
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Generator

import mysql.connector
from mysql.connector import MySQLConnection

from config import DB_CONFIG

logger = logging.getLogger(__name__)


@contextmanager
def get_connection() -> Generator[MySQLConnection, None, None]:
    """Yield an open MariaDB connection inside a transaction.

    - Commits automatically on clean exit.
    - Rolls back and re-raises on any exception.
    - Always closes the connection on exit.
    """
    conn: MySQLConnection = mysql.connector.connect(
        host=DB_CONFIG.host,
        port=DB_CONFIG.port,
        user=DB_CONFIG.user,
        password=DB_CONFIG.password,
        database=DB_CONFIG.database,
        autocommit=False,
        charset="utf8mb4",
    )
    logger.debug("Database connection opened to %s:%s/%s", DB_CONFIG.host, DB_CONFIG.port, DB_CONFIG.database)
    try:
        yield conn
        conn.commit()
        logger.debug("Transaction committed.")
    except Exception:
        conn.rollback()
        logger.warning("Transaction rolled back due to an error.")
        raise
    finally:
        conn.close()
        logger.debug("Database connection closed.")
