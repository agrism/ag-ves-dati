#!/usr/bin/env python3
"""
Database Adapter for Garmin Analytics.
Supports both SQLite (local development) and MySQL (Hetzner / remote production).

Reads configuration from environment variables or .env file.
"""

import os
import re
from pathlib import Path
from typing import Any, List, Dict, Optional, Union, Tuple

# Try loading dotenv if present
try:
    from dotenv import load_dotenv
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)
except ImportError:
    pass

DB_TYPE = os.getenv("DB_TYPE", "sqlite").lower()

WORKSPACE_DIR = Path(__file__).resolve().parent.parent
SQLITE_DB_PATH = Path(os.getenv("SQLITE_DB_PATH", str(WORKSPACE_DIR / "garmin_db" / "garmin.db")))

MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "garmin_db")
MYSQL_USER = os.getenv("MYSQL_USER", "garmin_user")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "garmin_secure_password_here")


class DBConnection:
    """Wrapper that normalizes SQLite and MySQL queries & cursor execution."""

    def __init__(self, db_type: str = DB_TYPE):
        self.db_type = db_type
        self._raw_conn = None
        self._connect()

    def _connect(self):
        if self.db_type == "mysql":
            try:
                import pymysql
                import pymysql.cursors
                self._raw_conn = pymysql.connect(
                    host=MYSQL_HOST,
                    port=MYSQL_PORT,
                    user=MYSQL_USER,
                    password=MYSQL_PASSWORD,
                    database=MYSQL_DATABASE,
                    charset="utf8mb4",
                    cursorclass=pymysql.cursors.DictCursor,
                    autocommit=True
                )
            except ImportError:
                try:
                    import mysql.connector
                    self._raw_conn = mysql.connector.connect(
                        host=MYSQL_HOST,
                        port=MYSQL_PORT,
                        user=MYSQL_USER,
                        password=MYSQL_PASSWORD,
                        database=MYSQL_DATABASE,
                        charset="utf8mb4",
                        autocommit=True
                    )
                except ImportError:
                    raise ImportError("Neither 'pymysql' nor 'mysql-connector-python' is installed. Please run: pip install pymysql")
        else:
            import sqlite3
            SQLITE_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
            self._raw_conn = sqlite3.connect(str(SQLITE_DB_PATH), timeout=60.0)
            self._raw_conn.execute("PRAGMA journal_mode=WAL;")
            self._raw_conn.row_factory = sqlite3.Row

    def _adapt_query(self, query: str) -> str:
        """Translates SQL syntax between SQLite and MySQL."""
        if self.db_type == "mysql":
            # Replace 'INSERT OR REPLACE INTO' with 'REPLACE INTO'
            q = re.sub(r"INSERT\s+OR\s+REPLACE\s+INTO", "REPLACE INTO", query, flags=re.IGNORECASE)
            # Replace '?' parameter placeholders with '%s'
            q = q.replace("?", "%s")
            return q
        return query

    def execute(self, query: str, params: Optional[Union[List, Tuple, Dict]] = None):
        adapted = self._adapt_query(query)
        cursor = self._raw_conn.cursor()
        if params is not None:
            cursor.execute(adapted, params)
        else:
            cursor.execute(adapted)
        return cursor

    def commit(self):
        if self._raw_conn:
            self._raw_conn.commit()

    def close(self):
        if self._raw_conn:
            self._raw_conn.close()

    def cursor(self):
        return self._raw_conn.cursor()

    @property
    def raw_connection(self):
        return self._raw_conn


def get_db(db_type: Optional[str] = None) -> DBConnection:
    """Returns an active DBConnection instance."""
    return DBConnection(db_type=db_type or DB_TYPE)


if __name__ == "__main__":
    print(f"Active DB Type: {DB_TYPE}")
    if DB_TYPE == "mysql":
        print(f"MySQL Target: {MYSQL_USER}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}")
    else:
        print(f"SQLite Target: {SQLITE_DB_PATH}")
    
    db = get_db()
    cur = db.execute("SELECT COUNT(*) as count FROM activities;")
    res = cur.fetchone()
    if isinstance(res, dict):
        count = res["count"]
    else:
        count = res[0]
    print(f"Activities count: {count}")
    db.close()
