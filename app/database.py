import os
import sqlite3

import pandas as pd
from dotenv import load_dotenv

class Database:
    def __init__(self):
        # load .env into os.environ
        load_dotenv()
        self.db_path = os.getenv("DATABASE_URL")
        self.archive_path = os.getenv("ARCHIVE_DATABASE_URL")

    def get_conn(self):
        """Connection to the main markets DB."""
        return sqlite3.connect(self.db_path)

    def get_archive_conn(self):
        """Connection to the archive DB, ensure table exists."""
        conn = sqlite3.connect(self.archive_path)
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS selected_markets (
                title TEXT,
                sub_title TEXT,
                market_event_ticker TEXT PRIMARY KEY,
                event_title TEXT,
                market_rules_primary TEXT,
                category TEXT
            )
        """)
        conn.commit()
        return conn

    def fetch_markets(self) -> pd.DataFrame:
        """Load and dedupe on market_event_ticker."""
        with self.get_conn() as conn:
            df = pd.read_sql_query("""
                SELECT
                    title,
                    sub_title,
                    market_event_ticker,
                    event_title,
                    market_rules_primary,
                    category
                FROM markets
            """, conn)
        return df.drop_duplicates(subset=["market_event_ticker"])\
                 .reset_index(drop=True)

    def archive(self, df: pd.DataFrame) -> int:
        """
        Append selected rows to selected_markets,
        skipping duplicates via PRIMARY KEY constraint.
        Returns the number of rows actually inserted.
        """
        inserted = 0
        conn = self.get_archive_conn()
        cur = conn.cursor()
        for row in df.itertuples(index=False):
            try:
                cur.execute(
                    "INSERT INTO selected_markets "
                    "(title, sub_title, market_event_ticker, event_title, market_rules_primary, category) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        row.title,
                        row.sub_title,
                        row.market_event_ticker,
                        row.event_title,
                        row.market_rules_primary,
                        row.category,
                    ),
                )
                inserted += 1
            except sqlite3.IntegrityError:
                # duplicate ticker → skip
                continue
        conn.commit()
        conn.close()
        return inserted