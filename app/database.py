import os
import sqlite3
import requests
import pandas as pd
import json
import logging
from dotenv import load_dotenv
from datetime import datetime, timedelta


# Initialize module-level logger
logger = logging.getLogger(__name__)

class Database:
    def __init__(self):
        # Load environment variables
        load_dotenv()
        # Debug flag
        self.debug = os.getenv('DEBUG', '0') == '1'
        if self.debug:
            logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
            logger.info('Debug mode is ON')
        # Prepare database directory
        base_dir = os.path.abspath(os.path.dirname(__file__))
        db_dir = os.path.join(base_dir, '..', 'database')
        os.makedirs(db_dir, exist_ok=True)
        # Paths for caches
        self.db_path = os.path.join(db_dir, 'markets.sqlite')
        self.archive_path = os.getenv('ARCHIVE_DATABASE_URL')
        if not self.archive_path:
            raise RuntimeError('ARCHIVE_DATABASE_URL must be set in .env')

    def get_conn(self):
        """
        Ensure the markets cache is refreshed (if needed), then return a connection.
        """
        self._refresh_markets_if_needed()
        return sqlite3.connect(self.db_path)

    def _refresh_markets_if_needed(self):
        """
        Fetch all open markets once per day (or always if DEBUG=1), using cursor pagination,
        and store in local SQLite cache.
        """
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        # Create metadata table if missing
        cur.execute("CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT)")
        conn.commit()
        # Check last refresh
        cur.execute("SELECT value FROM metadata WHERE key='last_refresh'")
        row = cur.fetchone()
        last_refresh = datetime.fromisoformat(row[0]) if row else None
        stale = (not last_refresh) or (datetime.now() - last_refresh > timedelta(days=1))
        # Check table existence and schema
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='markets'")
        exists = cur.fetchone() is not None
        if exists:
            cur.execute("PRAGMA table_info(markets)")
            cols = {r[1] for r in cur.fetchall()}
            required = {'ticker', 'subtitle', 'open_time', 'close_time', 'last_price'}
            schema_ok = required.issubset(cols)
        else:
            schema_ok = False
        # Decide if refresh
        if self.debug or (not exists) or stale or (not schema_ok):
            if self.debug:
                logger.info(f"Refreshing market cache: exists={exists}, stale={stale}, schema_ok={schema_ok}")
            # Fetch all pages
            all_markets = []
            cursor = None
            url = 'https://api.elections.kalshi.com/trade-api/v2/markets'
            while True:
                params = {'limit': 1000, 'status': 'open'}
                if cursor:
                    params['cursor'] = cursor
                if self.debug:
                    logger.info(f"Requesting page (cursor={cursor})")
                resp = requests.get(url, params=params, timeout=15)
                resp.raise_for_status()
                data = resp.json()
                batch = data.get('markets', [])
                all_markets.extend(batch)
                if self.debug:
                    logger.info(f"Fetched {len(batch)} markets")
                cursor = data.get('cursor')
                if not cursor:
                    break
            if self.debug:
                logger.info(f"Total markets fetched: {len(all_markets)}")
            # Recreate markets table
            cur.execute("DROP TABLE IF EXISTS markets")
            # Define schema
            schema_fields = [
                ('ticker', 'TEXT PRIMARY KEY'), ('event_ticker', 'TEXT'), ('market_type', 'TEXT'),
                ('title', 'TEXT'), ('subtitle', 'TEXT'), ('yes_sub_title', 'TEXT'), ('no_sub_title', 'TEXT'),
                ('open_time', 'TEXT'), ('close_time', 'TEXT'), ('expected_expiration_time', 'TEXT'),
                ('expiration_time', 'TEXT'), ('latest_expiration_time', 'TEXT'),
                ('settlement_timer_seconds', 'REAL'), ('status', 'TEXT'), ('response_price_units', 'TEXT'),
                ('notional_value', 'REAL'), ('tick_size', 'REAL'), ('yes_bid', 'REAL'), ('yes_ask', 'REAL'),
                ('no_bid', 'REAL'), ('no_ask', 'REAL'), ('last_price', 'REAL'),
                ('previous_yes_bid', 'REAL'), ('previous_yes_ask', 'REAL'), ('previous_price', 'REAL'),
                ('volume', 'REAL'), ('volume_24h', 'REAL'), ('liquidity', 'REAL'),
                ('open_interest', 'REAL'), ('result', 'TEXT'),
                ('can_close_early', 'INTEGER'), ('expiration_value', 'TEXT'), ('category', 'TEXT'),
                ('risk_limit_cents', 'REAL'), ('strike_type', 'TEXT'), ('custom_strike', 'TEXT'),
                ('rules_primary', 'TEXT'), ('rules_secondary', 'TEXT')
            ]
            cols_sql = ", ".join(f"{name} {typ}" for name, typ in schema_fields)
            cur.execute(f"CREATE TABLE markets ({cols_sql})")
            # Insert data
            col_names = [name for name, _ in schema_fields]
            placeholders = ",".join('?' for _ in col_names)
            insert_sql = f"INSERT OR IGNORE INTO markets ({','.join(col_names)}) VALUES ({placeholders})"
            for m in all_markets:
                vals = []
                for name in col_names:
                    if name == 'custom_strike':
                        vals.append(json.dumps(m.get('custom_strike', {})))
                    elif name == 'can_close_early':
                        vals.append(1 if m.get('can_close_early') else 0)
                    else:
                        vals.append(m.get(name))
                cur.execute(insert_sql, vals)
            # Update timestamp
            now_str = datetime.now().isoformat()
            cur.execute("INSERT OR REPLACE INTO metadata (key, value) VALUES ('last_refresh', ?)" , (now_str,))
            conn.commit()
            if self.debug:
                count = cur.execute("SELECT COUNT(*) FROM markets").fetchone()[0]
                logger.info(f"Markets table row count: {count}")
        conn.close()

    def fetch_markets(self) -> pd.DataFrame:
        """Return DataFrame for UI, reading from local markets cache."""
        self._refresh_markets_if_needed()
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql_query(
            """
            SELECT
              title,
              subtitle AS sub_title,
              ticker AS market_event_ticker,
              event_ticker AS event_title,
              rules_primary AS market_rules_primary,
              category,
              open_time,
              close_time,
              last_price
            FROM markets
            """,
            conn
        )
        conn.close()
        if self.debug:
            logger.info(f"fetch_markets returned {len(df)} rows")
        return df

    def archive(self, df: pd.DataFrame) -> int:
        """Append selected rows to selected_markets DB, skipping duplicates."""
        conn = sqlite3.connect(self.archive_path)
        cur = conn.cursor()
        cur.execute(
            "CREATE TABLE IF NOT EXISTS selected_markets ("
            "title TEXT, sub_title TEXT, market_event_ticker TEXT PRIMARY KEY,"
            "event_title TEXT, market_rules_primary TEXT, category TEXT)"
        )
        conn.commit()
        inserted = 0
        for row in df.itertuples(index=False):
            cur.execute(
                "INSERT OR IGNORE INTO selected_markets "
                "(title, sub_title, market_event_ticker, event_title, market_rules_primary, category) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (row.title, row.sub_title, row.market_event_ticker, row.event_title, row.market_rules_primary, row.category)
            )
            if cur.rowcount:
                inserted += 1
        conn.commit()
        conn.close()
        return inserted
