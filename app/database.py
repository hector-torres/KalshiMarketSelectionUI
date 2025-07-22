import os
import sqlite3
import requests
import pandas as pd
import json
import logging
import time
from dotenv import load_dotenv
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class Database:
    def __init__(self):
        load_dotenv()
        # Debug levels:
        # 0 (default) = normal daily refresh
        # 1 = force refresh every run (always call API)
        # 2 = offline mode (never call API unless table/schema missing)
        self.debug_level = int(os.getenv('DEBUG', '0') or 0)
        if self.debug_level:
            logging.basicConfig(
                level=logging.INFO,
                format='%(asctime)s %(levelname)s: %(message)s'
            )
            logger.info(f'Debug level set to {self.debug_level}')

        # Prepare local database directory
        base_dir = os.path.abspath(os.path.dirname(__file__))
        db_dir = os.path.join(base_dir, '..', 'database')
        os.makedirs(db_dir, exist_ok=True)

        # Local cache DB
        self.db_path = os.path.join(db_dir, 'markets.sqlite')

        # Archive DB for selected_markets (must be provided)
        self.archive_path = os.getenv('ARCHIVE_DATABASE_URL')
        if not self.archive_path:
            raise RuntimeError('ARCHIVE_DATABASE_URL must be set in .env')

    def get_conn(self):
        """Ensure cache policy, then return a connection."""
        self._refresh_markets_if_needed()
        return sqlite3.connect(self.db_path)

    def fetch_markets(self) -> pd.DataFrame:
        """Return DataFrame for UI from local cache (refresh logic applied)."""
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
        if self.debug_level:
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
                (
                    row.title,
                    row.sub_title,
                    row.market_event_ticker,
                    row.event_title,
                    row.market_rules_primary,
                    row.category,
                )
            )
            if cur.rowcount:
                inserted += 1

        conn.commit()
        conn.close()
        return inserted

    def _refresh_markets_if_needed(self):
        """
        Refresh policy:
          DEBUG=0: refresh if table missing OR >24h stale OR schema incomplete.
          DEBUG=1: always refresh (force API).
          DEBUG=2: offline mode (never refresh; use cached DB only unless unusable).
        """
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()

        # Metadata table
        cur.execute("CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT)")
        conn.commit()

        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='markets'")
        table_exists = cur.fetchone() is not None
        required = {'ticker', 'subtitle', 'open_time', 'close_time', 'last_price'}
        if table_exists:
            cur.execute("PRAGMA table_info(markets)")
            existing_cols = {r[1] for r in cur.fetchall()}
            schema_ok = required.issubset(existing_cols)
        else:
            schema_ok = False

        cur.execute("SELECT value FROM metadata WHERE key='last_refresh'")
        row = cur.fetchone()
        last_refresh = datetime.fromisoformat(row[0]) if row else None
        stale = (not last_refresh) or (datetime.now() - last_refresh > timedelta(days=1))

        if self.debug_level == 2:
            if not schema_ok:
                logger.info("DEBUG=2 (offline) but table missing/incomplete -> performing one-time fetch.")
                self._fetch_and_rebuild(cur, conn)
            else:
                logger.info("DEBUG=2: using cached markets (no API call).")
                count = cur.execute("SELECT COUNT(*) FROM markets").fetchone()[0]
                if count == 0:
                    logger.warning("DEBUG=2: markets table empty.")
        else:
            must_refresh = (
                self.debug_level == 1
                or not table_exists
                or stale
                or not schema_ok
            )
            if must_refresh:
                logger.info(
                    f"Refreshing markets (debug_level={self.debug_level}, "
                    f"table_exists={table_exists}, stale={stale}, schema_ok={schema_ok})"
                )
                self._fetch_and_rebuild(cur, conn)
            else:
                logger.info("Markets cache is fresh – no refresh needed.")

        conn.close()

    def _fetch_and_rebuild(self, cur, conn):
        """
        Fetch all open markets via cursor pagination and rebuild the markets table.
        """
        all_markets = []
        cursor_token = None
        url = 'https://api.elections.kalshi.com/trade-api/v2/markets'
        page = 0

        while True:
            page += 1
            params = {'limit': 1000, 'status': 'open'}
            if cursor_token:
                params['cursor'] = cursor_token
            if self.debug_level:
                logger.info(f"API fetch page {page} (cursor={cursor_token})")
            resp = requests.get(url, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            batch = data.get('markets', [])
            all_markets.extend(batch)
            if self.debug_level:
                logger.info(f"Page {page}: {len(batch)} markets (cumulative {len(all_markets)})")
            cursor_token = data.get('cursor')
            if not cursor_token:
                break

        logger.info(f"Total markets fetched: {len(all_markets)}")

        # Enrich categories by series lookup
        series_set = {m.get('event_ticker', '').split('-', 1)[0] for m in all_markets}
        series_to_category = {}
        count = 0
        for series in series_set:
            count += 1
            try:
                resp = requests.get(
                    f"https://api.elections.kalshi.com/trade-api/v2/series/{series}",
                    timeout=10
                )
                resp.raise_for_status()
                data = resp.json()
                category_val = data.get('series', {}).get('category', '')
                series_to_category[series] = category_val
            except Exception as e:
                logger.warning(f"Error fetching category for series {series}: {e}")
            finally:
                time.sleep(.12)

            if count == 1:
                logger.info(
                    f"Fetched category data for 1 series: category={category_val}, response={data}"
                )
            elif count % 50 == 0:
                logger.info(f"Fetched category data for {count} series")

        logger.info(f"Series API calls: {len(series_to_category)}")

        for m in all_markets:
            base = m.get('event_ticker', '').split('-', 1)[0]
            m['category'] = series_to_category.get(base, '')

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

        cur.execute("DROP TABLE IF EXISTS markets")
        cols_sql = ", ".join(f"{name} {typ}" for name, typ in schema_fields)
        cur.execute(f"CREATE TABLE markets ({cols_sql})")

        col_names = [name for name, _ in schema_fields]
        placeholders = ",".join("?" for _ in col_names)
        insert_sql = f"INSERT OR IGNORE INTO markets ({','.join(col_names)}) VALUES ({placeholders})"

        for m in all_markets:
            row_vals = []
            for col in col_names:
                if col == 'custom_strike':
                    row_vals.append(json.dumps(m.get('custom_strike', {})))
                elif col == 'can_close_early':
                    row_vals.append(1 if m.get('can_close_early') else 0)
                else:
                    row_vals.append(m.get(col))
            cur.execute(insert_sql, row_vals)

        cur.execute(
            "INSERT OR REPLACE INTO metadata (key, value) VALUES ('last_refresh', ?)" ,
            (datetime.now().isoformat(),)
        )
        conn.commit()

        if self.debug_level:
            count = cur.execute("SELECT COUNT(*) FROM markets").fetchone()[0]
            logger.info(f"Rebuild complete. markets row count = {count}")
