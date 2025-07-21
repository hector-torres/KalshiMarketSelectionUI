import math
import pandas as pd
import numpy as np
import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder

from app.database import Database


class MarketDashboard:
    def __init__(self):
        st.set_page_config(page_title="Market Events", layout="wide")
        self.db = Database()
        self.df = pd.DataFrame()

        # Display current mode
        dbg = getattr(self.db, 'debug_level', 0)
        mode_map = {
            0: "Production (daily refresh)",
            1: "Force Refresh Mode",
            2: "Offline Mode"
        }
        st.sidebar.markdown(
            f"**Mode:** `{dbg}` — {mode_map.get(dbg, 'Unknown')}")

    def load_data(self):
        # Fetch from local cache (with refresh logic)
        self.df = self.db.fetch_markets()

        # Parse datetimes
        self.df["open_time"] = pd.to_datetime(
            self.df["open_time"], utc=True, errors="coerce", exact=False
        ).dt.tz_convert(None)
        self.df["close_time"] = pd.to_datetime(
            self.df["close_time"], utc=True, errors="coerce", exact=False
        ).dt.tz_convert(None)

        # Prices: convert from cents to dollars
        self.df["last_price"] = pd.to_numeric(self.df["last_price"], errors="coerce")
        self.df["last_price_dollars"] = self.df["last_price"] / 100.0

        # Compute time filter helper
        now = pd.Timestamp.now()
        self.df["days_to_close"] = (self.df["close_time"] - now).dt.days

    def render(self):
        # — Sidebar: Search & Filters —
        st.sidebar.header("Search & Filters")
        search_term = st.sidebar.text_input("Search")

        # — Sidebar: Category Filter —
        cats = sorted(self.df["category"].dropna().unique())
        selected_cats = st.sidebar.multiselect("Category", options=cats, default=cats)
        # — Sidebar: Time-to-Close Filter —
        st.sidebar.header("Time to Close")
        # Determine maximum days to close from data
        max_close_val = int(self.df["days_to_close"].max())
        min_days, max_days = st.sidebar.slider(
            "Days until close",
            min_value=0,
            max_value=max_close_val,
            value=(0, max_close_val),
            step=1,
            help="Filter by days until market close"
        )

        # — Sidebar: Price Range Filter —
        st.sidebar.header("Price Range")
        price_min, price_max = st.sidebar.slider(
            "Price range ($)",
            min_value=0.00,
            max_value=0.99,
            value=(0.00, 0.50),
            step=0.01,
            help="Filter markets by last Yes price in dollars"
        )

        # — Sidebar: Pagination —
        st.sidebar.header("📄 Pagination")
        # page_size = st.sidebar.number_input("Rows per page", 1, 100, 10, step=1)
        page_size = 10

        # — Apply Filters —
        df_filtered = self.df.copy()
        if search_term:
            mask = (
                    df_filtered["title"].fillna("").str.contains(search_term, case=False)
                    | df_filtered["sub_title"].fillna("").str.contains(search_term, case=False)
                    | df_filtered["event_title"].fillna("").str.contains(search_term, case=False)
            )
            df_filtered = df_filtered[mask]
        if selected_cats:
            df_filtered = df_filtered[df_filtered["category"].isin(selected_cats)]
        # Time-to-close
        if max_days < 30:
            df_filtered = df_filtered[df_filtered["days_to_close"].between(min_days, max_days)]
        else:
            df_filtered = df_filtered[df_filtered["days_to_close"] >= min_days]
        # Price range
        df_filtered = df_filtered[df_filtered["last_price_dollars"].between(price_min, price_max)]

        # — Pagination Logic —
        total = len(df_filtered)
        total_pages = max(1, math.ceil(total / page_size))
        page = st.sidebar.number_input("Page number", 1, total_pages, 1)
        start, end = (page - 1) * page_size, page * page_size
        df_page = df_filtered.iloc[start:end]

        # — Main View: Table & Selections —
        st.title("Market Events Dashboard")
        st.write(f"Showing rows {start + 1}–{min(end, total)} of {total}")

        # Add display column for price
        df_page = df_page.assign(last_price_dollars=df_page["last_price_dollars"].round(2))

        # Add display column for price
        df_page = df_page.assign(last_price_dollars=df_page["last_price_dollars"].round(2))

        gb = GridOptionsBuilder.from_dataframe(df_page)
        # Enable multi-row selection with checkboxes
        gb.configure_selection(selection_mode="multiple", use_checkbox=True)

        # Hide technical columns
        gb.configure_column("market_event_ticker", hide=True)
        gb.configure_column("last_price", hide=True)
        gb.configure_column("market_rules_primary", hide=True)

        # Set minimum widths: Title 3x other columns
        gb.configure_default_column(minWidth=100)
        gb.configure_column("title", minWidth=300)

        grid_opts = gb.build()

        grid_response = AgGrid(
            df_page,
            gridOptions=grid_opts,
            enable_enterprise_modules=False,
            fit_columns_on_grid_load=True,
            height=400,
            width="100%",
        )

        # — Selected for Archive —
        sel = pd.DataFrame(grid_response["selected_rows"])
        if not sel.empty:
            st.markdown("---")
            st.subheader("Selected for Analysis")
            st.dataframe(
                sel[["market_event_ticker", "title", "sub_title", "event_title", "category"]],
                use_container_width=True
            )
            if st.button("Save Selected to Archive"):
                n = self.db.archive(sel)
                st.success(f"Saved {n} new rows to archive DB.")

                # — Detail View via Selection —
        if not sel.empty:
            # Show details for each selected row
            for row in sel.itertuples(index=False):
                st.markdown("---")
                st.subheader(f"Details for `{row.market_event_ticker}`")
                st.write("**Title:**", row.title)
                st.write("**Subtitle:**", row.sub_title)
                st.write("**Event Title:**", row.event_title)
                st.write("**Market Rules:**", row.market_rules_primary)
                st.write("**Category:**", row.category)

    def run(self):
        self.load_data()
        self.render()
