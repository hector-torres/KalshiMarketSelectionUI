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

    def load_data(self):
        self.df = self.db.fetch_markets()
        # parse datetimes and prices
        self.df["open_time"] = pd.to_datetime(
            self.df["open_time"],
            utc=True,
            errors="coerce",
            exact=False
        ).dt.tz_convert(None)
        self.df["close_time"] = pd.to_datetime(
            self.df["close_time"],
            utc=True,
            errors="coerce",
            exact=False
        ).dt.tz_convert(None)
        self.df["last_price"] = pd.to_numeric(self.df["last_price"], errors="coerce")
        # compute filter helpers
        now = pd.Timestamp.now()
        self.df["days_to_close"] = (self.df["close_time"] - now).dt.days
        clipped = self.df["last_price"].clip(0, 1)
        self.df["price_distance"] = np.minimum(clipped, 1 - clipped) * 100

    def render(self):
        st.sidebar.header("🔎 Search & Filters")
        search_term = st.sidebar.text_input("Search title / subtitle / event…")

        cats = sorted(self.df["category"].dropna().unique())
        selected_cats = st.sidebar.multiselect("Category", options=cats, default=cats)

        st.sidebar.header("⏳ Time to Close")
        min_days, max_days = st.sidebar.slider(
            "Days until close", 0, 30, (0, 30), step=1,
            help="Filter by days until market close (30 = no upper bound)"
        )

        st.sidebar.header("💲 Distance to $0 or $1")
        min_cents, max_cents = st.sidebar.slider(
            "Price distance from boundaries (in cents)", 0, 50, (0, 50), step=1,
            help="Filter by closeness to $0 or $1 (50 = no upper bound)"
        )

        st.sidebar.header("📄 Pagination")
        page_size = st.sidebar.number_input("Rows per page", 1, 100, 10, step=1)

        st.sidebar.header("📝 Detail View")
        ticker_opts = [""] + list(self.df["market_event_ticker"])
        selected_ticker = st.sidebar.selectbox("Select ticker", options=ticker_opts, index=0)

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
        if max_days < 30:
            df_filtered = df_filtered[df_filtered["days_to_close"].between(min_days, max_days)]
        else:
            df_filtered = df_filtered[df_filtered["days_to_close"] >= min_days]
        if max_cents < 50:
            df_filtered = df_filtered[df_filtered["price_distance"].between(min_cents, max_cents)]
        else:
            df_filtered = df_filtered[df_filtered["price_distance"] >= min_cents]

        total = len(df_filtered)
        total_pages = max(1, math.ceil(total / page_size))
        page = st.sidebar.number_input("Page number", 1, total_pages, 1)
        start, end = (page - 1) * page_size, page * page_size
        df_page = df_filtered.iloc[start:end]

        st.title("📊 Market Events Dashboard")
        st.write(f"Showing rows {start+1}–{min(end, total)} of {total}")

        gb = GridOptionsBuilder.from_dataframe(df_page)
        gb.configure_selection(selection_mode="multiple", use_checkbox=True)
        grid_response = AgGrid(
            df_page,
            gridOptions=gb.build(),
            enable_enterprise_modules=False,
            fit_columns_on_grid_load=True,
            height=400,
        )

        sel = pd.DataFrame(grid_response["selected_rows"])
        if not sel.empty:
            st.markdown("---")
            st.subheader("🔍 Selected for Analysis")
            st.dataframe(sel[["market_event_ticker", "title", "sub_title", "event_title", "category"]], use_container_width=True)
            if st.button("💾 Save Selected to Archive"):
                n = self.db.archive(sel)
                st.success(f"Saved {n} new rows to archive DB.")

        if selected_ticker:
            rec = self.df[self.df["market_event_ticker"] == selected_ticker].iloc[0]
            st.markdown("---")
            st.subheader(f"Details for `{selected_ticker}`")
            st.write("**Title:**", rec["title"])
            st.write("**Subtitle:**", rec["sub_title"])
            st.write("**Event Title:**", rec["event_title"])
            st.write("**Market Rules:**", rec["market_rules_primary"])
            st.write("**Category:**", rec["category"])

    def run(self):
        self.load_data()
        self.render()
