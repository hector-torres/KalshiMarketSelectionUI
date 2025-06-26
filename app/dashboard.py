import math
import pandas as pd
import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder

from app.database import Database


class MarketDashboard:
    def __init__(self):
        # Configure page for full-width layout
        st.set_page_config(page_title="Market Events", layout="wide")

        # Initialize Database helper and data container
        self.db = Database()
        self.df = pd.DataFrame()

    def load_data(self):
        """Load and dedupe markets from the main database."""
        self.df = self.db.fetch_markets()

    def render(self):
        # — Sidebar: Search & Filters —
        st.sidebar.header("🔎 Search & Filters")
        search_term = st.sidebar.text_input("Search title / subtitle / event…")

        # — Sidebar: Category Filter —
        cats = sorted(self.df["category"].dropna().unique())
        selected_cats = st.sidebar.multiselect("Category", options=cats, default=cats)

        # — Sidebar: Pagination —
        st.sidebar.header("📄 Pagination")
        page_size = st.sidebar.number_input("Rows per page", min_value=1, max_value=100, value=10, step=1)

        # — Sidebar: Detail View picker —
        st.sidebar.header("📝 Detail View")
        ticker_opts = [""] + list(self.df["market_event_ticker"])
        selected_ticker = st.sidebar.selectbox("Select ticker", options=ticker_opts, index=0)

        # — Apply filters to DataFrame —
        df_filtered = self.df.copy()
        if search_term:
            mask = (
                df_filtered["title"].fillna("").str.contains(search_term, case=False, regex=False)
                | df_filtered["sub_title"].fillna("").str.contains(search_term, case=False, regex=False)
                | df_filtered["event_title"].fillna("").str.contains(search_term, case=False, regex=False)
            )
            df_filtered = df_filtered[mask]

        if selected_cats:
            df_filtered = df_filtered[df_filtered["category"].isin(selected_cats)]

        total_filtered = len(df_filtered)
        total_pages = max(1, math.ceil(total_filtered / page_size))
        page_number = st.sidebar.number_input(
            "Page number", min_value=1, max_value=total_pages, value=1, step=1
        )

        start = (page_number - 1) * page_size
        end = start + page_size
        df_page = df_filtered.iloc[start:end]

        # — Main View: Title & Table —
        st.title("📊 Market Events Dashboard")
        st.write(f"Showing rows {start+1}–{min(end, total_filtered)} of {total_filtered}")

        # Configure AgGrid for multi-row selection
        gb = GridOptionsBuilder.from_dataframe(df_page)
        gb.configure_selection(selection_mode="multiple", use_checkbox=True)
        grid_opts = gb.build()

        # Render the interactive grid at full width
        grid_response = AgGrid(
            df_page,
            gridOptions=grid_opts,
            enable_enterprise_modules=False,
            fit_columns_on_grid_load=True,
            height=400,
            width="100%",
        )

        # — Show selected rows & Save to Archive Button —
        selected = grid_response["selected_rows"]
        sel_df = pd.DataFrame(selected)
        if not sel_df.empty:
            st.markdown("---")
            st.subheader("🔍 Selected for Analysis")
            st.dataframe(
                sel_df[
                    ["market_event_ticker", "title", "sub_title", "event_title", "category"]
                ],
                use_container_width=True,
            )

            if st.button("💾 Save Selected to Archive"):
                n = self.db.archive(sel_df)
                st.success(f"Saved {n} new rows to archive DB.")

        # — Detail View —
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