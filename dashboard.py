import duckdb
import streamlit as st
import altair as alt
from chart_helper import time_series_chart_maker
from datetime import timedelta, date
import polars as pl

# establish a connection to the database
con = duckdb.connect(
    "migrant_crossings_db.duckdb",
    read_only=True
)

# grab some data
df1 = con.execute('SELECT date_ending, migrants_arrived, boats_arrived FROM latest.migrants_arrived_7_days;').pl()
df2 = con.execute('SELECT date_ending, migrants_arrived, boats_arrived FROM latest.migrants_arrived_daily;').pl()

# dashboard things
st.set_page_config(
    # title to show in web browser bar
    page_title="Small boat activity in the English Channel",
    # layout style
    layout="wide"
)

"""
# Small boat activity in the English Channel

## Managing disinformation

Disinformation is everywhere. With immigration one of the most fiercely debated topics in the UK right now,
social media could be swamped with factually incorrect data, influencing politics and public perception.

This dashboard aims to bring transparency -- at least to figures relating to migrant crossings. There is no disinformation here,
this data comes straight from UK Government statistics and is both fully cited and not altered.

## What is a small boat?

A ‘small boat’ is one of a number of vessels used by individuals who cross the English Channel,
with the aim of gaining entry to the UK without a visa or permission to enter – either directly by landing in the UK or having been intercepted at sea by the authorities and brought ashore.
 
The most common small vessels detected making these types of crossings are rigid-hulled inflatable boats (RHIBs), dinghies and kayaks.
"""

# comparison dates
latest_preliminary_date = df1['date_ending'].max()
latest_date = df2['date_ending'].max()
iso_latest_date = latest_date.isocalendar()
last_year_same_day_comparison_date = latest_date - timedelta(days=365)

# latest preliminary data
latest_migrants_arrived = df1['migrants_arrived'][0]
comparison_migrants_arrived = df2.filter(pl.col('date_ending') == last_year_same_day_comparison_date)['migrants_arrived'][0]

# week summary
start_of_week = latest_date - timedelta(days=latest_date.weekday())
end_of_week = start_of_week + timedelta(days=7)
current_week_total_migrants_arrived = (
    df2
    .filter(
       (pl.col("date_ending") >= start_of_week) &
       (pl.col("date_ending") <= end_of_week)
    )
    .select(pl.col('migrants_arrived').sum())
    .item()
)

# week comparisons
prev_start_of_week = start_of_week - timedelta(days=7)
prev_end_of_week = start_of_week
previous_week_total_migrants_arrived = (
    df2
    .filter(
       (pl.col("date_ending") >= prev_start_of_week) &
       (pl.col("date_ending") < prev_end_of_week)
    )
    .select(pl.col('migrants_arrived').sum())
    .item()
)

# month summary
latest_month_start = date(latest_date.year, latest_date.month, 1)
if latest_date.month == 12:
    next_month_start = date(latest_date.year + 1, 1, 1)
else:
    next_month_start = date(latest_date.year, latest_date.month + 1, 1)

current_month_total_migrants_arrived = (
    df2
    .filter(
       (pl.col("date_ending") >= latest_month_start) &
       (pl.col("date_ending") < next_month_start)
    )
    .select(pl.col('migrants_arrived').sum())
    .item()
)

# month comparison
prev_month_end = latest_month_start - timedelta(days=1)
prev_month_start = date(prev_month_end.year, prev_month_end.month, 1)
previous_month_total_migrants_arrived = (
    df2
    .filter(
        (pl.col('date_ending') >= prev_month_start) &
        (pl.col('date_ending') <= prev_month_end)
    )
    .select(pl.col('migrants_arrived').sum())
    .item()
)

# year summary
current_year_total_migrants_arrived = (
    df2
    .filter(pl.col('date_ending').dt.year() == latest_date.year)
    .select(pl.col('migrants_arrived').sum())
    .item()
)

# year comparison
previous_year_total_migrants_arrived = (
    df2
    .filter(pl.col('date_ending').dt.year() == (latest_date.year - 1))
    .select(pl.col('migrants_arrived').sum())
    .item()
)

f"""
## Summary as of {latest_date:%d %B %Y}

Please note *Total Migrants That Arrived on* is based upon a preliminary dataset subject to change that is updated quicker than
the more comprehensive statistical tables. Other metrics lag behind by approximately **1 week** (new data every Friday).
"""

with st.container(horizontal=True, gap="medium"):
    cols = st.columns(4, gap="medium", width=1400)
    with cols[0]:
        delta_metric_1 = latest_migrants_arrived - comparison_migrants_arrived
        delta_metric_1_inv = -delta_metric_1
        st.metric(
            f"Total Migrants That Arrived on {latest_preliminary_date:%d %B %Y}",
            f"{latest_migrants_arrived}",
            delta=f"{delta_metric_1_inv:,}",
            width="content",
        )

    with cols[1]:
        delta_metric_2 = current_week_total_migrants_arrived - previous_week_total_migrants_arrived
        delta_metric_2_inv = -delta_metric_2
        st.metric(
            "Total Migrants This Week",
            f"{current_week_total_migrants_arrived:,}",
            delta=f"{delta_metric_2_inv:,}",
            width="content",
        )

    with cols[2]:
        delta_metric_3 = current_month_total_migrants_arrived - previous_month_total_migrants_arrived
        delta_metric_3_inv = -delta_metric_3
        st.metric(
            "Total Migrants This Month",
            f"{current_month_total_migrants_arrived:,}",
            delta=f"{delta_metric_3_inv:,}",
            width="content",
        )

    with cols[3]:
        prev_year_total =  current_year_total_migrants_arrived - previous_year_total_migrants_arrived
        st.metric(
            f"Total Migrants This Year",
            f"{current_year_total_migrants_arrived:,}",
            delta=f"{-prev_year_total:,}",
            width="content"
        )

"""
### Migrants arrived on small boats: last 7 days
"""

st.info("""
Statistical data for the below table is updated daily and more up to date than the weekly statistical
returns but is subject to change
""")
tab1, tab2 = st.tabs(["Graph", "Data"])
with tab1:
    cols = st.columns(1)
    with cols[0].container(border=True, height="stretch"):
        st.altair_chart(
            alt.Chart(df1)
            .mark_bar()
            .encode(
                alt.X(
                    "date_ending:N",
                    timeUnit="yearmonthdate",
                    title="Date",
                    axis=alt.Axis(
                        format="%a %e %b",
                        tickBand='center'
                    )
                ),
                alt.Y(
                    "migrants_arrived:Q",
                    aggregate="sum",
                    title="Migrants Arrived"
                )
            )
        )
with tab2:
    st.dataframe(df1)

seven_days_source_text = """
Source: 
<a href="https://www.gov.uk/government/publications/migrants-detected-crossing-the-english-channel-in-small-boats/migrants-detected-crossing-the-english-channel-in-small-boats-last-7-days" 
   target="_blank" 
   rel="noopener noreferrer">
  Migrants detected crossing the English Channel in small boats (last 7 days)
</a>
"""
st.html(seven_days_source_text)

"""
## Time-series data
"""
st.info(f"""
Statistical data for the below tables is updated every Friday.
The current maximum known date is {latest_date:%d %B %Y}.
""")

daily_source_text = """
Source: 
<a href="https://www.gov.uk/government/publications/migrants-detected-crossing-the-english-channel-in-small-boats" 
   target="_blank" 
   rel="noopener noreferrer">
  Small boat activity in the English Channel
</a>
"""

# setup structure
tab1, tab2, tab3 = st.tabs(["Last 30 days", "Last 90 days", "Last 6 months"])
with tab1:
    "### Migrants arrived on small boats: last 30 days"
    seven_days_chart = time_series_chart_maker(data=df2, time_series=7, tickCount=7)
    st.altair_chart(seven_days_chart, use_container_width=True)
with tab2:
    "### Migrants arrived on small boats: last 90 days"
    thirty_days_chart = time_series_chart_maker(data=df2, time_series=30, tickCount=15)
    st.altair_chart(thirty_days_chart, use_container_width=True)
with tab3:
    "### Migrants arrived on small boats: last 6 months"
    months_chart = time_series_chart_maker(data=df2, time_series=180, tickCount=15)
    st.altair_chart(months_chart, use_container_width=True)

st.html(daily_source_text)

"""
## Historical data
"""
tab1, tab2 = st.tabs(["Graph", "Data"])
with tab1:
    st.altair_chart(
            alt.Chart(df2[['date_ending', 'migrants_arrived']])
            .mark_bar()
            .encode(
                alt.X("date_ending:N", timeUnit="month").title("Date"),
                alt.Y("migrants_arrived:Q").aggregate("sum").title("Migrants Arrived"),
                alt.Color("date_ending:N", timeUnit="year").title("Year"),
            )
            .configure_legend(orient="bottom")
        )
with tab2:
    st.dataframe(df2)

st.html(daily_source_text)

"""
Made with :heart: by adam-dot-py
"""

con.close()