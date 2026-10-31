
import os

import pandas as pd
import pyarrow.dataset as ds
import streamlit as st
from pyarrow import fs


# --------------------------------------------------
# Configuration
# --------------------------------------------------

BUCKET = os.getenv("RUSTFS_BUCKET", "chicago-taxi-lake")

GOLD_TABLES = {
    "Daily Revenue": "gold/daily_revenue",
    "Daily Trip Metrics": "gold/daily_trip_metrics",
    "Pickup Zone Performance": "gold/pickup_zone_performance",
}

QUALITY_PATH = "quality/silver/monthly_report"


# --------------------------------------------------
# RustFS connection
# --------------------------------------------------

@st.cache_resource
def get_rustfs():
    return fs.S3FileSystem(
        endpoint_override=os.getenv(
            "RUSTFS_ENDPOINT_URL", "http://rustfs:9000"
        ),
        access_key=os.environ["RUSTFS_ACCESS_KEY"],
        secret_key=os.environ["RUSTFS_SECRET_KEY"],
        scheme="http",
        region="us-east-1",
    )


# --------------------------------------------------
# Data loading
# --------------------------------------------------

@st.cache_data(ttl=300)
def load_parquet(prefix):
    filesystem = get_rustfs()
    path = f"{BUCKET}/{prefix}"

    dataset = ds.dataset(
        path,
        filesystem=filesystem,
        format="parquet",
    )

    return dataset.to_table().to_pandas()


@st.cache_data(ttl=300)
def load_gold_tables():
    return {
        name: load_parquet(path)
        for name, path in GOLD_TABLES.items()
    }


@st.cache_data(ttl=300)
def load_quality_report():
    return load_parquet(QUALITY_PATH)


# --------------------------------------------------
# Dashboard
# --------------------------------------------------

st.set_page_config(
    page_title="Chicago Taxi Analytics",
    page_icon="🚕",
    layout="wide",
)

st.title("Chicago Taxi Trips Analytics")
st.caption("Chicago taxi trip data | September – December 2023")

try:
    tables = load_gold_tables()
    quality = load_quality_report()

except Exception as exc:
    st.error(f"Could not load data from RustFS: {exc}")
    st.stop()


revenue = tables["Daily Revenue"].copy()
metrics = tables["Daily Trip Metrics"].copy()
zones = tables["Pickup Zone Performance"].copy()

# Normalize date columns
for dataframe in (revenue, metrics):
    dataframe["trip_date"] = pd.to_datetime(dataframe["trip_date"])

# Month filter
months = sorted(revenue["month"].dropna().unique())
month_labels = {
    month: pd.Timestamp(f"2023-{int(month):02d}-01").strftime("%B")
    for month in months
}

selected_months = st.multiselect(
    "Filter by month",
    options=months,
    default=months,
    format_func=lambda month: month_labels[month],
)

revenue = revenue[revenue["month"].isin(selected_months)]
metrics = metrics[metrics["month"].isin(selected_months)]
zones = zones[zones["month"].isin(selected_months)]
quality = quality[quality["month"].isin(selected_months)]


# --------------------------------------------------
# KPI cards
# --------------------------------------------------

total_trips = int(revenue["total_trips"].sum())
total_revenue = revenue["total_revenue"].sum()
revenue_eligible = revenue["revenue_eligible_trips"].sum()

average_revenue = (
    total_revenue / revenue_eligible
    if revenue_eligible
    else 0
)

col1, col2, col3 = st.columns(3)

col1.metric("Total trips", f"{total_trips:,.0f}")
col2.metric("Total revenue", f"${total_revenue:,.2f}")
col3.metric("Average revenue / trip", f"${average_revenue:,.2f}")


# --------------------------------------------------
# Daily revenue
# --------------------------------------------------

st.divider()
st.subheader("Daily Revenue")

if not revenue.empty:
    daily_revenue = revenue.sort_values("trip_date").set_index("trip_date")
    st.line_chart(daily_revenue["total_revenue"])
else:
    st.info("No revenue data for the selected months.")


# --------------------------------------------------
# Daily trip metrics
# --------------------------------------------------

st.divider()
st.subheader("Daily Trip Metrics")

if not metrics.empty:
    daily_metrics = metrics.sort_values("trip_date").set_index("trip_date")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Daily trip volume**")
        st.bar_chart(daily_metrics["valid_duration_trips"])

    with col2:
        st.markdown("**Average trip duration (minutes)**")
        st.line_chart(
            daily_metrics["average_trip_duration_minutes"]
        )

    st.markdown("**Average trip distance (miles)**")
    st.line_chart(daily_metrics["average_trip_distance_miles"])
else:
    st.info("No trip metrics for the selected months.")


# --------------------------------------------------
# Pickup zone performance
# --------------------------------------------------

st.divider()
st.subheader("Pickup Zone Performance")

if not zones.empty:
    selected_zone_month = st.selectbox(
        "Select month for pickup zone ranking",
        options=selected_months,
        format_func=lambda month: month_labels[month],
    )

    monthly_zones = zones[
        zones["month"] == selected_zone_month
    ].sort_values("monthly_rank")

    st.bar_chart(
        monthly_zones.set_index("pickup_community_area")["total_trips"]
    )

    st.dataframe(
        monthly_zones,
        use_container_width=True,
        hide_index=True,
    )
else:
    st.info("No pickup zone data for the selected months.")


# --------------------------------------------------
# Data quality
# --------------------------------------------------

st.divider()
st.subheader("Silver Data Quality")

if not quality.empty:
    st.dataframe(
        quality.sort_values(["year", "month"]),
        use_container_width=True,
        hide_index=True,
    )
else:
    st.info("No quality report for the selected months.")

st.caption(
    "Source: City of Chicago Taxi Trips dataset. "
    "Analytics are computed in the Gold layer and read from RustFS."
)
