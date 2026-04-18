import os
import streamlit as st
import pandas as pd
import pydeck as pdk
import plotly.express as px
import requests
from google.cloud import bigquery
from datetime import datetime
import pytz

MTR_LINES_CSV      = "https://opendata.mtr.com.hk/data/mtr_lines_and_stations.csv"
MTR_BUS_STOPS_CSV  = "https://opendata.mtr.com.hk/data/mtr_bus_stops.csv"
MTR_FARES_CSV      = "https://opendata.mtr.com.hk/data/mtr_lines_fares.csv"
MTR_LR_CSV         = "https://opendata.mtr.com.hk/data/light_rail_routes_and_stops.csv"
MTR_SCHEDULE_API   = "https://rt.data.gov.hk/v1/transport/mtr/getSchedule.php"

_BROWSER_UA = {"User-Agent": "Mozilla/5.0 (compatible; hk-transit-pulse/1.0)"}

@st.cache_data(ttl=3600)
def load_csv_url(url):
    import io
    resp = requests.get(url, headers=_BROWSER_UA, timeout=30)
    resp.raise_for_status()
    return pd.read_csv(io.StringIO(resp.content.decode("utf-8-sig")))

@st.cache_data(ttl=30)
def get_mtr_schedule(line, station, _refresh=0):
    try:
        resp = requests.get(MTR_SCHEDULE_API, params={"line": line, "sta": station}, timeout=10)
        return resp.json()
    except Exception as e:
        return {"status": 0, "message": str(e)}

PROJECT_ID = os.environ["GOOGLE_CLOUD_PROJECT"]

st.set_page_config(
    page_title="🇭🇰 香港交通脈搏 Hong Kong Transit Pulse",
    page_icon="🚌",
    layout="wide"
)


# ── HK Theme ──────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    /* HK red accent */
    h1, h2, h3 { color: #C8102E; }
    .stMetric label { color: #C8102E; font-weight: bold; }
    .stTabs [data-baseweb="tab-list"] { gap: 8px; }
    .stTabs [data-baseweb="tab"] {
        background-color: #f5f5f5;
        border-radius: 4px 4px 0 0;
        padding: 8px 20px;
        font-weight: bold;
        color: #333;
    }
    .stTabs [aria-selected="true"] {
        background-color: #C8102E !important;
        color: white !important;
    }
    .stInfo { border-left: 4px solid #C8102E; }
</style>
""", unsafe_allow_html=True)

import base64 as _b64

@st.cache_resource
def get_flag_b64():
    with open("dashboard/hk_flag.png", "rb") as _f:
        return _b64.b64encode(_f.read()).decode()

_flag_b64 = get_flag_b64()
st.markdown(f"""
<div style='display:flex; align-items:center; gap:12px; margin-bottom:0;'>
    <img src='data:image/png;base64,{_flag_b64}' height='44'/>
    <span style='font-size:2.2rem; font-weight:700; line-height:1.2;'>香港交通脈搏 Hong Kong Transit Pulse</span>
</div>
""", unsafe_allow_html=True)
st.markdown("Hong Kong public transport network — routes, stops, and peak hours.")

@st.cache_resource
def get_bq_client():
    return bigquery.Client(project=PROJECT_ID)

@st.cache_data(ttl=3600)
def load_data(query):
    return get_bq_client().query(query).to_dataframe()

@st.cache_data(ttl=3600)
def load_peak_by_type(project_id, route_type=None):
    rt_sql = f"AND route_type = {route_type}" if route_type is not None else ""
    return get_bq_client().query(f"""
    SELECT CAST(SUBSTR(st.departure_time, 1, 2) AS INT64) AS hour_of_day,
           COUNT(*) AS total_trips
    FROM `{project_id}.staging.stg_stop_times` st
    JOIN `{project_id}.staging.stg_trips` t ON st.trip_id = t.trip_id
    JOIN `{project_id}.staging.stg_routes` r ON t.route_id = r.route_id
    WHERE REGEXP_CONTAINS(st.departure_time, r'^\\d+:\\d{{2}}:\\d{{2}}$')
      AND CAST(SUBSTR(st.departure_time, 1, 2) AS INT64) BETWEEN 0 AND 23
      {rt_sql}
    GROUP BY hour_of_day ORDER BY hour_of_day
    """).to_dataframe()

ROUTE_TYPE_LABEL = {0: "Tram", 1: "MTR", 3: "Bus", 4: "Ferry"}
COLOR_MAP = {"Bus": "#ff3232", "Ferry": "#0078ff", "Tram": "#00c864", "MTR": "#b400ff"}

def format_gtfs_time(t):
    """Convert GTFS time (e.g. 25:30:00) to readable format (e.g. 01:30 +1day)."""
    if not t or ":" not in str(t):
        return t
    parts = str(t).split(":")
    h, m = int(parts[0]), int(parts[1])
    if h >= 24:
        return f"{h-24:02d}:{m:02d} (+1day)"
    return f"{h:02d}:{m:02d}"


hk_tz = pytz.timezone("Asia/Hong_Kong")


visible_types = [0, 3, 4, 7]


# ── Load data ──────────────────────────────────────────────────────────────────
stops_query = f"""
SELECT stop_id, stop_name, latitude, longitude, total_departures,
       COALESCE(route_type, 3) AS route_type
FROM `{PROJECT_ID}.marts.mart_stops_ranked`
WHERE latitude IS NOT NULL AND longitude IS NOT NULL
"""
stops_df = load_data(stops_query)

def route_type_color(rt):
    return {
        0: [0, 200, 100, 220],
        1: [180, 0, 255, 220],
        3: [255, 50, 50, 220],
        4: [0, 120, 255, 220],
    }.get(rt, [200, 200, 200, 180])

stops_df["color"] = stops_df["route_type"].apply(route_type_color)
stops_df["Transport Type"] = stops_df["route_type"].map(ROUTE_TYPE_LABEL).fillna("Unknown")

trips_query = f"""
SELECT r.route_short_name, r.route_long_name, r.route_type, COUNT(t.trip_id) AS total_trips
FROM `{PROJECT_ID}.staging.stg_trips` t
JOIN `{PROJECT_ID}.staging.stg_routes` r ON t.route_id = r.route_id
GROUP BY r.route_short_name, r.route_long_name, r.route_type
ORDER BY total_trips DESC
LIMIT 20
"""
trips_df = load_data(trips_query)
trips_df["Route Type"] = trips_df["route_type"].map(ROUTE_TYPE_LABEL).fillna("Unknown")

route_count_query = f"""
SELECT COUNT(DISTINCT route_short_name) AS total_routes
FROM `{PROJECT_ID}.staging.stg_routes`
"""
route_count_df = load_data(route_count_query)

peak_query = f"""
SELECT hour_of_day, total_trips
FROM `{PROJECT_ID}.marts.peak_hour_analysis`
WHERE hour_of_day BETWEEN 0 AND 23
ORDER BY hour_of_day
"""
peak_df = load_data(peak_query)



# ── Auto-generated summary card ────────────────────────────────────────────────
busiest_route = trips_df.iloc[0]["route_short_name"] if not trips_df.empty else "N/A"
busiest_route_trips = int(trips_df.iloc[0]["total_trips"]) if not trips_df.empty else 0
peak_hour = int(peak_df.loc[peak_df["total_trips"].idxmax(), "hour_of_day"])
top_stop = stops_df.sort_values("total_departures", ascending=False).iloc[0]["stop_name"]
total_stops = len(stops_df)
total_routes = int(route_count_df.iloc[0]["total_routes"]) if not route_count_df.empty else trips_df["route_short_name"].nunique()

st.info(
    f"**Network Snapshot:** HK public transport has **{total_stops:,} stops** across **{total_routes} routes**. "
    f"Route **{busiest_route}** is the busiest with **{busiest_route_trips:,} trips**. "
    f"The network peaks at **{peak_hour:02d}:00** — morning rush hour. "
    f"The highest-traffic stop is **{top_stop}**."
)


# ── KPI row ────────────────────────────────────────────────────────────────────
mtr_net_kpi = load_csv_url(MTR_LINES_CSV)
mtr_net_kpi.columns = [c.strip() for c in mtr_net_kpi.columns]
mtr_lines_count = mtr_net_kpi["Line Code"].nunique() if "Line Code" in mtr_net_kpi.columns else 10
mtr_stations_count = mtr_net_kpi["Station Code"].nunique() if "Station Code" in mtr_net_kpi.columns else 98

col1, col2, col3, col4, col5, col6 = st.columns(6)
col1.metric("GTFS Stops", f"{total_stops:,}")
col2.metric("GTFS Routes", f"{total_routes:,}")
col3.metric("Total Departures", f"{stops_df['total_departures'].sum():,.0f}")
col4.metric("Peak Hour", f"{peak_hour:02d}:00")
col5.metric("MTR Lines", mtr_lines_count)
col6.metric("MTR Stations", mtr_stations_count)

st.divider()

# ── Tabs ───────────────────────────────────────────────────────────────────────
tab_network, tab_mtr, tab_streaming, tab_about = st.tabs(["Network Analytics", "MTR Live 港鐵", "Streaming Analytics 實時分析", "About"])


# ══════════════════════════════════════════════════════════════════════════════
# TAB 1: Network Analytics
# ══════════════════════════════════════════════════════════════════════════════
with tab_network:

    # ── Transport type selector ────────────────────────────────────────────────
    TYPE_OPTIONS = {
        "🚦 All":        None,
        "🚌 Bus":        3,
        "🚃 Tram":       0,
        "⛴️ Ferry":      4,
        "🚡 Peak Tram":  7,
    }
    selected_label = st.radio(
        "Transport Type", list(TYPE_OPTIONS.keys()),
        horizontal=True, label_visibility="collapsed",
    )
    rt_filter = TYPE_OPTIONS[selected_label]

    st.info(
        "**Why is MTR not in these charts?** "
        "All analytics on this tab are derived from the **GTFS open data feed** published by the Hong Kong government "
        "(`data.gov.hk/td/pt-headway-en/gtfs.zip`), which covers **Bus, Tram, Ferry and Peak Tram** only. "
        "MTR (heavy rail and Light Rail) publishes its data separately at `opendata.mtr.com.hk` in a non-GTFS format — "
        "it cannot be directly merged into this pipeline without a custom transformation layer. "
        "MTR network statistics are shown separately in the **MTR Rail Network** section at the bottom of this tab."
    )

    # Filtered dataframes (used throughout this tab)
    f_stops = stops_df if rt_filter is None else stops_df[stops_df["route_type"] == rt_filter]
    f_trips = trips_df if rt_filter is None else trips_df[trips_df["route_type"] == rt_filter]
    rt_sql  = "" if rt_filter is None else f"AND route_type = {rt_filter}"
    rt_sql_where = "" if rt_filter is None else f"WHERE route_type = {rt_filter}"

    st.divider()

    # ── Stop map ───────────────────────────────────────────────────────────────
    st.subheader("Stop Locations / 站點位置")
    st.caption(f"Showing: **{selected_label}** stops from GTFS open data.")
    try:
        st.pydeck_chart(pdk.Deck(
            layers=[pdk.Layer(
                "ScatterplotLayer", data=f_stops,
                get_position=["longitude", "latitude"], get_radius=120,
                get_fill_color="color", pickable=True, auto_highlight=True,
            )],
            initial_view_state=pdk.ViewState(latitude=22.35, longitude=114.15, zoom=10, pitch=40),
            tooltip={"text": "{stop_name}\nDepartures: {total_departures}\nType: {Transport Type}"},
            map_style="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
        ))
    except Exception:
        st.info("Map unavailable — refresh the page to reload.")

    st.divider()

    # ── Busiest stops ──────────────────────────────────────────────────────────
    st.subheader("Top 10 Busiest Stops / 最繁忙十大站點")
    st.caption("Ranked by total scheduled departures.")
    top_stops = (
        f_stops[["stop_name", "total_departures", "Transport Type"]]
        .sort_values("total_departures", ascending=False).head(10).reset_index(drop=True)
    )
    top_stops.index += 1
    top_stops.columns = ["Stop Name", "Total Departures", "Transport Type"]
    st.dataframe(top_stops, use_container_width=True)
    st.download_button("⬇ Download CSV", data=top_stops.to_csv(index=False),
                       file_name="hk_busiest_stops.csv", mime="text/csv")

    st.divider()

    # ── Charts ─────────────────────────────────────────────────────────────────
    col_left, col_right = st.columns(2)
    with col_left:
        st.subheader("Top 20 Routes by Trips / 最多班次二十條路線")
        st.caption("Each bar = one route. Taller = more scheduled trips.")
        fig_trips = px.bar(
            f_trips.head(20), x="route_short_name", y="total_trips", color="Route Type",
            color_discrete_map=COLOR_MAP,
            labels={"route_short_name": "Route", "total_trips": "Total Trips"},
            hover_data=["route_long_name"],
        )
        fig_trips.update_layout(legend_title="Transport Type")
        st.plotly_chart(fig_trips, use_container_width=True)

    with col_right:
        st.subheader("Departures by Hour of Day / 每小時出發班次")
        st.caption("Total departures per hour. Orange bands = AM peak (7-9) and PM peak (17-19).")
        peak_filtered = load_peak_by_type(PROJECT_ID, rt_filter) if rt_filter is not None else peak_df
        fig_peak = px.line(
            peak_filtered, x="hour_of_day", y="total_trips",
            labels={"hour_of_day": "Hour of Day", "total_trips": "Total Departures"},
            markers=True, color_discrete_sequence=["#0078ff"],
        )
        fig_peak.add_vrect(x0=7, x1=9, fillcolor="orange", opacity=0.15, annotation_text="AM Peak")
        fig_peak.add_vrect(x0=17, x1=19, fillcolor="orange", opacity=0.15, annotation_text="PM Peak")
        fig_peak.update_layout(showlegend=False)
        st.plotly_chart(fig_peak, use_container_width=True)

    st.divider()

    # ── Route type pie (only when showing All) ─────────────────────────────────
    if rt_filter is None:
        st.subheader("Routes by Transport Type / 按交通類型分類路線")
        st.caption("Share of total trips by mode.")
        type_summary = trips_df.groupby("Route Type")["total_trips"].sum().reset_index()
        type_summary.columns = ["Transport Type", "Total Trips"]
        fig_pie = px.pie(type_summary, names="Transport Type", values="Total Trips",
                         color="Transport Type", color_discrete_map=COLOR_MAP, hole=0.4)
        fig_pie.update_traces(textposition="inside", textinfo="percent+label")
        st.plotly_chart(fig_pie, use_container_width=True)
        st.divider()

    # ── Service hours ──────────────────────────────────────────────────────────
    st.subheader("First & Last Service per Route / 各路線首末班車 (Top 20)")
    st.caption("Times past 24:00 mean next-day service (e.g. 00:15 +1day = 12:15 AM).")
    service_hours_df = load_data(f"""
    SELECT route_short_name, route_long_name, route_type,
           first_departure, last_departure, total_trips, total_stops
    FROM `{PROJECT_ID}.marts.mart_route_service_hours`
    {rt_sql_where} ORDER BY total_trips DESC LIMIT 20
    """)
    service_hours_df["Route Type"] = service_hours_df["route_type"].map(ROUTE_TYPE_LABEL).fillna("Unknown")
    service_hours_df["first_departure"] = service_hours_df["first_departure"].apply(format_gtfs_time)
    service_hours_df["last_departure"]  = service_hours_df["last_departure"].apply(format_gtfs_time)
    st.dataframe(
        service_hours_df[["route_short_name", "first_departure", "last_departure", "total_trips", "total_stops", "Route Type"]]
        .rename(columns={"route_short_name": "Route", "first_departure": "First Service",
                         "last_departure": "Last Service", "total_trips": "Total Trips", "total_stops": "Total Stops"}),
        use_container_width=True,
    )

    st.divider()

    # ── Weekday vs weekend + frequency heatmap ─────────────────────────────────
    st.subheader("Weekday vs Weekend Service / 平日對週末服務")
    st.caption("Left: trips by schedule type. Right: service frequency heatmap for top 10 routes.")
    wkd_df = load_data(f"SELECT service_type, total_trips FROM `{PROJECT_ID}.marts.mart_weekday_vs_weekend`")
    col_wkd, col_freq = st.columns(2)
    with col_wkd:
        fig_wkd = px.bar(wkd_df, x="service_type", y="total_trips", color="service_type",
                         labels={"service_type": "Service Type", "total_trips": "Total Trips"},
                         color_discrete_sequence=["#ff3232", "#0078ff", "#00c864"])
        fig_wkd.update_layout(showlegend=False)
        st.plotly_chart(fig_wkd, use_container_width=True)
    with col_freq:
        freq_df = load_data(f"""
        SELECT route_short_name, hour_of_day, SUM(trips_per_hour) AS trips_per_hour
        FROM `{PROJECT_ID}.marts.mart_service_frequency`
        WHERE route_short_name IN (
            SELECT route_short_name FROM `{PROJECT_ID}.marts.mart_route_service_hours`
            {rt_sql_where} ORDER BY total_trips DESC LIMIT 10
        )
        GROUP BY route_short_name, hour_of_day ORDER BY route_short_name, hour_of_day
        """)
        freq_pivot = freq_df.pivot(index="route_short_name", columns="hour_of_day", values="trips_per_hour").fillna(0)
        fig_freq = px.imshow(freq_pivot, labels={"x": "Hour of Day", "y": "Route", "color": "Trips"},
                             color_continuous_scale="Reds", title="Service Frequency Heatmap (Top 10 Routes)")
        st.plotly_chart(fig_freq, use_container_width=True)

    st.divider()

    # ── Transfer hubs ──────────────────────────────────────────────────────────
    st.subheader("Transfer Hubs / 轉乘樞紐")
    st.markdown("Stops served by 3+ routes — key interchange points in the network.")
    st.caption("Larger circle = more routes. These are the most important interchange stops in HK.")
    hubs_df = load_data(f"""
    SELECT stop_id, stop_name, latitude, longitude, route_count, transport_modes, routes_serving
    FROM `{PROJECT_ID}.marts.mart_transfer_hubs` ORDER BY route_count DESC
    """)
    col_hub_map, col_hub_table = st.columns([2, 1])
    with col_hub_map:
        try:
            st.pydeck_chart(pdk.Deck(
                layers=[pdk.Layer("ScatterplotLayer", data=hubs_df,
                                  get_position=["longitude", "latitude"], get_radius="route_count * 15",
                                  get_fill_color=[255, 140, 0, 120], pickable=True, auto_highlight=True)],
                initial_view_state=pdk.ViewState(latitude=22.35, longitude=114.15, zoom=10, pitch=30),
                tooltip={"text": "{stop_name}\nRoutes: {route_count}\nModes: {transport_modes}\n{routes_serving}"},
                map_style="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
            ))
        except Exception:
            st.info("Map unavailable — refresh the page to reload.")
    with col_hub_table:
        st.dataframe(
            hubs_df[["stop_name", "route_count", "transport_modes", "routes_serving"]].head(15)
            .rename(columns={"stop_name": "Stop", "route_count": "Routes",
                             "transport_modes": "Modes", "routes_serving": "Serving"}),
            use_container_width=True, height=400,
        )

    st.divider()

    # ── Longest routes ─────────────────────────────────────────────────────────
    st.subheader("Longest Routes by Stop Count / 最長路線（按站數）")
    st.caption("Routes ranked by number of unique stops served.")
    longest_df = load_data(f"""
    SELECT route_short_name, route_long_name, route_type, unique_stops, total_trips, max_stop_sequence
    FROM `{PROJECT_ID}.marts.mart_longest_routes`
    {rt_sql_where} ORDER BY unique_stops DESC LIMIT 15
    """)
    longest_df["Route Type"] = longest_df["route_type"].map(ROUTE_TYPE_LABEL).fillna("Unknown")
    fig_longest = px.bar(longest_df, x="route_short_name", y="unique_stops", color="Route Type",
                         color_discrete_map=COLOR_MAP,
                         labels={"route_short_name": "Route", "unique_stops": "Unique Stops"},
                         hover_data=["route_long_name", "total_trips"])
    st.plotly_chart(fig_longest, use_container_width=True)

    st.divider()

    # ── Early bird & night owl ─────────────────────────────────────────────────
    st.subheader("Early Bird & Night Owl Routes / 早班及夜班路線")
    st.caption("Early bird = first departure before 06:00. Night owl = last departure after 23:00.")
    early_night_df = load_data(f"""
    SELECT route_short_name, route_long_name, route_type,
           first_departure, last_departure, is_early_bird, is_night_owl
    FROM `{PROJECT_ID}.marts.mart_early_night_routes`
    {rt_sql_where} ORDER BY first_departure ASC
    """)
    early_night_df["Route Type"] = early_night_df["route_type"].map(ROUTE_TYPE_LABEL).fillna("Unknown")
    early_night_df["first_departure"] = early_night_df["first_departure"].apply(format_gtfs_time)
    early_night_df["last_departure"]  = early_night_df["last_departure"].apply(format_gtfs_time)
    col_early, col_night = st.columns(2)
    with col_early:
        st.markdown("**🌅 Early Bird Routes** (start before 06:00)")
        st.dataframe(
            early_night_df[early_night_df["is_early_bird"] == True][["route_short_name", "first_departure", "Route Type"]]
            .rename(columns={"route_short_name": "Route", "first_departure": "First Service"}),
            use_container_width=True, height=300,
        )
    with col_night:
        st.markdown("**🌙 Night Owl Routes** (run past 23:00)")
        st.dataframe(
            early_night_df[early_night_df["is_night_owl"] == True][["route_short_name", "last_departure", "Route Type"]]
            .rename(columns={"route_short_name": "Route", "last_departure": "Last Service"}),
            use_container_width=True, height=300,
        )

    st.divider()

    # ── Underserved areas ──────────────────────────────────────────────────────
    st.subheader("Underserved Areas")
    st.caption("Stops with below-median departures — identifies gaps in coverage.")
    avg_dep = f_stops["total_departures"].median()
    well_served_stops = f_stops[f_stops["total_departures"] >= avg_dep]
    underserved_stops  = f_stops[f_stops["total_departures"] < avg_dep]
    col_us1, col_us2, col_us3 = st.columns(3)
    col_us1.metric("Total Stops", len(f_stops))
    col_us2.metric("Well Served", len(well_served_stops))
    col_us3.metric("Low Service", len(underserved_stops))
    try:
        st.pydeck_chart(pdk.Deck(
            layers=[pdk.Layer("HeatmapLayer", data=underserved_stops,
                              get_position=["longitude", "latitude"], get_weight="total_departures",
                              radiusPixels=40, opacity=0.8,
                              color_range=[[255,255,178],[254,217,118],[254,178,76],[253,141,60],[240,59,32],[189,0,38]])],
            initial_view_state=pdk.ViewState(latitude=22.35, longitude=114.15, zoom=10, pitch=0),
            map_style="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
        ))
    except Exception:
        st.info("Map unavailable — refresh the page to reload.")

    st.divider()

    # ── Zone Coverage ──────────────────────────────────────────────────────────
    st.subheader("Zone Coverage")
    st.caption("Stop density across HK — red = dense, yellow = sparse.")
    z_df = f_stops.copy()
    z_df["lat_bin"] = (z_df["latitude"] / 0.02).astype(int) * 0.02 + 0.01
    z_df["lon_bin"] = (z_df["longitude"] / 0.02).astype(int) * 0.02 + 0.01
    z_summary = z_df.groupby(["lat_bin", "lon_bin"]).agg(
        stop_count=("stop_id", "count"), total_departures=("total_departures", "sum"),
    ).reset_index()
    z_summary.columns = ["latitude", "longitude", "stop_count", "total_departures"]
    col_zc1, col_zc2, col_zc3 = st.columns(3)
    col_zc1.metric("Total Zones", len(z_summary))
    col_zc2.metric("Well Served (3+ stops)", len(z_summary[z_summary["stop_count"] >= 3]))
    col_zc3.metric("Underserved (<3 stops)", len(z_summary[z_summary["stop_count"] < 3]))
    try:
        st.pydeck_chart(pdk.Deck(
            layers=[pdk.Layer("HeatmapLayer", data=z_summary,
                              get_position=["longitude", "latitude"], get_weight="stop_count",
                              radiusPixels=40, opacity=0.7,
                              color_range=[[255,255,178],[254,217,118],[254,178,76],[253,141,60],[240,59,32],[189,0,38]])],
            initial_view_state=pdk.ViewState(latitude=22.35, longitude=114.15, zoom=10, pitch=0),
            map_style="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
        ))
    except Exception:
        st.info("Map unavailable — refresh the page to reload.")

    st.divider()

    # ── Route comparison ───────────────────────────────────────────────────────
    st.subheader("Route Comparison")
    st.markdown("Compare two routes side by side.")
    all_routes = f_trips["route_short_name"].dropna().str.strip().replace("", None).dropna().sort_values().unique().tolist()
    col_r1, col_r2 = st.columns(2)
    with col_r1:
        route_a = st.selectbox("Route A", ["— Select —"] + all_routes, key="route_a")
    with col_r2:
        route_b = st.selectbox("Route B", ["— Select —"] + all_routes, key="route_b")

    if route_a != "— Select —" and route_b != "— Select —":
        def get_route_info(route_name):
            return load_data(f"""
            SELECT route_short_name, route_long_name, route_type,
                   first_departure, last_departure, total_trips, total_stops
            FROM `{PROJECT_ID}.marts.mart_route_service_hours`
            WHERE route_short_name = '{route_name}' LIMIT 1
            """)
        ra, rb = get_route_info(route_a), get_route_info(route_b)
        if not ra.empty and not rb.empty:
            col_ca, col_cb = st.columns(2)
            for col, df, name in [(col_ca, ra, route_a), (col_cb, rb, route_b)]:
                with col:
                    st.markdown(f"**Route {name}** — {df.iloc[0]['route_long_name']}")
                    st.metric("Total Trips", int(df.iloc[0]["total_trips"]))
                    st.metric("Total Stops", int(df.iloc[0]["total_stops"]))
                    st.metric("First Service", df.iloc[0]["first_departure"])
                    st.metric("Last Service", df.iloc[0]["last_departure"])
                    st.metric("Type", ROUTE_TYPE_LABEL.get(int(df.iloc[0]["route_type"]), "Unknown"))

    st.divider()

    # ── MTR Rail Network ───────────────────────────────────────────────────────
    st.subheader("MTR Rail Network 港鐵路綫")

    MTR_LINE_FULL = {
        "AEL": "Airport Express", "TCL": "Tung Chung Line", "TML": "Tuen Ma Line",
        "KTL": "Kwun Tong Line", "TWL": "Tsuen Wan Line", "ISL": "Island Line",
        "EAL": "East Rail Line", "SIL": "South Island Line", "TKL": "Tseung Kwan O Line",
        "DRL": "Disneyland Resort Line",
    }

    # ── Heavy Rail ─────────────────────────────────────────────────────────────
    st.markdown("#### Heavy Rail 重鐵")
    st.caption("10 lines serving the core MTR network.")

    mtr_net_df = load_csv_url(MTR_LINES_CSV)
    mtr_net_df.columns = [c.strip() for c in mtr_net_df.columns]
    mtr_net_df = mtr_net_df.dropna(subset=["Line Code", "Station Code"])
    mtr_net_df["Line Code"] = mtr_net_df["Line Code"].astype(str).str.strip()
    mtr_net_df["Line Name"] = mtr_net_df["Line Code"].map(MTR_LINE_FULL).fillna(mtr_net_df["Line Code"])

    stations_per_line = (
        mtr_net_df.groupby(["Line Code", "Line Name"])["Station Code"]
        .nunique().reset_index()
        .rename(columns={"Station Code": "Stations"})
        .sort_values("Stations", ascending=False)
    )

    hr_kpi1, hr_kpi2, hr_kpi3 = st.columns(3)
    hr_kpi1.metric("Heavy Rail Lines", mtr_net_df["Line Code"].nunique())
    hr_kpi2.metric("Heavy Rail Stations", mtr_net_df["Station Code"].nunique())
    hr_kpi3.metric("Largest Line", stations_per_line.iloc[0]["Line Name"] if not stations_per_line.empty else "N/A")

    col_hr_bar, col_hr_table = st.columns([2, 1])
    with col_hr_bar:
        fig_hr = px.bar(
            stations_per_line, x="Line Name", y="Stations", color="Line Name",
            labels={"Line Name": "MTR Line", "Stations": "Stations"},
            color_discrete_sequence=["#b400ff"] * len(stations_per_line),
        )
        fig_hr.update_layout(showlegend=False, xaxis_tickangle=-30)
        st.plotly_chart(fig_hr, use_container_width=True)
    with col_hr_table:
        st.dataframe(
            stations_per_line[["Line Name", "Line Code", "Stations"]].reset_index(drop=True),
            use_container_width=True, height=320,
        )

    # Fare summary
    try:
        mtr_fares_raw = load_csv_url(MTR_FARES_CSV)
        mtr_fares_raw.columns = [c.strip() for c in mtr_fares_raw.columns]
        if "OCT_ADT_FARE" in mtr_fares_raw.columns and "LINE" in mtr_fares_raw.columns:
            fare_summary = (
                mtr_fares_raw.groupby("LINE")["OCT_ADT_FARE"]
                .mean().reset_index()
                .rename(columns={"LINE": "Line Code", "OCT_ADT_FARE": "Avg Octopus Fare (HK$)"})
            )
            fare_summary["Line Name"] = fare_summary["Line Code"].map(MTR_LINE_FULL).fillna(fare_summary["Line Code"])
            fare_summary["Avg Octopus Fare (HK$)"] = fare_summary["Avg Octopus Fare (HK$)"].round(2)
            fare_summary = fare_summary.sort_values("Avg Octopus Fare (HK$)", ascending=False)
            st.markdown("**Average Octopus Card Fare by Line**")
            st.caption("Mean adult Octopus fare across all origin-destination pairs on each line.")
            fig_fare = px.bar(
                fare_summary, x="Line Name", y="Avg Octopus Fare (HK$)", color="Line Name",
                color_discrete_sequence=["#b400ff"] * len(fare_summary),
            )
            fig_fare.update_layout(showlegend=False, xaxis_tickangle=-30)
            st.plotly_chart(fig_fare, use_container_width=True)
    except Exception:
        pass

    st.divider()

    # ── Light Rail ─────────────────────────────────────────────────────────────
    st.markdown("#### Light Rail 輕鐵")
    st.caption("12 routes serving Tuen Mun and Yuen Long districts in the New Territories.")

    LR_URL = "https://opendata.mtr.com.hk/data/light_rail_routes_and_stops.csv"
    try:
        lr_raw = load_csv_url(LR_URL)
        lr_raw.columns = [c.strip() for c in lr_raw.columns]

        # Detect column names (MTR uses different conventions across CSVs)
        route_col   = next((c for c in lr_raw.columns if "ROUTE" in c.upper() and "NAME" not in c.upper()), None)
        name_col    = next((c for c in lr_raw.columns if "NAMEE" in c.upper() or ("NAME" in c.upper() and "ENG" in c.upper())), None)
        station_col = next((c for c in lr_raw.columns if "STATION" in c.upper() and "NAME" in c.upper() and ("ENG" in c.upper() or "NAMEE" in c.upper())), None)
        lat_col     = next((c for c in lr_raw.columns if "LAT" in c.upper()), None)
        lon_col     = next((c for c in lr_raw.columns if "LON" in c.upper() or "LONG" in c.upper()), None)
        stop_id_col = next((c for c in lr_raw.columns if "STATION_ID" in c.upper() or "STOP_ID" in c.upper()), None)

        lr_kpi1, lr_kpi2, lr_kpi3 = st.columns(3)
        if route_col:
            lr_routes = lr_raw[route_col].dropna().astype(str).str.strip()
            lr_kpi1.metric("Light Rail Routes", lr_routes.nunique())
        if stop_id_col:
            lr_kpi2.metric("Light Rail Stops", lr_raw[stop_id_col].nunique())
        elif lat_col:
            lr_kpi2.metric("Light Rail Stop Records", len(lr_raw))
        lr_kpi3.metric("District", "Tuen Mun / Yuen Long")

        if route_col and stop_id_col:
            lr_stops_per_route = (
                lr_raw.groupby(route_col)[stop_id_col].nunique()
                .reset_index().rename(columns={route_col: "Route", stop_id_col: "Stops"})
                .sort_values("Stops", ascending=False)
            )
            col_lr_bar, col_lr_table = st.columns([2, 1])
            with col_lr_bar:
                fig_lr = px.bar(
                    lr_stops_per_route, x="Route", y="Stops", color="Route",
                    labels={"Route": "Light Rail Route", "Stops": "Stops"},
                    color_discrete_sequence=["#ff8c00"] * len(lr_stops_per_route),
                )
                fig_lr.update_layout(showlegend=False)
                st.plotly_chart(fig_lr, use_container_width=True)
            with col_lr_table:
                st.dataframe(lr_stops_per_route.reset_index(drop=True), use_container_width=True, height=320)
        else:
            st.dataframe(lr_raw.head(30), use_container_width=True)

        # Light Rail stop map (if coordinates present)
        if lat_col and lon_col and stop_id_col:
            lr_map_df = lr_raw[[stop_id_col, lat_col, lon_col]].copy()
            if station_col:
                lr_map_df["name"] = lr_raw[station_col]
            lr_map_df = lr_map_df.rename(columns={lat_col: "latitude", lon_col: "longitude"})
            lr_map_df = lr_map_df.dropna(subset=["latitude", "longitude"])
            lr_map_df = lr_map_df.drop_duplicates(subset=[stop_id_col])
            if not lr_map_df.empty:
                st.markdown("**Light Rail Stop Locations**")
                try:
                    st.pydeck_chart(pdk.Deck(
                        layers=[pdk.Layer(
                            "ScatterplotLayer", data=lr_map_df,
                            get_position=["longitude", "latitude"], get_radius=180,
                            get_fill_color=[255, 140, 0, 220], pickable=True, auto_highlight=True,
                        )],
                        initial_view_state=pdk.ViewState(latitude=22.40, longitude=113.97, zoom=11, pitch=30),
                        tooltip={"text": "{name}"},
                        map_style="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
                    ))
                except Exception:
                    st.info("Map unavailable — refresh the page to reload.")
    except Exception as e:
        st.warning(f"Light Rail data unavailable: {e}")



# ══════════════════════════════════════════════════════════════════════════════
# TAB 2: MTR Live
# ══════════════════════════════════════════════════════════════════════════════
with tab_mtr:
    st.subheader("MTR Live 港鐵實時班次")
    st.markdown("Real-time next train arrivals for MTR lines. Data from [MTR Open Data](https://opendata.mtr.com.hk), refreshed every 10 seconds.")

    # Load MTR station list
    mtr_stations_df = load_csv_url(MTR_LINES_CSV)
    mtr_stations_df.columns = [c.strip() for c in mtr_stations_df.columns]

    lines = sorted(mtr_stations_df["Line Code"].dropna().astype(str).unique().tolist())
    LINE_NAMES = {
        "AEL": "Airport Express", "TCL": "Tung Chung Line", "TML": "Tuen Ma Line",
        "TKL": "Tseung Kwan O Line", "EAL": "East Rail Line", "SIL": "South Island Line",
        "TWL": "Tsuen Wan Line", "ISL": "Island Line", "KTL": "Kwun Tong Line",
        "DRL": "Disneyland Resort Line",
    }
    line_labels = [f"{LINE_NAMES.get(l, l)} ({l})" for l in lines]
    line_map = dict(zip(line_labels, lines))

    col_line, col_sta = st.columns(2)
    with col_line:
        selected_line_label = st.selectbox("Select Line 選擇路線", line_labels, key="mtr_line")
    selected_line = line_map[selected_line_label]

    # Unique stations for this line
    line_stations = (
        mtr_stations_df[mtr_stations_df["Line Code"] == selected_line]
        .drop_duplicates(subset=["Station Code"])
        [["Station Code", "English Name", "Chinese Name"]]
    )
    station_labels = [f"{row['English Name']} {row['Chinese Name']} ({row['Station Code']})"
                      for _, row in line_stations.iterrows()]
    station_code_map = dict(zip(station_labels, line_stations["Station Code"].tolist()))

    with col_sta:
        selected_sta_label = st.selectbox("Select Station 選擇車站", station_labels, key="mtr_sta")
    selected_sta_code = station_code_map[selected_sta_label]

    # Refresh counter (busts only schedule cache, not BQ data)
    if "mtr_refresh" not in st.session_state:
        st.session_state.mtr_refresh = 0
    if st.button("🔄 Refresh 刷新"):
        st.session_state.mtr_refresh += 1

    with st.spinner("Fetching live schedule..."):
        schedule = get_mtr_schedule(selected_line, selected_sta_code, st.session_state.mtr_refresh)

    if schedule.get("status") == 1:
        curr_time = schedule.get("curr_time", "N/A")
        st.success(f"Live data as of **{curr_time}** HKT")

        if schedule.get("isdelay") == "Y":
            st.warning("⚠️ Service delay reported on this line.")

        data_key = f"{selected_line}-{selected_sta_code}"
        station_data = schedule.get("data", {}).get(data_key, {})

        col_up, col_down = st.columns(2)
        for col, direction, label in [(col_up, "UP", "⬆ Upbound"), (col_down, "DOWN", "⬇ Downbound")]:
            with col:
                st.markdown(f"**{label}**")
                trains = station_data.get(direction, [])
                valid_trains = [t for t in trains if t.get("valid") == "Y"]
                if valid_trains:
                    train_df = pd.DataFrame(valid_trains)[["seq", "dest", "plat", "time", "ttnt"]]
                    train_df.columns = ["#", "Destination", "Platform", "Arrival Time", "Min Away"]
                    st.dataframe(train_df, use_container_width=True, hide_index=True)
                else:
                    st.info("No trains scheduled in this direction.")
    else:
        st.error(f"Could not fetch schedule: {schedule.get('message', 'Unknown error')}")

    st.caption("Source: MTR Open Data API · Refreshes every 10 seconds on server · Click Refresh for latest.")

    st.divider()

    # ── Fare Explorer ──────────────────────────────────────────────────────────
    st.subheader("MTR Fare Explorer 票價查詢")
    st.caption("Fares between any two MTR stations. Compare Octopus card vs single journey ticket across passenger types.")

    fares_df = load_csv_url(MTR_FARES_CSV)
    fares_df.columns = [c.strip() for c in fares_df.columns]
    all_fare_stations = sorted(fares_df["SRC_STATION_NAME"].dropna().unique().tolist())

    col_f1, col_f2 = st.columns(2)
    with col_f1:
        fare_from = st.selectbox("From 出發站", all_fare_stations, key="fare_from")
    with col_f2:
        fare_to_opts = [s for s in all_fare_stations if s != fare_from]
        fare_to = st.selectbox("To 目的站", fare_to_opts, key="fare_to")

    fare_row = fares_df[
        (fares_df["SRC_STATION_NAME"] == fare_from) &
        (fares_df["DEST_STATION_NAME"] == fare_to)
    ]

    if not fare_row.empty:
        r = fare_row.iloc[0]
        col_oct, col_single = st.columns(2)
        with col_oct:
            st.markdown("**🟢 Octopus Card 八達通**")
            st.metric("Adult 成人", f"HK$ {float(r['OCT_ADT_FARE']):.1f}")
            st.metric("Child / Elderly 小童/長者", f"HK$ {float(r['OCT_CON_CHILD_FARE']):.1f}")
            st.metric("PWD 殘疾人士", f"HK$ {float(r['OCT_CON_PWD_FARE']):.1f}")
            st.metric("JoyYou 60", f"HK$ {float(r['OCT_JOYYOU_SIXTY_FARE']):.1f}")
        with col_single:
            st.markdown("**🎫 Single Ticket 單程票**")
            st.metric("Adult 成人", f"HK$ {float(r['SINGLE_ADT_FARE']):.1f}")
            st.metric("Child / Elderly 小童/長者", f"HK$ {float(r['SINGLE_CON_CHILD_FARE']):.1f}")
    else:
        st.info("No fare data found. Try swapping origin and destination.")



# ══════════════════════════════════════════════════════════════════════════════
# TAB 4: Streaming Analytics
# ══════════════════════════════════════════════════════════════════════════════
with tab_streaming:
    st.subheader("Streaming Analytics 實時分析")
    st.markdown("Live MTR schedule events streamed via **Redpanda** → **BigQuery**. Producer polls every 30 seconds.")

    # ── KPI row ────────────────────────────────────────────────────────────────
    stream_counts = load_data(f"""
    SELECT
        COUNT(*) AS total_events,
        COUNT(DISTINCT line) AS lines_tracked,
        COUNT(DISTINCT station) AS stations_tracked,
        COUNTIF(is_delayed) AS delayed_events,
        MAX(timestamp) AS last_updated
    FROM `{PROJECT_ID}.streaming.mtr_schedule_raw`
    """)

    if not stream_counts.empty:
        r = stream_counts.iloc[0]
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Total Events", f"{int(r['total_events']):,}")
        k2.metric("Lines Tracked", int(r['lines_tracked']))
        k3.metric("Stations Tracked", int(r['stations_tracked']))
        k4.metric("Delay Events", int(r['delayed_events']))
        st.caption(f"Last updated: {r['last_updated']} HKT")

    st.divider()

    # ── Events per line ────────────────────────────────────────────────────────
    st.subheader("Events per MTR Line")
    st.caption("Total schedule events captured per line since streaming started. Higher = more active line.")
    line_counts = load_data(f"""
    SELECT line, COUNT(*) AS events, COUNTIF(is_delayed) AS delays
    FROM `{PROJECT_ID}.streaming.mtr_schedule_raw`
    GROUP BY line ORDER BY events DESC
    """)
    LINE_NAMES = {
        "AEL": "Airport Express", "TCL": "Tung Chung", "TML": "Tuen Ma",
        "TKL": "Tseung Kwan O", "EAL": "East Rail", "SIL": "South Island",
        "TWL": "Tsuen Wan", "ISL": "Island", "KTL": "Kwun Tong", "DRL": "Disneyland",
    }
    line_counts["Line Name"] = line_counts["line"].map(LINE_NAMES).fillna(line_counts["line"])
    fig_lines = px.bar(
        line_counts, x="Line Name", y="events", color="delays",
        color_continuous_scale="Reds",
        labels={"events": "Total Events", "delays": "Delay Events", "Line Name": "MTR Line"},
    )
    fig_lines.update_layout(coloraxis_showscale=True)
    st.plotly_chart(fig_lines, use_container_width=True)

    st.divider()

    # ── Events over time ───────────────────────────────────────────────────────
    st.subheader("Event Volume Over Time")
    st.caption("Number of schedule events captured per minute. Shows polling cadence and data volume.")
    time_series = load_data(f"""
    SELECT
        TIMESTAMP_TRUNC(timestamp, MINUTE) AS minute,
        COUNT(*) AS events
    FROM `{PROJECT_ID}.streaming.mtr_schedule_raw`
    GROUP BY minute ORDER BY minute
    """)
    if not time_series.empty:
        fig_ts = px.line(
            time_series, x="minute", y="events",
            labels={"minute": "Time", "events": "Events per Minute"},
            color_discrete_sequence=["#C8102E"],
        )
        st.plotly_chart(fig_ts, use_container_width=True)

    st.divider()

    # ── Direction split ────────────────────────────────────────────────────────
    col_dir, col_dest = st.columns(2)

    with col_dir:
        st.subheader("UP vs DOWN Trains")
        st.caption("Split of inbound vs outbound trains across all lines.")
        dir_counts = load_data(f"""
        SELECT direction, COUNT(*) AS events
        FROM `{PROJECT_ID}.streaming.mtr_schedule_raw`
        GROUP BY direction
        """)
        fig_dir = px.pie(
            dir_counts, names="direction", values="events", hole=0.4,
            color_discrete_sequence=["#C8102E", "#0078ff"],
        )
        fig_dir.update_traces(textposition="inside", textinfo="percent+label")
        st.plotly_chart(fig_dir, use_container_width=True)

    with col_dest:
        st.subheader("Top Destinations")
        st.caption("Most frequent train destinations across all streamed events.")
        dest_counts = load_data(f"""
        SELECT destination, COUNT(*) AS events
        FROM `{PROJECT_ID}.streaming.mtr_schedule_raw`
        WHERE destination IS NOT NULL
        GROUP BY destination ORDER BY events DESC LIMIT 10
        """)
        fig_dest = px.bar(
            dest_counts, x="events", y="destination", orientation="h",
            color_discrete_sequence=["#b400ff"],
            labels={"events": "Events", "destination": "Destination"},
        )
        fig_dest.update_layout(yaxis=dict(autorange="reversed"))
        st.plotly_chart(fig_dest, use_container_width=True)

    st.divider()

    # ── Raw events table ───────────────────────────────────────────────────────
    st.subheader("Latest Events")
    st.caption("Most recent 20 MTR schedule events from the stream.")
    latest_events = load_data(f"""
    SELECT timestamp, line, station, direction, destination, platform, minutes_away, is_delayed
    FROM `{PROJECT_ID}.streaming.mtr_schedule_raw`
    ORDER BY timestamp DESC LIMIT 20
    """)
    st.dataframe(latest_events, use_container_width=True, hide_index=True)


# TAB 5: About
# ══════════════════════════════════════════════════════════════════════════════
with tab_about:
    st.markdown("""
    <div style='text-align:center; padding:30px 0 10px 0;'>
        <span style='font-size:60px;'>🚌</span>
        <h1 style='color:#C8102E; margin:0;'>🇭🇰 香港交通脈搏 Hong Kong Transit Pulse</h1>
        <p style='color:#666; font-size:18px;'>End-to-end batch data engineering pipeline for Hong Kong public transport</p>
        <p style='margin-top:10px;'>
            <span style='background:#C8102E; color:white; padding:4px 14px; border-radius:20px; font-size:15px; font-weight:600;'>
                📚 Data Engineering Zoomcamp 2026 — Capstone Project
            </span>
        </p>
    </div>
    """, unsafe_allow_html=True)

    st.divider()

    st.markdown("### Why This Project Exists")
    st.markdown("""
    Hong Kong runs one of the world's most complex public transport networks — MTR heavy rail,
    Light Rail, over 700 bus routes, trams, and ferries — yet the data describing how these
    systems actually perform sits scattered across multiple government portals in raw GTFS files
    and CSV exports that are difficult for the public to interpret.

    This project was built to answer simple but important questions:

    - **Which stops and routes are most heavily used?**
    - **How does service differ between weekdays and weekends?**
    - **Which areas of the city are underserved by public transport?**
    - **When do first and last services run on each route?**
    - **How do MTR and bus networks complement each other geographically?**

    By pulling open data into a structured pipeline — ingesting, cleaning, and aggregating it
    daily — the goal is to make Hong Kong's transit network legible and explorable for anyone,
    not just data engineers.

    Built for **Data Engineering Zoomcamp 2026** by Rizal, an Indonesian data engineer based
    in Jakarta with a keen interest in urban mobility, open government data, and Hong Kong cinema.
    """)

    st.divider()

    st.markdown("### Data Sources")
    st.markdown("""
    - **GTFS Static Feed** from [data.gov.hk](https://data.gov.hk)
      — KMB buses, CTB/NWFB citybus, trams, ferries · ~165,000+ stop-time records
      · Updated daily at **06:00 HKT**
    - **MTR Open Data** from [opendata.mtr.com.hk](https://opendata.mtr.com.hk)
      — MTR Heavy Rail lines & stations, Light Rail routes & stops, fare table
      · Note: MTR does not publish GTFS — trip-level data is not publicly available

    > All data is open and freely published by the Hong Kong government and MTR Corporation.
    """)

st.caption("Data source: GTFS static feed — data.gov.hk | Built for Data Engineering Zoomcamp 2026")
