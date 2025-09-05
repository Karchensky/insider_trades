import os
from urllib.parse import urlparse, parse_qs
import psycopg2
import pandas as pd
import streamlit as st
from dotenv import load_dotenv


load_dotenv()


def _ensure_ssl_in_dsn(dsn: str) -> str:
    if not dsn:
        return dsn
    # If DSN already has sslmode, return as-is
    if 'sslmode=' in dsn:
        return dsn
    # If URL form, append query
    if dsn.startswith('postgres://') or dsn.startswith('postgresql://'):
        sep = '&' if ('?' in dsn) else '?'
        return f"{dsn}{sep}sslmode=require"
    # Else append key=value
    return f"{dsn} sslmode=require"


@st.cache_resource(show_spinner=False)
def get_conn(dsn_override: str | None = None):
    dsn = dsn_override or os.getenv('SUPABASE_DB_URL')
    if not dsn:
        raise RuntimeError("SUPABASE_DB_URL is not set. Provide it in .env or via the sidebar.")
    dsn = _ensure_ssl_in_dsn(dsn)
    return psycopg2.connect(dsn)


def load_latest_anomalies(limit: int = 200, dsn: str | None = None):
    with get_conn(dsn).cursor() as cur:
        cur.execute(
            """
            SELECT 
                event_date, 
                symbol, 
                direction,
                score,
                anomaly_types,
                total_individual_anomalies,
                max_individual_score,
                CASE 
                    WHEN score >= 7.0 THEN 'CRITICAL'
                    WHEN score >= 5.0 THEN 'HIGH'
                    WHEN score >= 3.0 THEN 'MEDIUM'
                    ELSE 'LOW'
                END as risk_level,
                details::text as details,
                as_of_timestamp
            FROM temp_anomaly
            ORDER BY event_date DESC, score DESC
            LIMIT %s
            """,
            (limit,)
        )
        rows = cur.fetchall()
        cols = [d.name for d in cur.description]
    return pd.DataFrame(rows, columns=cols)


def load_symbol_history(symbol: str, days: int = 30, dsn: str | None = None):
    with get_conn(dsn).cursor() as cur:
        cur.execute(
            """
            SELECT date, SUM(volume)::bigint AS options_volume
            FROM daily_option_snapshot
            WHERE symbol = %s AND date >= CURRENT_DATE - %s::interval
            GROUP BY date
            ORDER BY date
            """,
            (symbol, f"{days} days",)
        )
        rows = cur.fetchall()
        cols = [d.name for d in cur.description]
    return pd.DataFrame(rows, columns=cols)


def load_contract_history(contract_ticker: str, days: int = 30, dsn: str | None = None):
    with get_conn(dsn).cursor() as cur:
        cur.execute(
            """
            SELECT date, volume
            FROM daily_option_snapshot
            WHERE contract_ticker = %s AND date >= CURRENT_DATE - %s::interval
            ORDER BY date
            """,
            (contract_ticker, f"{days} days",)
        )
        rows = cur.fetchall()
        cols = [d.name for d in cur.description]
    return pd.DataFrame(rows, columns=cols)


def load_symbol_type_series(symbol: str, start_date: str, end_date: str, dsn: str | None = None):
    """Time series by contract_type using daily_option_snapshot."""
    with get_conn(dsn).cursor() as cur:
        cur.execute(
            """
            SELECT date AS date, contract_type, SUM(session_volume)::bigint AS vol
            FROM daily_option_snapshot
            WHERE symbol = %s AND date BETWEEN %s AND %s
            GROUP BY date, contract_type
            ORDER BY date
            """,
            (symbol, start_date, end_date)
        )
        rows = cur.fetchall()
        cols = [d.name for d in cur.description]
    df = pd.DataFrame(rows, columns=cols)
    if df.empty:
        return df
    return df.pivot_table(index='date', columns='contract_type', values='vol', fill_value=0)


def load_symbol_heatmap(symbol: str, day: str, dsn: str | None = None, put_calls: str = 'both'):
    """Heatmap data by expiration_date x strike using daily_option_snapshot."""
    q_extra = ""
    if put_calls == 'calls':
        q_extra = " AND contract_type = 'call'"
    elif put_calls == 'puts':
        q_extra = " AND contract_type = 'put'"
    with get_conn(dsn).cursor() as cur:
        cur.execute(
            f"""
            SELECT expiration_date, strike_price::float AS strike, SUM(session_volume)::bigint AS vol
            FROM daily_option_snapshot
            WHERE symbol = %s AND date = %s{q_extra}
            GROUP BY expiration_date, strike
            """,
            (symbol, day)
        )
        rows = cur.fetchall()
        cols = [d.name for d in cur.description]
    df = pd.DataFrame(rows, columns=cols)
    if df.empty:
        return df
    return df.pivot_table(index='expiration_date', columns='strike', values='vol', fill_value=0).sort_index()


st.set_page_config(page_title="Insider Anomalies", layout="wide")
st.title("Insider Anomaly Monitor")

with st.sidebar:
    st.header("Filters")
    dsn_input = st.text_input("Database DSN (optional)", value=os.getenv('SUPABASE_DB_URL', ""), type="password")
    kind = st.selectbox("Anomaly kind", options=["(any)",
        "contract_volume_spike", "contract_volume_p95_exceed",
        "symbol_options_volume_spike", "contract_intraday_volume_spike",
        "oom_short_term_cluster", "otm_put_short_term_cluster",
        "strike_ladder_concentration", "open_interest_jump",
        "options_spike_with_stock_move"
    ])
    top_n = st.slider("Show top N", min_value=50, max_value=1000, value=300, step=50)

df = load_latest_anomalies(limit=top_n, dsn=dsn_input or None)
if kind != "(any)":
    df = df[df['kind'] == kind]

st.subheader("Recent Anomalies")
st.dataframe(df, use_container_width=True)

st.subheader("Explore")
col1, col2 = st.columns(2)
with col1:
    sym = st.text_input("Symbol", value=(df['symbol'].iloc[0] if not df.empty else ""))
    if sym:
        sh = load_symbol_history(sym, days=60, dsn=dsn_input or None)
        st.line_chart(sh.set_index('date'))
with col2:
    ct = st.text_input("Contract Ticker", value=(df['contract_ticker'].iloc[0] if not df.empty else ""))
    if ct:
        ch = load_contract_history(ct, days=60, dsn=dsn_input or None)
        st.line_chart(ch.set_index('date'))

st.subheader("Symbol Deep Dive")
sym2 = st.text_input("Symbol (deep dive)", value=(df['symbol'].iloc[0] if not df.empty else ""), key="sym2")
date2 = st.date_input("Date", value=pd.to_datetime(df['event_date'].iloc[0]).date() if not df.empty else pd.Timestamp.today().date())
pc = st.selectbox("Type", options=["both", "calls", "puts"], index=0)
if sym2:
    series = load_symbol_type_series(sym2, (pd.to_datetime(date2) - pd.Timedelta(days=60)).date().isoformat(), pd.to_datetime(date2).date().isoformat(), dsn=dsn_input or None)
    if not series.empty:
        st.area_chart(series)
    heat = load_symbol_heatmap(sym2, pd.to_datetime(date2).date().isoformat(), dsn=dsn_input or None, put_calls=pc)
    if not heat.empty:
        st.caption("Session volume heatmap (expiration x strike)")
        st.dataframe(heat)


