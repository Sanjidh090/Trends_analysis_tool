# db.py
import sqlite3
import logging
import json
from pathlib import Path
from datetime import datetime
import pandas as pd

logger = logging.getLogger(__name__)


class TrendsDB:
    def __init__(self, db_path: str = "trends.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _connect(self):
        return sqlite3.connect(self.db_path)

    def _init_schema(self):
        with self._connect() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS interest_over_time (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    collected_at TEXT NOT NULL,
                    date         TEXT NOT NULL,
                    keyword      TEXT NOT NULL,
                    value        INTEGER NOT NULL,
                    geo          TEXT NOT NULL,
                    timeframe    TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS related_queries (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    collected_at TEXT NOT NULL,
                    keyword      TEXT NOT NULL,
                    query        TEXT NOT NULL,
                    value        TEXT,
                    query_type   TEXT NOT NULL,
                    geo          TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS ad_briefs (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at   TEXT NOT NULL,
                    keyword      TEXT NOT NULL,
                    geo          TEXT NOT NULL,
                    trend_label  TEXT NOT NULL,
                    momentum     REAL,
                    brief_json   TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS breakout_log (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    detected_at  TEXT NOT NULL,
                    keyword      TEXT NOT NULL,
                    geo          TEXT NOT NULL,
                    z_score      REAL,
                    current_val  INTEGER,
                    pct_above    REAL
                );
                CREATE TABLE IF NOT EXISTS keyword_registry (
                    keyword      TEXT PRIMARY KEY,
                    category     TEXT,
                    added_at     TEXT,
                    active       INTEGER DEFAULT 1
                );
                CREATE INDEX IF NOT EXISTS idx_iot_keyword_geo
                    ON interest_over_time(keyword, geo, date);
            """)
        logger.info(f"DB ready at {self.db_path}")

    def save_interest_over_time(self, df: pd.DataFrame, geo: str, timeframe: str):
        now  = datetime.utcnow().isoformat()
        rows = []
        for date, row in df.iterrows():
            for col in df.columns:
                if col in ("geo", "timeframe", "isPartial"):
                    continue
                rows.append((now, str(date)[:10], col, int(row[col]), geo, timeframe))
        with self._connect() as conn:
            conn.executemany(
                "INSERT INTO interest_over_time (collected_at,date,keyword,value,geo,timeframe) VALUES (?,?,?,?,?,?)",
                rows,
            )
        logger.debug(f"Saved {len(rows)} rows | geo={geo}")

    def save_related_queries(self, related: dict, geo: str):
        now  = datetime.utcnow().isoformat()
        rows = []
        for keyword, data in related.items():
            for qtype in ("top", "rising"):
                df = data.get(qtype)
                if df is None or df.empty:
                    continue
                for _, row in df.iterrows():
                    rows.append((now, keyword, str(row.get("query", "")),
                                 str(row.get("value", "")), qtype, geo))
        with self._connect() as conn:
            conn.executemany(
                "INSERT INTO related_queries (collected_at,keyword,query,value,query_type,geo) VALUES (?,?,?,?,?,?)",
                rows,
            )

    def save_ad_brief(self, brief: dict):
        now = datetime.utcnow().isoformat()
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO ad_briefs (created_at,keyword,geo,trend_label,momentum,brief_json) VALUES (?,?,?,?,?,?)",
                (now, brief["keyword"], brief["geo"], brief["trend"],
                 brief["momentum"], json.dumps(brief)),
            )

    def log_breakout(self, keyword, geo, z_score, current_val, pct_above):
        now = datetime.utcnow().isoformat()
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO breakout_log (detected_at,keyword,geo,z_score,current_val,pct_above) VALUES (?,?,?,?,?,?)",
                (now, keyword, geo, z_score, current_val, pct_above),
            )

    def get_interest_history(self, keyword: str, geo: str, days: int = 90) -> pd.DataFrame:
        q = """
            SELECT date, AVG(value) as value
            FROM interest_over_time
            WHERE keyword=? AND geo=? AND date >= date('now', ?)
            GROUP BY date ORDER BY date
        """
        with self._connect() as conn:
            df = pd.read_sql_query(q, conn, params=(keyword, geo, f"-{days} days"))
        if not df.empty:
            df["date"] = pd.to_datetime(df["date"])
            df = df.set_index("date")
        return df

    def get_breakout_log(self, days: int = 7) -> pd.DataFrame:
        with self._connect() as conn:
            return pd.read_sql_query(
                "SELECT * FROM breakout_log WHERE detected_at >= datetime('now', ?) ORDER BY detected_at DESC",
                conn, params=(f"-{days} days",),
            )

    def get_rising_queries(self, geo: str, limit: int = 50) -> pd.DataFrame:
        q = """
            SELECT keyword, query, value, collected_at
            FROM related_queries
            WHERE query_type='rising' AND geo=?
            ORDER BY collected_at DESC LIMIT ?
        """
        with self._connect() as conn:
            return pd.read_sql_query(q, conn, params=(geo, limit))
