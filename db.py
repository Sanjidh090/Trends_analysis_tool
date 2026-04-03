# db.py
import sqlite3
import logging
import json
from pathlib import Path
from datetime import datetime
from typing import Optional
import pandas as pd

logger = logging.getLogger(__name__)


class TrendsDB:
    """
    Database layer for the Trends Intelligence Platform.

    Supports SQLite (default, zero-config) and PostgreSQL (for team use).

    Initialise with a db_config dict from config.yaml["database"]:
        db = TrendsDB(db_path="storage/trends.db")                       # SQLite
        db = TrendsDB(db_config={"type":"postgres","host":...,"name":...})# Postgres
    """

    def __init__(
        self,
        db_path: str = "trends.db",
        db_config: Optional[dict] = None,
    ):
        cfg = db_config or {}
        self._db_type = cfg.get("type", "sqlite").lower()

        if self._db_type == "postgres":
            self._pg_dsn = {
                "host":     cfg.get("host", "localhost"),
                "port":     int(cfg.get("port", 5432)),
                "dbname":   cfg.get("name", "trends_intel"),
                "user":     cfg.get("user", "postgres"),
                "password": cfg.get("password", ""),
            }
            logger.info(f"DB mode: PostgreSQL | host={self._pg_dsn['host']} | db={self._pg_dsn['dbname']}")
        else:
            self.db_path = Path(db_path)
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            logger.info(f"DB mode: SQLite | path={self.db_path}")

        self._init_schema()

    # ── Connection factory ────────────────────────────────────────────────────

    def get_connection(self):
        """Return an open database connection (SQLite or psycopg2)."""
        if self._db_type == "postgres":
            import psycopg2
            return psycopg2.connect(**self._pg_dsn)
        return sqlite3.connect(self.db_path)

    # Backwards-compatible alias used inside this class
    def _connect(self):
        return self.get_connection()

    # ── Helpers ───────────────────────────────────────────────────────────────

    @property
    def _ph(self) -> str:
        """SQL placeholder character: ? for SQLite, %s for PostgreSQL."""
        return "%s" if self._db_type == "postgres" else "?"

    def _date_range_expr(self, days: int) -> str:
        """SQL expression for 'N days ago' compatible with both backends."""
        if self._db_type == "postgres":
            return f"NOW() - INTERVAL '{days} days'"
        return f"date('now', '-{days} days')"

    def _datetime_range_expr(self, days: int) -> str:
        """SQL expression for 'N days ago' (datetime precision)."""
        if self._db_type == "postgres":
            return f"NOW() - INTERVAL '{days} days'"
        return f"datetime('now', '-{days} days')"

    def _executemany(self, conn, sql: str, rows: list):
        """Run executemany, handling cursor differences between backends."""
        if self._db_type == "postgres":
            with conn.cursor() as cur:
                cur.executemany(sql, rows)
            conn.commit()
        else:
            conn.executemany(sql, rows)

    def _execute(self, conn, sql: str, params: tuple = ()):
        """Run a single execute, handling cursor differences."""
        if self._db_type == "postgres":
            with conn.cursor() as cur:
                cur.execute(sql, params)
            conn.commit()
        else:
            conn.execute(sql, params)

    def _read_sql(self, sql: str, params: tuple = ()) -> pd.DataFrame:
        """Run a SELECT and return a DataFrame, handling both backends."""
        with self._connect() as conn:
            if self._db_type == "postgres":
                import psycopg2.extras
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(sql, params)
                    rows    = cur.fetchall()
                    columns = [desc[0] for desc in cur.description] if cur.description else []
                return pd.DataFrame([dict(r) for r in rows], columns=columns)
            return pd.read_sql_query(sql, conn, params=params)

    # ── Schema ────────────────────────────────────────────────────────────────

    def _init_schema(self):
        if self._db_type == "postgres":
            self._init_schema_postgres()
        else:
            self._init_schema_sqlite()

    def _init_schema_sqlite(self):
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
                CREATE TABLE IF NOT EXISTS share_of_search (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    collected_at TEXT NOT NULL,
                    date         TEXT NOT NULL,
                    keyword      TEXT NOT NULL,
                    share_pct    REAL NOT NULL,
                    geo          TEXT NOT NULL,
                    timeframe    TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_sos_keyword_geo
                    ON share_of_search(keyword, geo, date);
                CREATE TABLE IF NOT EXISTS ad_copy (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at   TEXT NOT NULL,
                    keyword      TEXT NOT NULL,
                    geo          TEXT NOT NULL,
                    platform     TEXT NOT NULL,
                    headline     TEXT,
                    body         TEXT,
                    cta          TEXT,
                    hashtags     TEXT,
                    model        TEXT
                );
            """)
        logger.info(f"SQLite schema ready at {self.db_path}")

    def _init_schema_postgres(self):
        statements = [
            """CREATE TABLE IF NOT EXISTS interest_over_time (
                id           SERIAL PRIMARY KEY,
                collected_at TEXT NOT NULL,
                date         TEXT NOT NULL,
                keyword      TEXT NOT NULL,
                value        INTEGER NOT NULL,
                geo          TEXT NOT NULL,
                timeframe    TEXT NOT NULL
            )""",
            """CREATE TABLE IF NOT EXISTS related_queries (
                id           SERIAL PRIMARY KEY,
                collected_at TEXT NOT NULL,
                keyword      TEXT NOT NULL,
                query        TEXT NOT NULL,
                value        TEXT,
                query_type   TEXT NOT NULL,
                geo          TEXT NOT NULL
            )""",
            """CREATE TABLE IF NOT EXISTS ad_briefs (
                id           SERIAL PRIMARY KEY,
                created_at   TEXT NOT NULL,
                keyword      TEXT NOT NULL,
                geo          TEXT NOT NULL,
                trend_label  TEXT NOT NULL,
                momentum     REAL,
                brief_json   TEXT NOT NULL
            )""",
            """CREATE TABLE IF NOT EXISTS breakout_log (
                id           SERIAL PRIMARY KEY,
                detected_at  TEXT NOT NULL,
                keyword      TEXT NOT NULL,
                geo          TEXT NOT NULL,
                z_score      REAL,
                current_val  INTEGER,
                pct_above    REAL
            )""",
            """CREATE TABLE IF NOT EXISTS keyword_registry (
                keyword      TEXT PRIMARY KEY,
                category     TEXT,
                added_at     TEXT,
                active       INTEGER DEFAULT 1
            )""",
            "CREATE INDEX IF NOT EXISTS idx_iot_keyword_geo ON interest_over_time(keyword, geo, date)",
            """CREATE TABLE IF NOT EXISTS share_of_search (
                id           SERIAL PRIMARY KEY,
                collected_at TEXT NOT NULL,
                date         TEXT NOT NULL,
                keyword      TEXT NOT NULL,
                share_pct    REAL NOT NULL,
                geo          TEXT NOT NULL,
                timeframe    TEXT NOT NULL
            )""",
            "CREATE INDEX IF NOT EXISTS idx_sos_keyword_geo ON share_of_search(keyword, geo, date)",
            """CREATE TABLE IF NOT EXISTS ad_copy (
                id           SERIAL PRIMARY KEY,
                created_at   TEXT NOT NULL,
                keyword      TEXT NOT NULL,
                geo          TEXT NOT NULL,
                platform     TEXT NOT NULL,
                headline     TEXT,
                body         TEXT,
                cta          TEXT,
                hashtags     TEXT,
                model        TEXT
            )""",
        ]
        with self._connect() as conn:
            for stmt in statements:
                self._execute(conn, stmt)
        logger.info("PostgreSQL schema ready")

    # ── Write methods ─────────────────────────────────────────────────────────

    def save_interest_over_time(self, df: pd.DataFrame, geo: str, timeframe: str):
        now  = datetime.utcnow().isoformat()
        ph   = self._ph
        rows = []
        for date, row in df.iterrows():
            for col in df.columns:
                if col in ("geo", "timeframe", "isPartial"):
                    continue
                rows.append((now, str(date)[:10], col, int(row[col]), geo, timeframe))
        with self._connect() as conn:
            self._executemany(
                conn,
                f"INSERT INTO interest_over_time (collected_at,date,keyword,value,geo,timeframe) VALUES ({ph},{ph},{ph},{ph},{ph},{ph})",
                rows,
            )
        logger.debug(f"Saved {len(rows)} rows | geo={geo}")

    def save_related_queries(self, related: dict, geo: str):
        now  = datetime.utcnow().isoformat()
        ph   = self._ph
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
            self._executemany(
                conn,
                f"INSERT INTO related_queries (collected_at,keyword,query,value,query_type,geo) VALUES ({ph},{ph},{ph},{ph},{ph},{ph})",
                rows,
            )

    def save_ad_brief(self, brief: dict):
        now = datetime.utcnow().isoformat()
        ph  = self._ph
        with self._connect() as conn:
            self._execute(
                conn,
                f"INSERT INTO ad_briefs (created_at,keyword,geo,trend_label,momentum,brief_json) VALUES ({ph},{ph},{ph},{ph},{ph},{ph})",
                (now, brief["keyword"], brief["geo"], brief["trend"],
                 brief["momentum"], json.dumps(brief)),
            )

    def log_breakout(self, keyword, geo, z_score, current_val, pct_above):
        now = datetime.utcnow().isoformat()
        ph  = self._ph
        with self._connect() as conn:
            self._execute(
                conn,
                f"INSERT INTO breakout_log (detected_at,keyword,geo,z_score,current_val,pct_above) VALUES ({ph},{ph},{ph},{ph},{ph},{ph})",
                (now, keyword, geo, z_score, current_val, pct_above),
            )

    # ── Read methods ──────────────────────────────────────────────────────────

    def get_interest_history(self, keyword: str, geo: str, days: int = 90) -> pd.DataFrame:
        date_expr = self._date_range_expr(days)
        ph = self._ph
        q  = f"""
            SELECT date, AVG(value) as value
            FROM interest_over_time
            WHERE keyword={ph} AND geo={ph} AND date >= {date_expr}
            GROUP BY date ORDER BY date
        """
        df = self._read_sql(q, (keyword, geo))
        if not df.empty:
            df["date"] = pd.to_datetime(df["date"])
            df = df.set_index("date")
        return df

    def get_breakout_log(self, days: int = 7) -> pd.DataFrame:
        dt_expr = self._datetime_range_expr(days)
        return self._read_sql(
            f"SELECT * FROM breakout_log WHERE detected_at >= {dt_expr} ORDER BY detected_at DESC",
        )

    def get_rising_queries(self, geo: str, limit: int = 50) -> pd.DataFrame:
        ph = self._ph
        q  = f"""
            SELECT keyword, query, value, collected_at
            FROM related_queries
            WHERE query_type='rising' AND geo={ph}
            ORDER BY collected_at DESC LIMIT {ph}
        """
        return self._read_sql(q, (geo, limit))

    # ── Share of Search ───────────────────────────────────────────────────────

    def save_share_of_search(self, df: pd.DataFrame, geo: str, timeframe: str):
        """Persist normalised share-of-search percentages for a set of keywords."""
        now  = datetime.utcnow().isoformat()
        ph   = self._ph
        rows = []
        for date, row in df.iterrows():
            for col in df.columns:
                rows.append((now, str(date)[:10], col, float(row[col]), geo, timeframe))
        with self._connect() as conn:
            self._executemany(
                conn,
                f"INSERT INTO share_of_search (collected_at,date,keyword,share_pct,geo,timeframe) VALUES ({ph},{ph},{ph},{ph},{ph},{ph})",
                rows,
            )
        logger.debug(f"Saved {len(rows)} share-of-search rows | geo={geo}")

    def get_share_of_search_history(self, keywords: list, geo: str, days: int = 90) -> pd.DataFrame:
        """Return share-of-search history for the given keywords pivoted wide."""
        ph           = self._ph
        placeholders = ",".join([ph] * len(keywords))
        date_expr    = self._date_range_expr(days)
        q = f"""
            SELECT date, keyword, AVG(share_pct) as share_pct
            FROM share_of_search
            WHERE keyword IN ({placeholders}) AND geo={ph} AND date >= {date_expr}
            GROUP BY date, keyword ORDER BY date
        """
        df = self._read_sql(q, (*keywords, geo))
        if df.empty:
            return pd.DataFrame()
        df["date"] = pd.to_datetime(df["date"])
        return df.pivot(index="date", columns="keyword", values="share_pct").fillna(0)

    # ── Ad Copy ───────────────────────────────────────────────────────────────

    def save_ad_copy(self, keyword: str, geo: str, platform: str, copy: dict, model: str = ""):
        """Persist GPT-generated ad copy."""
        now = datetime.utcnow().isoformat()
        ph  = self._ph
        with self._connect() as conn:
            self._execute(
                conn,
                f"INSERT INTO ad_copy (created_at,keyword,geo,platform,headline,body,cta,hashtags,model) VALUES ({ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph})",
                (
                    now, keyword, geo, platform,
                    copy.get("headline"), copy.get("body"), copy.get("cta"),
                    json.dumps(copy.get("hashtags", [])), model,
                ),
            )

    def get_ad_copy_history(self, keyword: str, geo: str, limit: int = 20) -> pd.DataFrame:
        """Retrieve recently generated ad copy for a keyword/geo."""
        ph = self._ph
        q  = f"""
            SELECT created_at, platform, headline, body, cta, hashtags, model
            FROM ad_copy
            WHERE keyword={ph} AND geo={ph}
            ORDER BY created_at DESC LIMIT {ph}
        """
        return self._read_sql(q, (keyword, geo, limit))
