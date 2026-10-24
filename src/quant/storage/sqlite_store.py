"""SQLite 存储层：alerts / candles 两表 CRUD + 查询过滤。

约定：
- 时间以 epoch ms（UTC）整数存储，读取时经 :mod:`quant.utils.timeutils`
  还原为 aware ``datetime``；
- ``save_candles`` 按 ``(symbol, open_time_ms)`` upsert，重复写入幂等；
- 连接 ``check_same_thread=False``（渲染/消费协程同一事件循环，
  键盘线程不触碰存储）；写盘为毫秒级小事务，不阻断行情主链。
"""

from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path

from quant.models import Alert, Candle
from quant.utils.timeutils import datetime_to_ms, ms_to_datetime

#: 建表 DDL（幂等）
_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS alerts (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_ms         INTEGER NOT NULL,
    symbol        TEXT    NOT NULL,
    rule          TEXT    NOT NULL,
    message       TEXT    NOT NULL,
    trigger_value REAL    NOT NULL,
    threshold     REAL    NOT NULL,
    level         TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_alerts_symbol  ON alerts(symbol);
CREATE INDEX IF NOT EXISTS idx_alerts_rule    ON alerts(rule);
CREATE INDEX IF NOT EXISTS idx_alerts_ts_ms   ON alerts(ts_ms);

CREATE TABLE IF NOT EXISTS candles (
    symbol       TEXT    NOT NULL,
    open_time_ms INTEGER NOT NULL,
    close_time_ms INTEGER NOT NULL,
    open         REAL    NOT NULL,
    high         REAL    NOT NULL,
    low          REAL    NOT NULL,
    close        REAL    NOT NULL,
    volume       REAL    NOT NULL,
    quote_volume REAL    NOT NULL,
    trades       INTEGER NOT NULL,
    is_closed    INTEGER NOT NULL,
    PRIMARY KEY (symbol, open_time_ms)
);
CREATE INDEX IF NOT EXISTS idx_candles_symbol_time ON candles(symbol, open_time_ms);
"""

#: candles upsert：同 (symbol, open_time_ms) 更新而非重复插入
_UPSERT_CANDLE_SQL = """
INSERT INTO candles (
    symbol, open_time_ms, close_time_ms, open, high, low, close,
    volume, quote_volume, trades, is_closed
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(symbol, open_time_ms) DO UPDATE SET
    close_time_ms = excluded.close_time_ms,
    open          = excluded.open,
    high          = excluded.high,
    low           = excluded.low,
    close         = excluded.close,
    volume        = excluded.volume,
    quote_volume  = excluded.quote_volume,
    trades        = excluded.trades,
    is_closed     = excluded.is_closed
"""

_INSERT_ALERT_SQL = """
INSERT INTO alerts (ts_ms, symbol, rule, message, trigger_value, threshold, level)
VALUES (?, ?, ?, ?, ?, ?, ?)
"""


class Storage:
    """SQLite 存储：告警与 K 线的持久化及查询。"""

    def __init__(self, db_path: str | Path) -> None:
        """打开（必要时创建）数据库文件并建立连接。

        参数:
            db_path: 数据库文件路径，父目录不存在时自动创建。
        """
        self.db_path = str(db_path)
        path = Path(self.db_path)
        if path.parent and not path.parent.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
        self.conn: sqlite3.Connection = sqlite3.connect(
            self.db_path, check_same_thread=False
        )

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------

    def init_schema(self) -> None:
        """建表与索引（幂等，可重复调用）。"""
        with self.conn:
            self.conn.executescript(_SCHEMA_SQL)

    def close(self) -> None:
        """关闭数据库连接（幂等）。"""
        try:
            self.conn.close()
        except sqlite3.Error:
            pass

    def __enter__(self) -> "Storage":
        """进入上下文时确保 schema 就绪。"""
        self.init_schema()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        """退出上下文时关闭连接。"""
        self.close()

    # ------------------------------------------------------------------
    # 告警
    # ------------------------------------------------------------------

    def save_alert(self, alert: Alert) -> None:
        """写入一条告警（毫秒级小事务）。"""
        with self.conn:
            self.conn.execute(
                _INSERT_ALERT_SQL,
                (
                    datetime_to_ms(alert.ts),
                    alert.symbol,
                    alert.rule,
                    alert.message,
                    float(alert.trigger_value),
                    float(alert.threshold),
                    alert.level,
                ),
            )

    def query_alerts(
        self,
        symbol: str | None = None,
        rule: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[Alert]:
        """按币种/规则/时间范围查询告警，按时间升序返回。

        参数:
            symbol: 币种过滤（如 ``"BTCUSDT"``），``None`` 不过滤。
            rule: 规则名过滤，``None`` 不过滤。
            start: 起始时间（含），naive 时间按 UTC 解释。
            end: 结束时间（含），naive 时间按 UTC 解释。
        """
        clauses: list[str] = []
        params: list[object] = []
        if symbol is not None:
            clauses.append("symbol = ?")
            params.append(symbol)
        if rule is not None:
            clauses.append("rule = ?")
            params.append(rule)
        if start is not None:
            clauses.append("ts_ms >= ?")
            params.append(datetime_to_ms(start))
        if end is not None:
            clauses.append("ts_ms <= ?")
            params.append(datetime_to_ms(end))

        sql = "SELECT ts_ms, symbol, rule, message, trigger_value, threshold, level FROM alerts"
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY ts_ms ASC, id ASC"

        alerts: list[Alert] = []
        for row in self.conn.execute(sql, params).fetchall():
            alerts.append(
                Alert(
                    ts=ms_to_datetime(int(row[0])),
                    symbol=str(row[1]),
                    rule=str(row[2]),
                    message=str(row[3]),
                    trigger_value=float(row[4]),
                    threshold=float(row[5]),
                    level=str(row[6]),
                )
            )
        return alerts

    # ------------------------------------------------------------------
    # K 线
    # ------------------------------------------------------------------

    def save_candles(self, symbol: str, candles: list[Candle]) -> None:
        """批量写入 K 线（upsert 幂等，单事务）。

        参数:
            symbol: 币种；与 ``candles[i].symbol`` 不一致时以参数为准。
            candles: K 线列表，可为空（空列表为无操作）。
        """
        if not candles:
            return
        rows = [
            (
                symbol,
                candle.open_time_ms,
                candle.close_time_ms,
                candle.open,
                candle.high,
                candle.low,
                candle.close,
                candle.volume,
                candle.quote_volume,
                candle.trades,
                1 if candle.is_closed else 0,
            )
            for candle in candles
        ]
        with self.conn:
            self.conn.executemany(_UPSERT_CANDLE_SQL, rows)

    def load_candles(self, symbol: str, limit: int, only_closed: bool = True) -> list[Candle]:
        """加载某币种最近 ``limit`` 根 K 线，按开盘时间升序返回。

        参数:
            symbol: 币种。
            limit: 最多返回根数（须为正整数）。
            only_closed: 为 ``True`` 时仅返回已收盘 K 线（默认）。
        """
        if limit <= 0:
            raise ValueError("limit 必须为正整数")
        sql = (
            "SELECT open_time_ms, close_time_ms, open, high, low, close, "
            "volume, quote_volume, trades, is_closed FROM candles WHERE symbol = ?"
        )
        params: list[object] = [symbol]
        if only_closed:
            sql += " AND is_closed = 1"
        sql += " ORDER BY open_time_ms DESC LIMIT ?"
        params.append(limit)

        rows = self.conn.execute(sql, params).fetchall()
        # 数据库中按时间倒序取最近 N 根，返回前反转为升序（适配指标窗口）
        candles: list[Candle] = []
        for row in reversed(rows):
            candles.append(
                Candle(
                    symbol=symbol,
                    open_time_ms=int(row[0]),
                    close_time_ms=int(row[1]),
                    open=float(row[2]),
                    high=float(row[3]),
                    low=float(row[4]),
                    close=float(row[5]),
                    volume=float(row[6]),
                    quote_volume=float(row[7]),
                    trades=int(row[8]),
                    is_closed=bool(row[9]),
                )
            )
        return candles
