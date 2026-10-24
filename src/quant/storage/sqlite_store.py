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

from quant.models import (
    Account,
    Alert,
    Candle,
    Fill,
    Order,
    PositionEvent,
    SignalRecord,
)
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

-- ===================== M2 模拟盘 / 复盘表 =====================
CREATE TABLE IF NOT EXISTS signals (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_ms         INTEGER NOT NULL,
    market        TEXT    NOT NULL,
    symbol        TEXT    NOT NULL,
    rule          TEXT    NOT NULL,
    direction     TEXT    NOT NULL,
    mode          TEXT    NOT NULL,
    status        TEXT    NOT NULL,
    trigger_value REAL    NOT NULL,
    order_id      INTEGER
);
CREATE INDEX IF NOT EXISTS idx_signals_symbol ON signals(symbol);
CREATE INDEX IF NOT EXISTS idx_signals_rule   ON signals(rule);
CREATE INDEX IF NOT EXISTS idx_signals_ts_ms ON signals(ts_ms);

CREATE TABLE IF NOT EXISTS orders (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_ms       INTEGER NOT NULL,
    signal_id   INTEGER,
    symbol      TEXT    NOT NULL,
    side        TEXT    NOT NULL,
    qty         REAL    NOT NULL,
    ref_price   REAL    NOT NULL,
    status      TEXT    NOT NULL,
    reason      TEXT
);
CREATE INDEX IF NOT EXISTS idx_orders_symbol ON orders(symbol);
CREATE INDEX IF NOT EXISTS idx_orders_ts_ms  ON orders(ts_ms);

CREATE TABLE IF NOT EXISTS fills (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id    INTEGER NOT NULL,
    ts_ms       INTEGER NOT NULL,
    symbol      TEXT    NOT NULL,
    qty         REAL    NOT NULL,
    fill_price  REAL    NOT NULL,
    fee         REAL    NOT NULL,
    amount      REAL    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_fills_symbol ON fills(symbol);
CREATE INDEX IF NOT EXISTS idx_fills_ts_ms  ON fills(ts_ms);

CREATE TABLE IF NOT EXISTS positions (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_ms         INTEGER NOT NULL,
    symbol        TEXT    NOT NULL,
    event         TEXT    NOT NULL,
    qty           REAL    NOT NULL,
    price         REAL    NOT NULL,
    realized_pnl  REAL    NOT NULL DEFAULT 0.0
);
CREATE INDEX IF NOT EXISTS idx_positions_symbol ON positions(symbol);
CREATE INDEX IF NOT EXISTS idx_positions_ts_ms  ON positions(ts_ms);

CREATE TABLE IF NOT EXISTS accounts (
    market        TEXT PRIMARY KEY,
    cash          REAL NOT NULL,
    realized_pnl  REAL NOT NULL DEFAULT 0.0
);

CREATE TABLE IF NOT EXISTS positions_current (
    symbol     TEXT PRIMARY KEY,
    market     TEXT NOT NULL,
    qty        REAL NOT NULL,
    avg_cost   REAL NOT NULL,
    opened_ts  INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_positions_current_market ON positions_current(market);
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

#: M2 模拟盘 / 复盘写入语句
_INSERT_SIGNAL_SQL = """
INSERT INTO signals (ts_ms, market, symbol, rule, direction, mode, status, trigger_value, order_id)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

_INSERT_ORDER_SQL = """
INSERT INTO orders (ts_ms, signal_id, symbol, side, qty, ref_price, status, reason)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
"""

_INSERT_FILL_SQL = """
INSERT INTO fills (order_id, ts_ms, symbol, qty, fill_price, fee, amount)
VALUES (?, ?, ?, ?, ?, ?, ?)
"""

_INSERT_POSITION_SQL = """
INSERT INTO positions (ts_ms, symbol, event, qty, price, realized_pnl)
VALUES (?, ?, ?, ?, ?, ?)
"""

_UPSERT_ACCOUNT_SQL = """
INSERT INTO accounts (market, cash, realized_pnl) VALUES (?, ?, ?)
ON CONFLICT(market) DO UPDATE SET
    cash = excluded.cash,
    realized_pnl = excluded.realized_pnl
"""

_UPSERT_POSITION_CURRENT_SQL = """
INSERT INTO positions_current (symbol, market, qty, avg_cost, opened_ts)
VALUES (?, ?, ?, ?, ?)
ON CONFLICT(symbol) DO UPDATE SET
    market = excluded.market,
    qty = excluded.qty,
    avg_cost = excluded.avg_cost,
    opened_ts = excluded.opened_ts
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

    # ------------------------------------------------------------------
    # 模拟盘 / 复盘（M2）
    # ------------------------------------------------------------------

    def save_signal(self, signal: SignalRecord) -> int:
        """写入一条信号记录，回填 ``signal.id`` 并返回。"""
        with self.conn:
            cursor = self.conn.execute(
                _INSERT_SIGNAL_SQL,
                (
                    int(signal.ts_ms),
                    signal.market,
                    signal.symbol,
                    signal.rule,
                    signal.direction,
                    signal.mode,
                    signal.status,
                    float(signal.trigger_value),
                    signal.order_id,
                ),
            )
            signal.id = cursor.lastrowid
            return int(signal.id)  # type: ignore[return-value]

    def link_signal_order(self, signal_id: int, order_id: int) -> None:
        """将信号关联到其成交订单（落库）。"""
        with self.conn:
            self.conn.execute(
                "UPDATE signals SET order_id = ? WHERE id = ?",
                (order_id, signal_id),
            )

    def update_signal_status(
        self, signal_id: int, status: str, order_id: int | None = None
    ) -> None:
        """更新信号状态（可选同时关联订单），用于 pending→ordered 等迁移。"""
        with self.conn:
            if order_id is not None:
                self.conn.execute(
                    "UPDATE signals SET status = ?, order_id = ? WHERE id = ?",
                    (status, order_id, signal_id),
                )
            else:
                self.conn.execute(
                    "UPDATE signals SET status = ? WHERE id = ?",
                    (status, signal_id),
                )

    def save_order(self, order: Order) -> int:
        """写入一笔订单，回填 ``order.id`` 并返回。"""
        with self.conn:
            cursor = self.conn.execute(
                _INSERT_ORDER_SQL,
                (
                    int(order.ts_ms),
                    order.signal_id,
                    order.symbol,
                    order.side,
                    float(order.qty),
                    float(order.ref_price),
                    order.status,
                    order.reason,
                ),
            )
            order.id = cursor.lastrowid
            return int(order.id)  # type: ignore[return-value]

    def save_fill(self, fill: Fill) -> int:
        """写入一笔成交，回填 ``fill.id`` 并返回。"""
        with self.conn:
            cursor = self.conn.execute(
                _INSERT_FILL_SQL,
                (
                    int(fill.order_id),
                    int(fill.ts_ms),
                    fill.symbol,
                    float(fill.qty),
                    float(fill.fill_price),
                    float(fill.fee),
                    float(fill.amount),
                ),
            )
            fill.id = cursor.lastrowid
            return int(fill.id)  # type: ignore[return-value]

    def save_position(self, event: PositionEvent) -> int:
        """写入一个持仓事件，回填 ``event.id`` 并返回。"""
        with self.conn:
            cursor = self.conn.execute(
                _INSERT_POSITION_SQL,
                (
                    int(event.ts_ms),
                    event.symbol,
                    event.event,
                    float(event.qty),
                    float(event.price),
                    float(event.realized_pnl),
                ),
            )
            event.id = cursor.lastrowid
            return int(event.id)  # type: ignore[return-value]

    def save_account(self, account: Account) -> None:
        """upsert 某市场账户当前态（现金/已实现盈亏）。"""
        with self.conn:
            self.conn.execute(
                _UPSERT_ACCOUNT_SQL,
                (account.market, float(account.cash), float(account.realized_pnl)),
            )

    def get_account(self, market: str) -> Account | None:
        """读取某市场账户当前态（无则 ``None``）。"""
        row = self.conn.execute(
            "SELECT market, cash, realized_pnl FROM accounts WHERE market = ?",
            (market,),
        ).fetchone()
        if row is None:
            return None
        return Account(market=str(row[0]), cash=float(row[1]), realized_pnl=float(row[2]))

    def update_position_current(
        self,
        symbol: str,
        market: str,
        qty: float,
        avg_cost: float,
        opened_ts: int,
    ) -> None:
        """upsert 某标的当前持仓态；``qty<=0`` 视为平仓并删除该行。"""
        with self.conn:
            if qty <= 0:
                self.conn.execute(
                    "DELETE FROM positions_current WHERE symbol = ?", (symbol,)
                )
                return
            self.conn.execute(
                _UPSERT_POSITION_CURRENT_SQL,
                (symbol, market, float(qty), float(avg_cost), int(opened_ts)),
            )

    def get_positions_current(self) -> list[dict]:
        """读取全部当前持仓态（供重启恢复 / 账户汇总）。"""
        rows = self.conn.execute(
            "SELECT symbol, market, qty, avg_cost, opened_ts FROM positions_current"
        ).fetchall()
        return [
            {
                "symbol": str(r[0]),
                "market": str(r[1]),
                "qty": float(r[2]),
                "avg_cost": float(r[3]),
                "opened_ts": int(r[4]),
            }
            for r in rows
        ]

    def query_signals(
        self,
        symbol: str | None = None,
        rule: str | None = None,
        start_ms: int | None = None,
        end_ms: int | None = None,
        limit: int | None = None,
    ) -> list[SignalRecord]:
        """按 标的/规则/时间范围 查询信号，按时间升序返回。

        参数:
            symbol/rule/start_ms/end_ms: 过滤条件（``None`` 不过滤）。
            limit: 最多返回条数（``None`` 不限）。
        """
        clauses: list[str] = []
        params: list[object] = []
        if symbol is not None:
            clauses.append("symbol = ?")
            params.append(symbol)
        if rule is not None:
            clauses.append("rule = ?")
            params.append(rule)
        if start_ms is not None:
            clauses.append("ts_ms >= ?")
            params.append(int(start_ms))
        if end_ms is not None:
            clauses.append("ts_ms <= ?")
            params.append(int(end_ms))

        sql = (
            "SELECT id, ts_ms, market, symbol, rule, direction, mode, "
            "status, trigger_value, order_id FROM signals"
        )
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY ts_ms ASC, id ASC"
        if limit is not None:
            sql += " LIMIT ?"
            params.append(int(limit))

        signals: list[SignalRecord] = []
        for row in self.conn.execute(sql, params).fetchall():
            signals.append(
                SignalRecord(
                    id=int(row[0]),
                    ts_ms=int(row[1]),
                    market=str(row[2]),
                    symbol=str(row[3]),
                    rule=str(row[4]),
                    direction=str(row[5]),
                    mode=str(row[6]),
                    status=str(row[7]),
                    trigger_value=float(row[8]),
                    order_id=int(row[9]) if row[9] is not None else None,
                )
            )
        return signals

    def query_trade_lifecycle(self, symbol: str) -> dict[str, list]:
        """按标的串联该标的一笔交易的全生命周期（signal→order→fill→position）。

        返回四类记录的列表（均按时间升序），其中 orders/fills 通过
        ``signal_id`` / ``order_id`` 与 signals 串联，便于复盘回放。
        """
        signals = self.query_signals(symbol=symbol)
        orders: list[Order] = []
        for row in self.conn.execute(
            "SELECT id, ts_ms, signal_id, symbol, side, qty, ref_price, status, reason "
            "FROM orders WHERE symbol = ? ORDER BY ts_ms ASC, id ASC",
            (symbol,),
        ).fetchall():
            orders.append(
                Order(
                    id=int(row[0]),
                    ts_ms=int(row[1]),
                    signal_id=int(row[2]) if row[2] is not None else None,
                    symbol=str(row[3]),
                    side=str(row[4]),
                    qty=float(row[5]),
                    ref_price=float(row[6]),
                    status=str(row[7]),
                    reason=str(row[8]) if row[8] is not None else None,
                )
            )
        fills: list[Fill] = []
        for row in self.conn.execute(
            "SELECT id, order_id, ts_ms, symbol, qty, fill_price, fee, amount "
            "FROM fills WHERE symbol = ? ORDER BY ts_ms ASC, id ASC",
            (symbol,),
        ).fetchall():
            fills.append(
                Fill(
                    id=int(row[0]),
                    order_id=int(row[1]),
                    ts_ms=int(row[2]),
                    symbol=str(row[3]),
                    qty=float(row[4]),
                    fill_price=float(row[5]),
                    fee=float(row[6]),
                    amount=float(row[7]),
                )
            )
        positions: list[PositionEvent] = []
        for row in self.conn.execute(
            "SELECT id, ts_ms, symbol, event, qty, price, realized_pnl "
            "FROM positions WHERE symbol = ? ORDER BY ts_ms ASC, id ASC",
            (symbol,),
        ).fetchall():
            positions.append(
                PositionEvent(
                    id=int(row[0]),
                    ts_ms=int(row[1]),
                    symbol=str(row[2]),
                    event=str(row[3]),
                    qty=float(row[4]),
                    price=float(row[5]),
                    realized_pnl=float(row[6]),
                )
            )
        return {
            "signals": signals,
            "orders": orders,
            "fills": fills,
            "positions": positions,
        }

    def account_summary(self, market: str) -> dict:
        """账户汇总：现金 / 已实现盈亏 / 未平仓数 / 持仓数量合计。

        用于 ``review --account`` 输出（持仓市值需外部行情价，此处不计算）。
        """
        account = self.get_account(market)
        cash = account.cash if account is not None else 0.0
        realized_pnl = account.realized_pnl if account is not None else 0.0
        rows = self.conn.execute(
            "SELECT COUNT(*), COALESCE(SUM(qty), 0) FROM positions_current WHERE market = ?",
            (market,),
        ).fetchone()
        open_positions = int(rows[0]) if rows is not None else 0
        position_qty = float(rows[1]) if rows is not None else 0.0
        return {
            "market": market,
            "cash": cash,
            "realized_pnl": realized_pnl,
            "open_positions": open_positions,
            "position_qty": position_qty,
        }
