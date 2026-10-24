"""告警管理器：冷却去重 / JSONL 落盘 / SQLite 持久化 / CSV+JSON 导出。

策略（对应设计 3.1）：
- 同币种同规则冷却（``cooldown_sec``）内重复触发直接丢弃，返回 ``False``；
- 发布成功路径：追加 JSONL → SQLite → 推送 CLI 滚动日志（后两步失败
  降级为记日志，不阻断行情主链）；
- 导出优先取 SQLite（结构化、有序），存储不可用时回退解析 JSONL。
"""

from __future__ import annotations

import csv
import io
import json
import logging
import time
from collections.abc import Callable
from pathlib import Path
from typing import Optional

from quant.models import Alert
from quant.storage.sqlite_store import Storage
from quant.utils.timeutils import datetime_to_ms, to_iso

logger = logging.getLogger("quant.alerts.manager")

#: JSONL/CSV/JSON 共用的告警字段顺序
_ALERT_FIELDS = ("ts", "symbol", "rule", "message", "trigger_value", "threshold", "level")

#: 冷却键类型：(symbol, rule)
_CooldownKey = tuple[str, str]


class AlertManager:
    """告警管理器：冷却去重 + 多通道输出 + 导出。"""

    def __init__(
        self,
        cooldown_sec: float,
        log_path: str | Path,
        store: Optional[Storage] = None,
        notify: Optional[Callable[[Alert], None]] = None,
        clock: Optional[Callable[[], float]] = None,
    ) -> None:
        """初始化告警管理器。

        参数:
            cooldown_sec: 同币种同规则冷却秒数（0 表示不冷却）。
            log_path: 告警 JSONL 文件路径（父目录不存在时自动创建）。
            store: SQLite 存储（可选；不可用时降级为仅 JSONL）。
            notify: 告警发布成功后的回调（如 ``DashboardState.push_alert``）。
            clock: 单调时钟（测试注入假时钟验证冷却窗口）。
        """
        self.cooldown_sec = float(cooldown_sec)
        self.log_path = Path(log_path)
        self.store = store
        self.notify = notify
        self.cooldowns: dict[_CooldownKey, float] = {}
        self._clock = clock if clock is not None else time.monotonic
        self.log_path.parent.mkdir(parents=True, exist_ok=True)

    def publish(self, alert: Alert) -> bool:
        """发布一条告警。

        返回:
            ``True`` 表示已发布；``False`` 表示处于冷却期被丢弃。
        """
        key: _CooldownKey = (alert.symbol, alert.rule)
        now = self._clock()
        deadline = self.cooldowns.get(key)
        if deadline is not None and now < deadline:
            logger.debug(
                "告警处于冷却期被丢弃：%s %s（剩余 %.1fs）",
                alert.symbol,
                alert.rule,
                deadline - now,
            )
            return False
        self.cooldowns[key] = now + self.cooldown_sec

        self._append_jsonl(alert)
        self._save_to_store(alert)
        self._notify_cli(alert)
        return True

    def export_csv(self, path: str | Path) -> int:
        """导出全部告警为 CSV，返回导出条数。"""
        alerts = self._load_all_alerts()
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        buffer = io.StringIO()
        writer = csv.DictWriter(buffer, fieldnames=list(_ALERT_FIELDS))
        writer.writeheader()
        for alert in alerts:
            writer.writerow(self._to_row(alert))
        target.write_text(buffer.getvalue(), encoding="utf-8-sig")
        logger.info("告警导出 CSV：%s（%d 条）", target, len(alerts))
        return len(alerts)

    def export_json(self, path: str | Path) -> int:
        """导出全部告警为 JSON，返回导出条数。"""
        alerts = self._load_all_alerts()
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        payload = [self._to_row(alert) for alert in alerts]
        target.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        logger.info("告警导出 JSON：%s（%d 条）", target, len(alerts))
        return len(alerts)

    # ------------------------------------------------------------------
    # 内部实现
    # ------------------------------------------------------------------

    def _append_jsonl(self, alert: Alert) -> None:
        """追加一条告警到 JSONL（失败仅记警告，不抛异常）。"""
        try:
            with self.log_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(self._to_row(alert), ensure_ascii=False) + "\n")
        except OSError as exc:
            logger.warning("告警 JSONL 写入失败（降级继续）：%s", exc)

    def _save_to_store(self, alert: Alert) -> None:
        """写入 SQLite（失败仅记警告，不抛异常）。"""
        if self.store is None:
            return
        try:
            self.store.save_alert(alert)
        except Exception as exc:  # sqlite3.Error 及其他：降级不阻断主链
            logger.warning("告警入库失败（降级继续）：%s", exc)

    def _notify_cli(self, alert: Alert) -> None:
        """推送 CLI 滚动日志（回调失败仅记警告，不抛异常）。"""
        if self.notify is None:
            return
        try:
            self.notify(alert)
        except Exception as exc:
            logger.warning("告警推送 CLI 失败（降级继续）：%s", exc)

    def _load_all_alerts(self) -> list[Alert]:
        """加载全部告警：优先 SQLite，回退 JSONL。"""
        if self.store is not None:
            try:
                return self.store.query_alerts()
            except Exception as exc:
                logger.warning("从 SQLite 读取告警失败，回退 JSONL：%s", exc)
        return self._load_from_jsonl()

    def _load_from_jsonl(self) -> list[Alert]:
        """从 JSONL 解析全部告警（坏行跳过）。"""
        alerts: list[Alert] = []
        if not self.log_path.is_file():
            return alerts
        for line in self.log_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
                alerts.append(self._from_row(row))
            except (json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
                logger.warning("JSONL 坏行跳过：%s", exc)
        return alerts

    @staticmethod
    def _to_row(alert: Alert) -> dict:
        """Alert → 可序列化行（ts 转 ISO 8601 UTC）。"""
        return {
            "ts": to_iso(datetime_to_ms(alert.ts)),
            "symbol": alert.symbol,
            "rule": alert.rule,
            "message": alert.message,
            "trigger_value": alert.trigger_value,
            "threshold": alert.threshold,
            "level": alert.level,
        }

    @staticmethod
    def _from_row(row: dict) -> Alert:
        """可序列化行 → Alert（ts 解析 ISO 8601）。"""
        from datetime import datetime

        return Alert(
            ts=datetime.fromisoformat(str(row["ts"]).replace("Z", "+00:00")),
            symbol=str(row["symbol"]),
            rule=str(row["rule"]),
            message=str(row["message"]),
            trigger_value=float(row["trigger_value"]),
            threshold=float(row["threshold"]),
            level=str(row["level"]),
        )
