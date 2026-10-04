"""CampaignPulse 通知择时组件：静默窗、滚动限额与重试共同约束下的排程。"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable
from zoneinfo import ZoneInfo

SendFn = Callable[[str, int, datetime], str]

QUIET_START_HOUR = 21
QUIET_END_HOUR = 8
WINDOW = timedelta(hours=168)
MAX_ATTEMPTS_IN_WINDOW = 4
RETRY_DELAYS = (timedelta(minutes=1), timedelta(minutes=5), timedelta(minutes=30))
MAX_ATTEMPTS = 1 + len(RETRY_DELAYS)

_VALID_OUTCOMES = ("delivered", "failed", "bounced")


@dataclass(frozen=True)
class Receipt:
    message_id: str
    subscriber_id: str
    attempt_no: int
    at: datetime
    outcome: str


@dataclass
class _Message:
    subscriber_id: str
    message_id: str
    seq: int
    base_time: datetime
    attempt_no: int


def _as_utc(moment: datetime) -> datetime:
    if moment.tzinfo is None:
        raise ValueError("时刻必须带时区信息（UTC）")
    return moment.astimezone(timezone.utc)


class CampaignPulseScheduler:
    """串行调用的内存态通知排程器。

    约束：账户本地 21:00（含）至次日 08:00（不含）静默；
    任意连续 168 小时（左开右闭）内同一订阅者最多 4 次尝试；
    failed 后按 1/5/30 分钟追加至多 3 次重试；bounced 即抑制订阅者。
    """

    def __init__(self, now: datetime, account_tz: str) -> None:
        self._now = _as_utc(now)
        self._tz = ZoneInfo(account_tz)
        self._attempts: dict[str, list[datetime]] = {}
        self._pending: list[_Message] = []
        self._suppressed: set[str] = set()
        self._message_ids: set[str] = set()
        self._next_seq = 0

    def enqueue(self, subscriber_id: str, message_id: str, not_before: datetime) -> None:
        if subscriber_id in self._suppressed:
            raise ValueError(f"订阅者已被抑制: {subscriber_id}")
        if message_id in self._message_ids:
            raise ValueError(f"消息ID已存在: {message_id}")
        base = max(_as_utc(not_before), self._now)
        self._message_ids.add(message_id)
        self._pending.append(
            _Message(subscriber_id, message_id, self._next_seq, base, attempt_no=1)
        )
        self._next_seq += 1

    def suppress(self, subscriber_id: str) -> None:
        self._suppressed.add(subscriber_id)
        self._pending = [m for m in self._pending if m.subscriber_id != subscriber_id]

    def advance(self, until: datetime, send: SendFn) -> list[Receipt]:
        until = _as_utc(until)
        if until < self._now:
            raise ValueError("时钟不能倒退")
        receipts: list[Receipt] = []
        while True:
            best: tuple[datetime, int, _Message] | None = None
            for msg in self._pending:
                at = self._earliest_legal(msg)
                if at > until:
                    continue
                key = (at, msg.seq)
                if best is None or key < (best[0], best[1]):
                    best = (at, msg.seq, msg)
            if best is None:
                break
            at, _, msg = best
            self._now = at
            outcome = send(msg.message_id, msg.attempt_no, at)
            if outcome not in _VALID_OUTCOMES:
                raise ValueError(f"非法发送结果: {outcome}")
            self._attempts.setdefault(msg.subscriber_id, []).append(at)
            receipts.append(Receipt(msg.message_id, msg.subscriber_id, msg.attempt_no, at, outcome))
            self._pending.remove(msg)
            if outcome == "delivered":
                pass
            elif outcome == "bounced":
                self.suppress(msg.subscriber_id)
            elif msg.attempt_no < MAX_ATTEMPTS:
                delay = RETRY_DELAYS[msg.attempt_no - 1]
                self._pending.append(
                    _Message(msg.subscriber_id, msg.message_id, msg.seq,
                             at + delay, msg.attempt_no + 1)
                )
        self._now = until
        return receipts

    def _earliest_legal(self, msg: _Message) -> datetime:
        at = max(self._now, msg.base_time)
        while True:
            quieted = self._skip_quiet(at)
            windowed = self._skip_window(msg.subscriber_id, quieted)
            if windowed == at:
                return at
            at = windowed

    def _skip_quiet(self, at: datetime) -> datetime:
        local = at.astimezone(self._tz)
        if QUIET_START_HOUR <= local.hour or local.hour < QUIET_END_HOUR:
            day = local.date()
            if local.hour >= QUIET_START_HOUR:
                day += timedelta(days=1)
            local_open = datetime(day.year, day.month, day.day, QUIET_END_HOUR, tzinfo=self._tz)
            return local_open.astimezone(timezone.utc)
        return at

    def _skip_window(self, subscriber_id: str, at: datetime) -> datetime:
        history = self._attempts.get(subscriber_id)
        if not history:
            return at
        recent = [t for t in history if t <= at]
        if len(recent) < MAX_ATTEMPTS_IN_WINDOW:
            return at
        fourth_newest = sorted(recent)[-MAX_ATTEMPTS_IN_WINDOW]
        return max(at, fourth_newest + WINDOW)
