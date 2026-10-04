"""CampaignPulse notification timing scheduler.

Serial, in-memory scheduling of delivery attempts under quiet hours,
a rolling attempt quota, and retry backoff constraints.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, time, timedelta, timezone
from typing import Callable, Literal
from zoneinfo import ZoneInfo

SendResult = Literal["delivered", "failed", "bounced"]
SendFn = Callable[[str, int, datetime], SendResult]

MAX_ATTEMPTS_PER_WINDOW = 4
QUOTA_WINDOW = timedelta(hours=168)
RETRY_BACKOFFS = (timedelta(minutes=1), timedelta(minutes=5), timedelta(minutes=30))
MAX_ATTEMPTS_PER_MESSAGE = 1 + len(RETRY_BACKOFFS)
QUIET_START_HOUR = 21
QUIET_END_HOUR = 8


@dataclass(frozen=True)
class Receipt:
    """Record of one executed delivery attempt."""

    subscriber_id: str
    message_id: str
    attempt_no: int
    at: datetime
    result: SendResult


@dataclass
class _Message:
    subscriber_id: str
    message_id: str
    seq: int
    base_time: datetime
    attempt_no: int = 1


@dataclass
class _SubscriberState:
    suppressed: bool = False
    attempts: list[datetime] = field(default_factory=list)
    pending: list[_Message] = field(default_factory=list)


class Scheduler:
    """Schedules notification attempts for subscribers."""

    def __init__(self, now: datetime, account_tz: str) -> None:
        self._now = _as_utc(now)
        self._tz = ZoneInfo(account_tz)
        self._subscribers: dict[str, _SubscriberState] = {}
        self._message_ids: set[str] = set()
        self._next_seq = 0

    def enqueue(self, subscriber_id: str, message_id: str, not_before: datetime) -> int:
        not_before = _as_utc(not_before)
        if message_id in self._message_ids:
            raise ValueError(f"duplicate message_id: {message_id!r}")
        state = self._subscribers.setdefault(subscriber_id, _SubscriberState())
        if state.suppressed:
            raise ValueError(f"subscriber suppressed: {subscriber_id!r}")
        self._message_ids.add(message_id)
        seq = self._next_seq
        self._next_seq += 1
        base = max(not_before, self._now)
        state.pending.append(_Message(subscriber_id, message_id, seq, base))
        return seq

    def suppress(self, subscriber_id: str) -> None:
        state = self._subscribers.setdefault(subscriber_id, _SubscriberState())
        state.suppressed = True
        state.pending.clear()

    def advance(self, until: datetime, send: SendFn) -> list[Receipt]:
        until = _as_utc(until)
        if until < self._now:
            raise ValueError("clock only moves forward")
        receipts: list[Receipt] = []
        while True:
            best: tuple[datetime, int, str, _Message] | None = None
            for sid, state in self._subscribers.items():
                if state.suppressed:
                    continue
                for msg in state.pending:
                    at = self._earliest_legal(state, max(self._now, msg.base_time))
                    if at <= until and (best is None or (at, msg.seq) < (best[0], best[1])):
                        best = (at, msg.seq, sid, msg)
            if best is None:
                break
            at, _seq, sid, msg = best
            self._now = at
            state = self._subscribers[sid]
            result = send(msg.message_id, msg.attempt_no, at)
            receipts.append(Receipt(sid, msg.message_id, msg.attempt_no, at, result))
            state.attempts.append(at)
            state.pending.remove(msg)
            if result == "delivered":
                pass
            elif result == "bounced":
                self.suppress(sid)
            elif msg.attempt_no < MAX_ATTEMPTS_PER_MESSAGE:
                backoff = RETRY_BACKOFFS[msg.attempt_no - 1]
                state.pending.append(
                    _Message(sid, msg.message_id, msg.seq, at + backoff, msg.attempt_no + 1)
                )
        self._now = until
        return receipts

    def _earliest_legal(self, state: _SubscriberState, t: datetime) -> datetime:
        t = _quota_earliest(state.attempts, t)
        quiet_end = self._quiet_end(t)
        if quiet_end is not None:
            t = quiet_end
        return t

    def _quiet_end(self, t: datetime) -> datetime | None:
        """Return the UTC end of the quiet window containing t, or None."""
        local = t.astimezone(self._tz)
        if local.hour >= QUIET_START_HOUR:
            end_date = local.date() + timedelta(days=1)
        elif local.hour < QUIET_END_HOUR:
            end_date = local.date()
        else:
            return None
        end_local = datetime.combine(end_date, time(QUIET_END_HOUR), tzinfo=self._tz)
        return end_local.astimezone(timezone.utc)


def _quota_earliest(attempts: list[datetime], t: datetime) -> datetime:
    """Earliest moment >= t at which the rolling 168h quota has a free slot."""
    while True:
        window = [a for a in attempts if t - a < QUOTA_WINDOW]
        if len(window) < MAX_ATTEMPTS_PER_WINDOW:
            return t
        t = min(window) + QUOTA_WINDOW


def _as_utc(moment: datetime) -> datetime:
    if moment.tzinfo is None:
        raise ValueError("datetimes must be timezone-aware UTC")
    return moment.astimezone(timezone.utc)
