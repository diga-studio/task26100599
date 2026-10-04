"""Tests for the CampaignPulse notification timing scheduler."""

from datetime import datetime, timedelta, timezone

import pytest

from campaignpulse import Scheduler

UTC = timezone.utc
NY = "America/New_York"


def utc(iso: str) -> datetime:
    return datetime.fromisoformat(iso).replace(tzinfo=UTC)


def send_all(result):
    def send(message_id, attempt_no, at):
        return result
    return send


def test_basic_enqueue_and_deliver() -> None:
    now = utc("2026-01-05T14:00:00")  # 09:00 EST, outside quiet hours
    sched = Scheduler(now, NY)
    sched.enqueue("u1", "m1", now)
    receipts = sched.advance(now + timedelta(hours=1), send_all("delivered"))
    assert len(receipts) == 1
    r = receipts[0]
    assert (r.subscriber_id, r.message_id, r.attempt_no) == ("u1", "m1", 1)
    assert r.at == now
    assert r.result == "delivered"


def test_not_before_clamped_to_clock() -> None:
    now = utc("2026-01-05T14:00:00")
    sched = Scheduler(now, NY)
    sched.enqueue("u1", "m1", now - timedelta(hours=2))
    receipts = sched.advance(now, send_all("delivered"))
    assert [r.at for r in receipts] == [now]


def test_not_before_defers_attempt() -> None:
    now = utc("2026-01-05T14:00:00")
    sched = Scheduler(now, NY)
    later = now + timedelta(hours=2)
    sched.enqueue("u1", "m1", later)
    assert sched.advance(now + timedelta(hours=1), send_all("delivered")) == []
    receipts = sched.advance(later, send_all("delivered"))
    assert [r.at for r in receipts] == [later]


def test_quiet_hours_start_inclusive() -> None:
    # 21:00 EST == 02:00 UTC next day; quiet window includes 21:00 local.
    now = utc("2026-01-05T14:00:00")
    sched = Scheduler(now, NY)
    sched.enqueue("u1", "m1", utc("2026-01-06T01:59:59"))  # 20:59:59 EST -> ok
    sched.enqueue("u2", "m2", utc("2026-01-06T02:00:00"))  # 21:00 EST -> quiet
    receipts = sched.advance(utc("2026-01-07T00:00:00"), send_all("delivered"))
    by_id = {r.message_id: r.at for r in receipts}
    assert by_id["m1"] == utc("2026-01-06T01:59:59")
    # Pushed to next local 08:00 EST == 13:00 UTC.
    assert by_id["m2"] == utc("2026-01-06T13:00:00")


def test_quiet_hours_end_exclusive() -> None:
    now = utc("2026-01-05T14:00:00")
    sched = Scheduler(now, NY)
    sched.enqueue("u1", "m1", utc("2026-01-06T12:59:59"))  # 07:59:59 EST -> quiet
    sched.enqueue("u2", "m2", utc("2026-01-06T13:00:00"))  # 08:00 EST -> ok
    receipts = sched.advance(utc("2026-01-06T14:00:00"), send_all("delivered"))
    by_id = {r.message_id: r.at for r in receipts}
    assert by_id["m1"] == utc("2026-01-06T13:00:00")
    assert by_id["m2"] == utc("2026-01-06T13:00:00")


def test_retry_backoffs_and_fourth_failure_terminates() -> None:
    now = utc("2026-01-05T14:00:00")  # 09:00 EST
    sched = Scheduler(now, NY)
    sched.enqueue("u1", "m1", now)
    receipts = sched.advance(now + timedelta(hours=2), send_all("failed"))
    assert [r.attempt_no for r in receipts] == [1, 2, 3, 4]
    assert [r.at for r in receipts] == [
        now,
        now + timedelta(minutes=1),
        now + timedelta(minutes=1 + 5),
        now + timedelta(minutes=1 + 5 + 30),
    ]
    # Message is finished: no further attempts on a later advance.
    assert sched.advance(now + timedelta(days=2), send_all("failed")) == []


def test_quota_allows_four_attempts_then_blocks() -> None:
    now = utc("2026-01-05T14:00:00")  # 09:00 EST
    sched = Scheduler(now, NY)
    for i in range(1, 6):
        sched.enqueue("u1", f"m{i}", now)
    receipts = sched.advance(now + timedelta(hours=1), send_all("delivered"))
    # Only 4 attempts fit in the rolling 168h window; order is stable by seq.
    assert [r.message_id for r in receipts] == ["m1", "m2", "m3", "m4"]
    assert all(r.at == now for r in receipts)


def test_quota_releases_exactly_after_168h() -> None:
    now = utc("2026-01-05T14:00:00")
    sched = Scheduler(now, NY)
    for i in range(1, 6):
        sched.enqueue("u1", f"m{i}", now)
    sched.advance(now + timedelta(hours=1), send_all("delivered"))
    release = now + timedelta(hours=168)  # 2026-01-12 09:00 EST, not quiet
    # One microsecond before release the quota is still exhausted.
    assert sched.advance(release - timedelta(microseconds=1), send_all("delivered")) == []
    # An attempt exactly 168h old no longer counts (left-open window).
    receipts = sched.advance(release, send_all("delivered"))
    assert [(r.message_id, r.at) for r in receipts] == [("m5", release)]


def test_retry_and_new_message_compete_at_quota_release() -> None:
    now = utc("2026-01-05T14:00:00")  # 09:00 EST
    sched = Scheduler(now, NY)
    # Three attempts at t0, delivered: quota has one slot left.
    for i in range(1, 4):
        sched.enqueue("u1", f"m{i}", now)
    # m4 fails one hour later, filling the quota; its retry is due at +1min
    # but must wait for the first quota release at t0 + 168h.
    sched.enqueue("u1", "m4", now + timedelta(hours=1))
    # m5 is also blocked by the exhausted quota until the same release moment.
    sched.enqueue("u1", "m5", now + timedelta(hours=2))

    def send(message_id, attempt_no, at):
        return "failed" if message_id == "m4" else "delivered"

    sched.advance(now + timedelta(hours=2), send)
    release = now + timedelta(hours=168)
    receipts = sched.advance(release, send)
    # At the exact release moment the retry (seq 4) wins the tie over the
    # newer message (seq 5); both fire at the same UTC instant.
    assert [(r.message_id, r.attempt_no) for r in receipts] == [("m4", 2), ("m5", 1)]
    assert all(r.at == release for r in receipts)


def test_bounce_suppresses_and_cancels_same_instant_work() -> None:
    now = utc("2026-01-05T14:00:00")
    sched = Scheduler(now, NY)
    sched.enqueue("u1", "m1", now)
    sched.enqueue("u1", "m2", now)  # same instant, must be cancelled by bounce
    sched.enqueue("u2", "m3", now)  # other subscriber unaffected

    def send(message_id, attempt_no, at):
        return "bounced" if message_id == "m1" else "delivered"

    receipts = sched.advance(now + timedelta(hours=1), send)
    assert [(r.message_id, r.result) for r in receipts] == [
        ("m1", "bounced"),
        ("m3", "delivered"),
    ]
    # Suppressed subscriber can never enqueue again.
    with pytest.raises(ValueError):
        sched.enqueue("u1", "m4", now)
    # u2 keeps working afterwards.
    sched.enqueue("u2", "m5", now)
    receipts = sched.advance(now + timedelta(hours=2), send_all("delivered"))
    assert [r.message_id for r in receipts] == ["m5"]


def test_blocked_subscriber_does_not_block_others() -> None:
    now = utc("2026-01-05T14:00:00")
    sched = Scheduler(now, NY)
    sched.enqueue("u1", "m1", now + timedelta(days=10))  # blocked far ahead
    sched.enqueue("u2", "m2", now)
    receipts = sched.advance(now + timedelta(hours=1), send_all("delivered"))
    assert [(r.subscriber_id, r.message_id) for r in receipts] == [("u2", "m2")]


def test_duplicate_message_id_rejected() -> None:
    now = utc("2026-01-05T14:00:00")
    sched = Scheduler(now, NY)
    sched.enqueue("u1", "m1", now)
    with pytest.raises(ValueError):
        sched.enqueue("u2", "m1", now)


def test_explicit_suppress_cancels_pending() -> None:
    now = utc("2026-01-05T14:00:00")
    sched = Scheduler(now, NY)
    sched.enqueue("u1", "m1", now)
    sched.suppress("u1")
    assert sched.advance(now + timedelta(hours=1), send_all("delivered")) == []
    with pytest.raises(ValueError):
        sched.enqueue("u1", "m2", now)


# --- DST scenarios for America/New_York (2026) ---
# Spring forward: 2026-03-08 02:00 EST -> 03:00 EDT (07:00 UTC).
# Fall back:     2026-11-01 02:00 EDT -> 01:00 EST (06:00 UTC).


def test_dst_spring_forward_quiet_end() -> None:
    # Quiet window of local 2026-03-07 21:00 EST ends at local 08:00 EDT,
    # which is 12:00 UTC (not 13:00 UTC) because of the spring transition.
    now = utc("2026-03-07T14:00:00")  # 09:00 EST
    sched = Scheduler(now, NY)
    sched.enqueue("u1", "m1", utc("2026-03-08T02:00:00"))  # 21:00 EST -> quiet
    receipts = sched.advance(utc("2026-03-09T00:00:00"), send_all("delivered"))
    assert [r.at for r in receipts] == [utc("2026-03-08T12:00:00")]


def test_dst_fall_back_quiet_end() -> None:
    # Quiet window of local 2026-10-31 21:00 EDT ends at local 08:00 EST,
    # which is 13:00 UTC (not 12:00 UTC) because of the fall transition.
    now = utc("2026-10-31T14:00:00")  # 10:00 EDT
    sched = Scheduler(now, NY)
    sched.enqueue("u1", "m1", utc("2026-11-01T01:00:00"))  # 21:00 EDT -> quiet
    receipts = sched.advance(utc("2026-11-02T00:00:00"), send_all("delivered"))
    assert [r.at for r in receipts] == [utc("2026-11-01T13:00:00")]


def test_dst_spring_quota_release_is_utc_duration() -> None:
    # 168h is a pure UTC duration, not 7 local calendar days: after the
    # spring transition the release lands at 09:00 local, still 13:00 UTC.
    t0 = utc("2026-03-07T13:00:00")  # 08:00 EST, quiet window just ended
    sched = Scheduler(t0, NY)
    for i in range(1, 6):
        sched.enqueue("u1", f"m{i}", t0)
    sched.advance(t0 + timedelta(hours=1), send_all("delivered"))
    release = t0 + timedelta(hours=168)  # 2026-03-14T13:00Z == 09:00 EDT
    assert sched.advance(release - timedelta(microseconds=1), send_all("delivered")) == []
    receipts = sched.advance(release, send_all("delivered"))
    assert [(r.message_id, r.at) for r in receipts] == [("m5", release)]


def test_dst_fall_quota_release_inside_quiet_window() -> None:
    # Quota release and quiet-hours end jointly determine the delivery:
    # the 168h UTC release lands at 07:00 local EST (quiet), so the
    # message waits for the 08:00 EST quiet-window end at 13:00 UTC.
    t0 = utc("2026-10-31T12:00:00")  # 08:00 EDT, quiet window just ended
    sched = Scheduler(t0, NY)
    for i in range(1, 6):
        sched.enqueue("u1", f"m{i}", t0)
    sched.advance(t0 + timedelta(hours=1), send_all("delivered"))
    release = t0 + timedelta(hours=168)  # 2026-11-07T12:00Z == 07:00 EST (quiet)
    quiet_end = utc("2026-11-07T13:00:00")  # 08:00 EST
    # Quota has released but the quiet window still blocks the attempt.
    assert sched.advance(release, send_all("delivered")) == []
    receipts = sched.advance(quiet_end, send_all("delivered"))
    assert [(r.message_id, r.at) for r in receipts] == [("m5", quiet_end)]
