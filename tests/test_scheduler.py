from datetime import datetime, timedelta, timezone

import pytest

from campaign_pulse import CampaignPulseScheduler

UTC = timezone.utc
TZ = "America/New_York"


def utc(y, mo, d, h, mi=0, s=0):
    return datetime(y, mo, d, h, mi, s, tzinfo=UTC)


def scripted_send(outcomes):
    it = iter(outcomes)
    calls = []

    def send(message_id, attempt_no, at):
        calls.append((message_id, attempt_no, at))
        return next(it)

    return send, calls


class TestStableOrdering:
    def test_same_instant_ordered_by_enqueue_seq(self):
        s = CampaignPulseScheduler(utc(2026, 3, 2, 12), TZ)
        s.enqueue("u1", "m1", utc(2026, 3, 2, 13))
        s.enqueue("u2", "m2", utc(2026, 3, 2, 13))
        s.enqueue("u1", "m3", utc(2026, 3, 2, 13))
        send, _ = scripted_send(["delivered"] * 3)
        recs = s.advance(utc(2026, 3, 3, 0), send)
        assert [r.message_id for r in recs] == ["m1", "m2", "m3"]
        assert all(r.at == utc(2026, 3, 2, 13) for r in recs)

    def test_retry_keeps_original_seq_for_ties(self):
        # m1 失败后在 t0+1min 重试；m2 基础时间同为 t0+1min，但 m1 序号更小
        s = CampaignPulseScheduler(utc(2026, 3, 2, 12), TZ)
        s.enqueue("u1", "m1", utc(2026, 3, 2, 13))
        s.enqueue("u2", "m2", utc(2026, 3, 2, 13, 1))
        send, _ = scripted_send(["failed", "delivered", "delivered"])
        recs = s.advance(utc(2026, 3, 3, 0), send)
        assert [(r.message_id, r.attempt_no) for r in recs] == [
            ("m1", 1), ("m1", 2), ("m2", 1),
        ]
        assert recs[1].at == utc(2026, 3, 2, 13, 1) == recs[2].at


class TestRetrySchedule:
    def test_retry_delays_and_fourth_failure_terminates(self):
        s = CampaignPulseScheduler(utc(2026, 3, 2, 12), TZ)
        s.enqueue("u1", "m1", utc(2026, 3, 2, 13))
        send, _ = scripted_send(["failed"] * 4)
        recs = s.advance(utc(2026, 3, 10, 0), send)
        assert [r.attempt_no for r in recs] == [1, 2, 3, 4]
        assert [r.at for r in recs] == [
            utc(2026, 3, 2, 13, 0),
            utc(2026, 3, 2, 13, 1),
            utc(2026, 3, 2, 13, 6),
            utc(2026, 3, 2, 13, 36),
        ]
        assert all(r.outcome == "failed" for r in recs)
        # 第4次失败后终止，继续推进无任何尝试
        recs2 = s.advance(utc(2026, 4, 1, 0), lambda *a: "delivered")
        assert recs2 == []


class TestBounceAndSuppress:
    def test_bounce_cancels_same_instant_tasks_but_not_other_subscribers(self):
        s = CampaignPulseScheduler(utc(2026, 3, 2, 12), TZ)
        s.enqueue("u1", "m1", utc(2026, 3, 2, 13))
        s.enqueue("u1", "m2", utc(2026, 3, 2, 13))
        s.enqueue("u2", "m3", utc(2026, 3, 2, 13))
        send, _ = scripted_send(["bounced", "delivered"])
        recs = s.advance(utc(2026, 3, 3, 0), send)
        assert [(r.message_id, r.outcome) for r in recs] == [
            ("m1", "bounced"),
            ("m3", "delivered"),
        ]
        # u1 被抑制：拒绝入队
        with pytest.raises(ValueError):
            s.enqueue("u1", "m4", utc(2026, 3, 3, 13))
        # u2 不受影响
        s.enqueue("u2", "m5", utc(2026, 3, 3, 13))
        send2, _ = scripted_send(["delivered"])
        recs2 = s.advance(utc(2026, 3, 4, 0), send2)
        assert [r.message_id for r in recs2] == ["m5"]

    def test_explicit_suppress_cancels_pending_and_retries(self):
        s = CampaignPulseScheduler(utc(2026, 3, 2, 12), TZ)
        s.enqueue("u1", "m1", utc(2026, 3, 2, 13))
        s.enqueue("u1", "m2", utc(2026, 3, 2, 13, 5))
        send, _ = scripted_send(["failed"])
        recs = s.advance(utc(2026, 3, 2, 13, 0, 30), send)
        assert [(r.message_id, r.outcome) for r in recs] == [("m1", "failed")]
        s.suppress("u1")  # 取消 m1 的待重试与 m2
        recs2 = s.advance(utc(2026, 3, 10, 0), lambda *a: "delivered")
        assert recs2 == []
        with pytest.raises(ValueError):
            s.enqueue("u1", "m3", utc(2026, 3, 10, 0))


class TestRollingQuota:
    def test_quota_release_competition_between_retry_and_new_message(self):
        t0 = utc(2026, 3, 2, 13)  # 周一 08:00 EST，合法
        s = CampaignPulseScheduler(utc(2026, 3, 2, 12), TZ)
        s.enqueue("u1", "m1", t0)
        s.enqueue("u1", "m2", t0)
        # m1 失败3次（t0, t0+1m, t0+6m），m2 在 t0 送达 -> 共用4次额度
        send, _ = scripted_send(["failed", "delivered", "failed", "failed"])
        recs = s.advance(t0 + timedelta(minutes=10), send)
        assert [(r.message_id, r.attempt_no) for r in recs] == [
            ("m1", 1), ("m2", 1), ("m1", 2), ("m1", 3),
        ]
        # 新消息 m3 在额度释放前就绪；m1 第4次重试基础时间 t0+36m 更早但被限额阻塞
        s.enqueue("u1", "m3", t0 + timedelta(hours=100))
        release = t0 + timedelta(hours=168)  # 恰好168小时前的尝试不占额度
        send2, _ = scripted_send(["delivered", "delivered"])
        recs2 = s.advance(release - timedelta(seconds=1), send2)
        assert recs2 == []  # 释放点之前一律阻塞
        send3, _ = scripted_send(["delivered", "delivered"])
        recs3 = s.advance(release, send3)
        # 同刻并列：重试沿用 m1 原始序号，先于 m3
        assert [(r.message_id, r.attempt_no, r.at) for r in recs3] == [
            ("m1", 4, release),
            ("m3", 1, release),
        ]

    def test_blocked_subscriber_does_not_block_others(self):
        t0 = utc(2026, 3, 2, 13)
        s = CampaignPulseScheduler(utc(2026, 3, 2, 12), TZ)
        s.enqueue("u1", "m1", t0)
        s.enqueue("u2", "m2", t0 + timedelta(hours=100))
        send, _ = scripted_send(["failed", "failed", "failed", "failed"])
        s.advance(t0 + timedelta(hours=1), send)  # u1 用满4次额度且消息终止
        s.enqueue("u1", "m3", t0 + timedelta(hours=100))
        send2, _ = scripted_send(["delivered"])
        recs = s.advance(t0 + timedelta(hours=100), send2)
        assert [(r.message_id, r.at) for r in recs] == [
            ("m2", t0 + timedelta(hours=100)),
        ]


class TestQuietHoursDST:
    def test_spring_forward_quiet_end_exact_utc(self):
        # 2026-03-08 02:00 EST -> 03:00 EDT；当天 08:00 EDT = 12:00 UTC
        s = CampaignPulseScheduler(utc(2026, 3, 7, 20), TZ)
        s.enqueue("u1", "m1", utc(2026, 3, 8, 2))  # 03-07 21:00 EST，静默开始（含）
        send, calls = scripted_send(["delivered"])
        recs = s.advance(utc(2026, 3, 9, 0), send)
        assert [r.at for r in recs] == [utc(2026, 3, 8, 12)]

    def test_fall_back_quiet_end_exact_utc(self):
        # 2026-11-01 02:00 EDT -> 01:00 EST；当天 08:00 EST = 13:00 UTC
        s = CampaignPulseScheduler(utc(2026, 10, 31, 20), TZ)
        s.enqueue("u1", "m1", utc(2026, 11, 1, 1))  # 10-31 21:00 EDT，静默开始（含）
        send, _ = scripted_send(["delivered"])
        recs = s.advance(utc(2026, 11, 2, 0), send)
        assert [r.at for r in recs] == [utc(2026, 11, 1, 13)]

    def test_quiet_end_and_quota_release_jointly(self):
        # 4次尝试集中在 03-05 01:30 UTC（03-04 20:30 EST，合法）。
        # 额度释放点 = 03-12 01:30 UTC = 03-11 21:30 EDT，落在静默窗内；
        # 静默结束 = 03-12 08:00 EDT = 03-12 12:00 UTC，二者共同决定投递时刻。
        t0 = utc(2026, 3, 5, 1, 30)
        s = CampaignPulseScheduler(utc(2026, 3, 4, 12), TZ)
        s.enqueue("u1", "m1", t0)
        s.enqueue("u1", "m2", t0)
        send, _ = scripted_send(["failed", "delivered", "failed", "failed"])
        recs = s.advance(t0 + timedelta(minutes=10), send)
        assert len(recs) == 4
        # m3 在 03-11 13:00 UTC（09:00 EDT，非静默）就绪，但仍受额度阻塞
        s.enqueue("u1", "m3", utc(2026, 3, 11, 13))
        send2, _ = scripted_send(["delivered", "delivered"])
        recs2 = s.advance(utc(2026, 3, 12, 11, 59, 59), send2)
        assert recs2 == []  # 额度未释放 + 静默窗，双重阻塞
        send3, _ = scripted_send(["delivered", "delivered"])
        recs3 = s.advance(utc(2026, 3, 12, 12), send3)
        assert [(r.message_id, r.attempt_no, r.at) for r in recs3] == [
            ("m1", 4, utc(2026, 3, 12, 12)),
            ("m3", 1, utc(2026, 3, 12, 12)),
        ]


class TestInputValidation:
    def test_duplicate_message_id_rejected(self):
        s = CampaignPulseScheduler(utc(2026, 3, 2, 12), TZ)
        s.enqueue("u1", "m1", utc(2026, 3, 2, 13))
        with pytest.raises(ValueError):
            s.enqueue("u2", "m1", utc(2026, 3, 2, 13))

    def test_not_before_clamped_to_clock(self):
        s = CampaignPulseScheduler(utc(2026, 3, 2, 13), TZ)
        s.enqueue("u1", "m1", utc(2026, 3, 1, 0))  # 早于当前时钟
        send, _ = scripted_send(["delivered"])
        recs = s.advance(utc(2026, 3, 3, 0), send)
        assert [r.at for r in recs] == [utc(2026, 3, 2, 13)]

    def test_advance_includes_until_endpoint(self):
        s = CampaignPulseScheduler(utc(2026, 3, 2, 12), TZ)
        s.enqueue("u1", "m1", utc(2026, 3, 2, 13))
        send, _ = scripted_send(["delivered"])
        recs = s.advance(utc(2026, 3, 2, 13), send)
        assert [r.at for r in recs] == [utc(2026, 3, 2, 13)]
