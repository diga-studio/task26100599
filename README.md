# task26100599

CampaignPulse 通知择时组件：在静默窗、滚动限额与重试退避的共同约束下排程投递尝试。
纯内存、串行调用、无外部服务依赖。

## 用法

```python
from datetime import datetime, timezone
from campaignpulse import Scheduler

sched = Scheduler(datetime(2026, 1, 5, 14, 0, tzinfo=timezone.utc), "America/New_York")
sched.enqueue("subscriber-1", "message-1", datetime(2026, 1, 5, 15, 0, tzinfo=timezone.utc))

def send(message_id: str, attempt_no: int, at: datetime) -> str:
    ...  # 同步本地发送，返回 "delivered" / "failed" / "bounced"

receipts = sched.advance(datetime(2026, 1, 6, 0, 0, tzinfo=timezone.utc), send)
```

- `enqueue(subscriber_id, message_id, not_before)`：入队，返回递增入队序号；消息 ID 全局唯一。
- `suppress(subscriber_id)`：永久抑制订阅者，取消其全部待发与待重试项，此后拒绝其入队。
- `advance(until, send)`：推进时钟至 `until`（含端点），按应发生时刻执行所有到期尝试并返回回执列表。

## 规则

- 同一订阅者任意连续 168 小时（UTC 时长）左开右闭窗口内最多 4 次尝试。
- 账户本地时间 21:00（含）至次日 08:00（不含）禁止尝试，遵循实际夏令时。
- `failed` 后最多追加 3 次重试，退避依次为 1、5、30 分钟；第 4 次失败终止。
- `bounced` 结束该消息并立即抑制该订阅者（含同刻尚未开始的尝试）。
- 每一步为各待尝试项求最早合法时刻，选最早者；并列按原始入队序号。

## 测试

```bash
python3 -m pytest tests/ -q
```
