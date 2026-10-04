# task26100599
## CampaignPulse 通知择时组件

`campaign_pulse.CampaignPulseScheduler`：静默窗（账户本地 21:00–08:00，含 21:00 不含 08:00，遵循夏令时）、
滚动限额（任意连续 168 小时左开右闭窗口内每订阅者最多 4 次尝试）与失败重试（1/5/30 分钟，至多 4 次）
共同约束下的内存态排程器。

```python
from datetime import datetime, timezone
from campaign_pulse import CampaignPulseScheduler

s = CampaignPulseScheduler(datetime(2026, 3, 2, 12, tzinfo=timezone.utc), "America/New_York")
s.enqueue("user-1", "msg-1", datetime(2026, 3, 2, 13, tzinfo=timezone.utc))
receipts = s.advance(
    datetime(2026, 3, 9, 12, tzinfo=timezone.utc),
    lambda message_id, attempt_no, at: "delivered",  # 或 "failed" / "bounced"
)
```

运行测试：`python3 -m pytest tests/ -q`
