import time
from collections import deque

from core.snapshot_bus import MeterQuality, MeterSnapshot
from utils.health import meter_is_fresh


class TrendBuffer:
    def __init__(self, maxlen=600):
        self.t = deque(maxlen=maxlen)
        self.y = deque(maxlen=maxlen)
        self._version = 0

    def push(self, ts: float, val: float):
        self.t.append(ts)
        self.y.append(val)
        self._version += 1

    @property
    def version(self) -> int:
        return self._version

class MeterState:
    def __init__(self, meter_id: int, slave_id: int):
        self.meter_id = meter_id
        self.slave_id = slave_id

        # config-driven
        self.enabled = True
        self.display = False
        self.include_in_total = True
        # Plant TOTAL aggregation sign.
        # +1 = ADD to total (import/load meters)
        # -1 = SUBTRACT from total (export/solar feeder meters)
        # Used by TotalAggregator for signed totals.
        self.total_sign = 1.0
        self.priority = False
        self.name = f"Meter {meter_id}"
        self.serial_no = ""
        self.scan_interval_sec = 1
        self.quality: MeterQuality = MeterQuality.DISABLED
        self.last_error = ""

        # Device model — selects which Modbus driver to use
        # See core/device_drivers.py: DEVICE_MODELS for valid values
        self.model: str = "selec_mfm384"

        # CT/PT ratio scaling (applied after decode, before logging/display)
        # 1.0 = no scaling (raw meter reading used as-is)
        self.ct_ratio: float = 1.0   # e.g. 200/5 = 40.0 if meter secondary is 5A
        self.pt_ratio: float = 1.0   # e.g. 11000/110 = 100.0 if meter secondary is 110V

        # runtime
        self.first_data_received = False
        self.data_valid = False
        self.comm_failure = False
        self.last_good_ts = None
        self.last_good_mon = None  # monotonic time of last good sample (duration-safe)
        self.values = {}
        self.raw_regs = None
        self.timeout_sec = 1.0
        self.retry_count = 1

        # comm diagnostics / protection against silent per-meter lockups
        self.poll_count = 0
        self.ok_count = 0
        self.fail_count = 0
        self.consecutive_fail = 0
        self.last_poll_ts = 0.0
        self.last_fail_ts = 0.0
        self.suspend_until_ts = 0.0  # per-meter backoff time (epoch seconds)

        # trend
        self.trend_kw = TrendBuffer(maxlen=1200)

    def age(self):
        """Seconds since last GOOD sample (duration-safe).

        Uses time.monotonic() to avoid Windows clock jumps.
        Falls back to wall clock if monotonic marker is missing.
        """
        if not self.last_good_ts:
            return None
        if self.last_good_mon is not None:
            try:
                return max(0.0, time.monotonic() - float(self.last_good_mon))
            except Exception:
                pass
        return max(0.0, time.time() - float(self.last_good_ts))

    def update_good(self, values: dict, raw_regs=None):
        self.values = values
        self.raw_regs = raw_regs
        self.first_data_received = True
        self.data_valid = True
        self.comm_failure = False
        self.last_good_ts = time.time()
        self.last_good_mon = time.monotonic()
        self.last_error = ""
        self.quality = MeterQuality.GOOD

        # diagnostics
        self.poll_count += 1
        self.ok_count += 1
        self.consecutive_fail = 0
        self.suspend_until_ts = 0.0

        kw = values.get("kW")
        if isinstance(kw, (int, float)):
            self.trend_kw.push(self.last_good_ts, float(kw))

    def mark_fail(self):
        # diagnostics
        self.poll_count += 1
        self.fail_count += 1
        self.consecutive_fail += 1
        self.last_fail_ts = time.time()
        self.comm_failure = True
        self.data_valid = False
        self.quality = MeterQuality.COMM_LOST

    def apply_snapshot(self, snapshot: MeterSnapshot) -> None:
        self.raw_regs = snapshot.raw_regs
        if snapshot.quality == MeterQuality.GOOD:
            self.values = snapshot.values or {}
            self.first_data_received = True
            self.data_valid = True
            self.last_good_ts = snapshot.ts
            self.last_good_mon = float(getattr(snapshot, "monotonic_ts", 0.0) or time.monotonic())
            self.comm_failure = False
            self.quality = MeterQuality.GOOD
            self.last_error = ""
        else:
            # For SCADA-grade behavior, keep last known values but
            # propagate quality explicitly (STALE/COMM_LOST/DISABLED).
            if snapshot.values:
                self.values = snapshot.values or {}
                self.first_data_received = True

            self.comm_failure = snapshot.quality == MeterQuality.COMM_LOST
            self.data_valid = False
            self.last_error = getattr(snapshot, "quality_reason", "") or snapshot.error or ""
            self.quality = snapshot.quality

    def refresh_quality(self, stale_sec: float) -> MeterQuality:
        if not self.enabled:
            self.quality = MeterQuality.DISABLED
            return self.quality
        if self.comm_failure and getattr(self, "data_valid", False) is False:
            self.quality = MeterQuality.COMM_LOST
            return self.quality
        if meter_is_fresh(self, stale_sec):
            self.quality = MeterQuality.GOOD
            return self.quality
        self.quality = MeterQuality.STALE
        return self.quality
