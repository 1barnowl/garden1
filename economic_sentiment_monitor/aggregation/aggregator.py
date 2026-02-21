"""Core signal aggregation engine: rolling windows, weighting, decay."""
class SignalAggregator:
    WINDOWS = ["1m", "15m", "1h", "4h", "1d", "7d", "30d"]
    def aggregate(self, signals: list, key: dict, window: str) -> dict: ...
