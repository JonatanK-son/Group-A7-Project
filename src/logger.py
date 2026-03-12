"""Structured JSON logger with a context-manager timer."""
import json
import logging
import time
from datetime import datetime, timezone


class StructuredLogger:
    def __init__(self, name: str):
        self._log = logging.getLogger(name)
        if not self._log.handlers:
            h = logging.StreamHandler()
            h.setFormatter(logging.Formatter("%(message)s"))
            self._log.addHandler(h)
        self._log.setLevel(logging.INFO)

    def _emit(self, level: str, event: str, **kw):
        payload = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": level.upper(),
            "event": event,
            **kw,
        }
        getattr(self._log, level)(json.dumps(payload))

    def info(self, event: str, **kw):    self._emit("info",    event, **kw)
    def warning(self, event: str, **kw): self._emit("warning", event, **kw)
    def error(self, event: str, **kw):   self._emit("error",   event, **kw)

    def timer(self, event: str) -> "_Timer":
        return _Timer(self, event)


class _Timer:
    def __init__(self, logger: StructuredLogger, event: str):
        self.logger = logger
        self.event  = event
        self.elapsed: float = 0.0

    def __enter__(self):
        self._t0 = time.time()
        self.logger.info(f"{self.event}_started")
        return self

    def __exit__(self, *_):
        self.elapsed = round(time.time() - self._t0, 2)
        self.logger.info(f"{self.event}_completed", elapsed_s=self.elapsed)
