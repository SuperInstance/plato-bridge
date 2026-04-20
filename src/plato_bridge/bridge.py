"""Cross-platform messaging bridge — routing, priorities, dedup, delivery tracking."""
import time
import hashlib
from dataclasses import dataclass, field
from typing import Optional
from enum import Enum

class BridgePlatform(Enum):
    TELEGRAM = "telegram"
    DISCORD = "discord"
    FLEET = "fleet"
    MUD = "mud"
    PLATO = "plato"
    WEBHOOK = "webhook"

class DeliveryStatus(Enum):
    QUEUED = "queued"
    DELIVERED = "delivered"
    FAILED = "failed"
    EXPIRED = "expired"
    DEDUPED = "deduped"

@dataclass
class BridgeMessage:
    content: str
    source: BridgePlatform
    sender: str = ""
    room: str = ""
    priority: int = 2  # 1=critical, 2=high, 3=normal, 4=low
    ttl: float = 3600.0  # time to live in seconds
    timestamp: float = field(default_factory=time.time)
    id: str = field(default_factory=lambda: hashlib.md5(str(time.time()).encode()).hexdigest()[:12])
    status: DeliveryStatus = DeliveryStatus.QUEUED

@dataclass
class BridgeConfig:
    name: str
    source: BridgePlatform
    target: BridgePlatform
    filter_priority: int = 4  # only route messages with priority <= this
    filter_rooms: list[str] = field(default_factory=list)
    filter_senders: list[str] = field(default_factory=list)
    enabled: bool = True
    dedup_window: float = 30.0  # dedup messages within this window

class MessageBridge:
    def __init__(self):
        self._configs: list[BridgeConfig] = []
        self._queue: list[BridgeMessage] = []
        self._history: list[BridgeMessage] = []
        self._dedup_cache: dict[str, float] = {}  # content_hash -> timestamp
        self._delivery_log: list[dict] = []
        self._platform_stats: dict[str, dict] = {}

    def add_config(self, name: str, source: str, target: str, **kwargs) -> BridgeConfig:
        cfg = BridgeConfig(name=name, source=BridgePlatform(source),
                          target=BridgePlatform(target), **kwargs)
        self._configs.append(cfg)
        return cfg

    def route(self, message: BridgeMessage) -> list[BridgeConfig]:
        matches = []
        for cfg in self._configs:
            if not cfg.enabled:
                continue
            if cfg.source != message.source:
                continue
            if message.priority > cfg.filter_priority:
                continue
            if cfg.filter_rooms and message.room not in cfg.filter_rooms:
                continue
            if cfg.filter_senders and message.sender not in cfg.filter_senders:
                continue
            matches.append(cfg)
        return matches

    def send(self, content: str, source: str, sender: str = "", room: str = "",
             priority: int = 2, ttl: float = 3600.0) -> BridgeMessage:
        msg = BridgeMessage(content=content, source=BridgePlatform(source),
                           sender=sender, room=room, priority=priority, ttl=ttl)
        # Dedup check
        content_hash = hashlib.md5(content.encode()).hexdigest()
        last_seen = self._dedup_cache.get(content_hash, 0)
        if time.time() - last_seen < 30.0:  # global 30s dedup
            msg.status = DeliveryStatus.DEDUPED
            self._history.append(msg)
            return msg
        self._dedup_cache[content_hash] = time.time()
        # Clean old dedup entries
        now = time.time()
        self._dedup_cache = {k: v for k, v in self._dedup_cache.items() if now - v < 300}

        routes = self.route(msg)
        for r in routes:
            msg.status = DeliveryStatus.DELIVERED
            msg.delivered_at = time.time()
            self._queue.append(msg)
            self._history.append(msg)
            self._delivery_log.append({"msg_id": msg.id, "config": r.name,
                                       "source": msg.source.value, "target": r.target.value,
                                       "timestamp": time.time()})
            # Track platform stats
            platform = r.target.value
            if platform not in self._platform_stats:
                self._platform_stats[platform] = {"sent": 0, "failed": 0, "last_sent": 0}
            self._platform_stats[platform]["sent"] += 1
            self._platform_stats[platform]["last_sent"] = time.time()
        if not routes:
            msg.status = DeliveryStatus.FAILED
            self._history.append(msg)
        return msg

    def drain(self, target: str = "", limit: int = 50) -> list[BridgeMessage]:
        msgs = self._queue[:limit]
        self._queue = self._queue[limit:]
        if target:
            msgs = [m for m in msgs if any(c.target.value == target for c in self.route(m))]
        return msgs

    def expire_old(self) -> int:
        now = time.time()
        before = len(self._queue)
        self._queue = [m for m in self._queue if now - m.timestamp < m.ttl]
        return before - len(self._queue)

    def delivery_history(self, limit: int = 50) -> list[dict]:
        return self._delivery_log[-limit:]

    def stats(self) -> dict:
        sources = {}
        for m in self._history:
            p = m.source.value
            sources[p] = sources.get(p, 0) + 1
        return {"configs": len(self._configs), "queued": len(self._queue),
                "history": len(self._history), "platforms": sources,
                "platform_stats": self._platform_stats,
                "dedup_cache": len(self._dedup_cache)}
