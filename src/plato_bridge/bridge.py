"""Cross-platform messaging bridge."""
import time, hashlib
from dataclasses import dataclass, field
from typing import Optional
from enum import Enum

class BridgePlatform(Enum):
    TELEGRAM = "telegram"
    DISCORD = "discord"
    FLEET = "fleet"
    MUD = "mud"
    PLATO = "plato"

@dataclass
class BridgeMessage:
    content: str
    source: BridgePlatform
    sender: str = ""
    room: str = ""
    priority: str = "P2"
    timestamp: float = field(default_factory=time.time)
    id: str = field(default_factory=lambda: hashlib.md5(str(time.time()).encode()).hexdigest()[:12])

@dataclass
class BridgeConfig:
    name: str
    source: BridgePlatform
    target: BridgePlatform
    filter_priority: str = "P2"
    filter_rooms: list[str] = field(default_factory=list)
    enabled: bool = True

class MessageBridge:
    def __init__(self):
        self._configs: list[BridgeConfig] = []
        self._queue: list[BridgeMessage] = []
        self._history: list[BridgeMessage] = []

    def add_config(self, name: str, source: str, target: str, **kwargs) -> BridgeConfig:
        cfg = BridgeConfig(name=name, source=BridgePlatform(source),
                           target=BridgePlatform(target), **kwargs)
        self._configs.append(cfg)
        return cfg

    def route(self, message: BridgeMessage) -> list[BridgeConfig]:
        matches = []
        for cfg in self._configs:
            if not cfg.enabled: continue
            if cfg.source != message.source: continue
            if cfg.filter_rooms and message.room not in cfg.filter_rooms: continue
            matches.append(cfg)
        return matches

    def send(self, content: str, source: str, sender: str = "", room: str = "", priority: str = "P2") -> BridgeMessage:
        msg = BridgeMessage(content=content, source=BridgePlatform(source),
                           sender=sender, room=room, priority=priority)
        routes = self.route(msg)
        for r in routes:
            self._queue.append(msg)
            self._history.append(msg)
        return msg

    def drain(self, target: str = "", limit: int = 50) -> list[BridgeMessage]:
        msgs = self._queue[:limit]
        self._queue = self._queue[limit:]
        if target:
            msgs = [m for m in msgs if any(c.target.value == target for c in self.route(m))]
        return msgs

    def stats(self) -> dict:
        platforms = {}
        for m in self._history:
            p = m.source.value
            platforms[p] = platforms.get(p, 0) + 1
        return {"configs": len(self._configs), "queued": len(self._queue),
                "history": len(self._history), "platforms": platforms}
