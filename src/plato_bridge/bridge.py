"""Platform bridge — multiplex messages across Telegram, Discord, fleet, MUD with transforms."""
import time
import hashlib
from dataclasses import dataclass, field
from typing import Optional, Callable
from collections import defaultdict
from enum import Enum

class Platform(Enum):
    TELEGRAM = "telegram"
    DISCORD = "discord"
    FLEET = "fleet"
    MUD = "mud"
    WEB = "web"
    MCP = "mcp"

class MessageDirection(Enum):
    INBOUND = "inbound"
    OUTBOUND = "outbound"

@dataclass
class BridgeMessage:
    id: str
    platform: Platform
    direction: MessageDirection
    content: str
    sender: str = ""
    room: str = ""
    reply_to: str = ""
    timestamp: float = field(default_factory=time.time)
    metadata: dict = field(default_factory=dict)
    transformed: bool = False

@dataclass
class PlatformConnection:
    platform: Platform
    connected: bool = False
    last_ping: float = 0.0
    messages_sent: int = 0
    messages_received: int = 0
    errors: int = 0

@dataclass
class TransformRule:
    name: str
    from_platforms: list[Platform] = field(default_factory=list)
    to_platforms: list[Platform] = field(default_factory=list)
    fn: Callable = None
    strip_mentions: bool = False
    max_length: int = 0
    prepend: str = ""
    append: str = ""

class PlatformBridge:
    def __init__(self):
        self._connections: dict[Platform, PlatformConnection] = {}
        self._transforms: list[TransformRule] = []
        self._message_log: list[BridgeMessage] = []
        self._handlers: dict[str, Callable] = {}
        self._room_map: dict[str, set[Platform]] = defaultdict(set)  # room → platforms
        self._stats = {"bridged": 0, "dropped": 0, "transformed": 0, "errors": 0}

    def connect(self, platform: Platform) -> PlatformConnection:
        conn = PlatformConnection(platform=platform, connected=True, last_ping=time.time())
        self._connections[platform] = conn
        return conn

    def disconnect(self, platform: Platform) -> bool:
        conn = self._connections.get(platform)
        if conn:
            conn.connected = False
            return True
        return False

    def add_transform(self, rule: TransformRule):
        self._transforms.append(rule)

    def register_handler(self, platform: Platform, fn: Callable):
        self._handlers[platform.value] = fn

    def bridge(self, message: BridgeMessage) -> list[BridgeMessage]:
        """Bridge a message from one platform to others."""
        if not self._connections.get(message.platform, PlatformConnection(message.platform)).connected:
            self._stats["dropped"] += 1
            return []
        # Apply transforms
        transformed = self._apply_transforms(message)
        if transformed.content != message.content:
            transformed.transformed = True
            self._stats["transformed"] += 1
        # Find target platforms
        targets = self._get_targets(message)
        results = []
        for target_platform in targets:
            conn = self._connections.get(target_platform)
            if not conn or not conn.connected:
                continue
            bridged = BridgeMessage(
                id=f"bridge-{message.id}-{target_platform.value}",
                platform=target_platform, direction=MessageDirection.OUTBOUND,
                content=transformed.content, sender=message.sender,
                room=message.room, reply_to=message.id,
                metadata={**transformed.metadata, "source_platform": message.platform.value,
                         "source_id": message.id}
            )
            # Send via handler
            handler = self._handlers.get(target_platform.value)
            if handler:
                try:
                    handler(bridged)
                    conn.messages_sent += 1
                except Exception as e:
                    conn.errors += 1
                    self._stats["errors"] += 1
                    continue
            results.append(bridged)
            self._stats["bridged"] += 1
        self._message_log.append(transformed)
        if message.direction == MessageDirection.INBOUND:
            conn = self._connections.get(message.platform)
            if conn:
                conn.messages_received += 1
        return results

    def _apply_transforms(self, message: BridgeMessage) -> BridgeMessage:
        result = message
        for rule in self._transforms:
            if rule.from_platforms and message.platform not in rule.from_platforms:
                continue
            if rule.strip_mentions:
                import re
                result = BridgeMessage(**{**result.__dict__,
                    "content": re.sub(r'@\w+', '', result.content).strip()})
            if rule.max_length and len(result.content) > rule.max_length:
                result = BridgeMessage(**{**result.__dict__,
                    "content": result.content[:rule.max_length] + "..."})
            if rule.prepend:
                result = BridgeMessage(**{**result.__dict__,
                    "content": rule.prepend + result.content})
            if rule.append:
                result = BridgeMessage(**{**result.__dict__,
                    "content": result.content + rule.append})
            if rule.fn:
                try:
                    new_content = rule.fn(result.content)
                    result = BridgeMessage(**{**result.__dict__, "content": new_content})
                except:
                    pass
        return result

    def _get_targets(self, message: BridgeMessage) -> list[Platform]:
        targets = set()
        # Room-based routing
        if message.room:
            targets.update(self._room_map.get(message.room, set()))
        # Default: bridge to all connected except source
        if not targets:
            targets = {p for p, c in self._connections.items()
                      if c.connected and p != message.platform}
        targets.discard(message.platform)
        return list(targets)

    def map_room(self, room: str, platforms: list[Platform]):
        self._room_map[room].update(platforms)

    def unmap_room(self, room: str, platform: Platform):
        self._room_map[room].discard(platform)

    def connections(self) -> dict[str, dict]:
        return {p.value: {"connected": c.connected, "sent": c.messages_sent,
                          "received": c.messages_received, "errors": c.errors,
                          "last_ping": c.last_ping}
                for p, c in self._connections.items()}

    def recent(self, n: int = 20) -> list[BridgeMessage]:
        return self._message_log[-n:]

    def ping(self, platform: Platform) -> bool:
        conn = self._connections.get(platform)
        if conn:
            conn.last_ping = time.time()
            return conn.connected
        return False

    @property
    def stats(self) -> dict:
        return {**self._stats, "connections": len([c for c in self._connections.values() if c.connected]),
                "transforms": len(self._transforms), "rooms_mapped": len(self._room_map)}
