import json
import random
import time
import uuid
from typing import List, Optional, Tuple

from .utils import peer_key, resolve_host


class GossipEngine:
    def __init__(self, node: "PeerNode"):
        self.node = node

    def mark_gossip_seen(self, gid: str) -> bool:
        now = time.time()
        self.node.gossip_cache = {k: v for k, v in self.node.gossip_cache.items() if now - v < 300}
        if gid in self.node.gossip_cache:
            return True
        self.node.gossip_cache[gid] = now
        return False

    def send_gossip(self, targets: List[Tuple[str, int]]):
        gid = str(uuid.uuid4())
        message = {
            "command": "GOSSIP",
            "host": self.node.peer_host,
            "port": self.node.peer_port,
            "name": self.node.peer_name,
            "id": gid,
            "cliPort": self.node.cli_port,
        }
        payload = json.dumps(message).encode()
        for host, port in targets:
            try:
                self.node.udp_socket.sendto(payload, (resolve_host(host), port))
            except OSError:
                continue

    def forward_gossip(self, msg: dict, exclude: Tuple[str, int]):
        peers = self.node.known_peers()
        random.shuffle(peers)
        peers = [p for p in peers if p != exclude]
        for host, port in peers[:5]:
            try:
                self.node.udp_socket.sendto(json.dumps(msg).encode(), (resolve_host(host), port))
            except OSError:
                continue

    def handle_gossip(self, msg: dict, addr: Tuple[str, int]):
        gid = msg.get("id", "")
        seen = self.mark_gossip_seen(gid)
        peer_name = msg.get("name")
        host = msg.get("host", addr[0])
        port = int(msg.get("port", addr[1]))
        key = peer_key(host, port)
        existing = key in self.node.peers
        self.node.add_peer(host, port, name=peer_name)
        if not seen:
            self.forward_gossip(msg, addr)
        if not existing or msg.get("command") == "GOSSIP":
            self.send_gossip_reply(addr[0], addr[1], peer_name)

    def send_gossip_reply(self, host: str, port: int, name: Optional[str]):
        reply = {
            "command": "GOSSIP_REPLY",
            "host": self.node.peer_host,
            "port": self.node.peer_port,
            "name": self.node.peer_name,
            "cliPort": self.node.cli_port,
            "id": str(uuid.uuid4()),
        }
        try:
            self.node.udp_socket.sendto(json.dumps(reply).encode(), (resolve_host(host), port))
        except OSError:
            pass


from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from .node import PeerNode
