import json
import logging
import random
import select
import socket
import time
import traceback
from typing import Dict, List, Optional, Tuple

from .cli import CliHandler
from .config import WELL_KNOWN_PEERS
from .consensus import ConsensusEngine
from .gossip import GossipEngine
from .utils import peer_key, resolve_host


class PeerNode:
    def __init__(self, peer_port: Optional[int] = None):
        self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.udp_socket.bind(("", peer_port if peer_port is not None else 0))
        _, self.peer_port = self.udp_socket.getsockname()
        try:
            self.peer_host = socket.gethostbyname(socket.gethostname())
        except socket.gaierror:
            self.peer_host = "127.0.0.1"
        self.peer_name = "Nakamichi Dragon"

        self.cli_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.cli_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.cli_socket.bind(("", 0))
        self.cli_socket.listen(5)
        self.cli_port = self.cli_socket.getsockname()[1]

        self.peers: Dict[str, dict] = {}
        self.gossip_cache: Dict[str, float] = {}

        self.word_list: List[str] = ["" for _ in range(5)]
        self.lie_mode: bool = False
        self.lie_rate: float = 1.0

        self.cli_clients: List[socket.socket] = []
        self.cli_buffers: Dict[socket.socket, str] = {}

        self.last_heartbeat = 0.0
        self.last_cleanup = 0.0

        self.consensus = ConsensusEngine(self)
        self.gossip = GossipEngine(self)
        self.cli_handler = CliHandler(self)

    # Peer helpers ---------------------------------------------------
    def add_peer(self, host: str, port: int, name: Optional[str] = None):
        resolved_host = resolve_host(host)
        key = peer_key(resolved_host, port)
        if key not in self.peers:
            self.peers[key] = {"host": resolved_host, "port": port, "name": name or key, "last_seen": time.time()}
            logging.info("Added peer %s", key)
        else:
            self.peers[key]["last_seen"] = time.time()
            if name:
                self.peers[key]["name"] = name
        return key

    def known_peers(self) -> List[Tuple[str, int]]:
        return [(info["host"], info["port"]) for info in self.peers.values()]

    # Loop utilities -------------------------------------------------
    def announce_to_well_known(self):
        self.gossip.send_gossip(WELL_KNOWN_PEERS)

    def cleanup_peers(self):
        now = time.time()
        stale = [k for k, v in self.peers.items() if now - v.get("last_seen", 0) > 120]
        for key in stale:
            logging.info("Removing stale peer %s", key)
            self.peers.pop(key, None)

    def heartbeat(self):
        peers = self.known_peers()
        random_peers = list(peers)
        random.shuffle(random_peers)
        self.gossip.send_gossip(random_peers[:5])

    # Main loop ------------------------------------------------------
    def run(self):
        logging.info("Peer listening on UDP %s:%d", self.peer_host, self.peer_port)
        logging.info("CLI listening on TCP port %d", self.cli_port)
        self.announce_to_well_known()
        while True:
            now = time.time()
            timeout = 1.0
            if now - self.last_cleanup > 5:
                self.cleanup_peers()
                self.last_cleanup = now
            if now - self.last_heartbeat > 60:
                self.heartbeat()
                self.last_heartbeat = now
            sockets = [self.udp_socket, self.cli_socket] + self.cli_clients
            readable, _, _ = select.select(sockets, [], [], timeout)
            for sock in readable:
                if sock is self.udp_socket:
                    self._handle_udp()
                elif sock is self.cli_socket:
                    self._accept_cli_client()
                else:
                    self._handle_cli_socket(sock)

    # Internal handlers ---------------------------------------------
    def _handle_udp(self):
        try:
            data, addr = self.udp_socket.recvfrom(4096)
            msg = json.loads(data.decode())
            command = msg.get("command")
            if command == "GOSSIP":
                self.gossip.handle_gossip(msg, addr)
            elif command == "GOSSIP_REPLY":
                self.add_peer(addr[0], addr[1], name=msg.get("name"))
            elif command == "CONSENSUS":
                self.consensus.handle_consensus(msg, addr)
        except Exception:
            traceback.print_exc()

    def _accept_cli_client(self):
        client, _ = self.cli_socket.accept()
        client.setblocking(False)
        self.cli_clients.append(client)
        self.cli_buffers[client] = ""
        client.sendall(
            b"Welcome to the OM node CLI. Commands: peers, current, consensus <idx> <word>, lie [pct], truth, exit\n"
        )

    def _handle_cli_socket(self, sock: socket.socket):
        try:
            chunk = sock.recv(4096)
            if not chunk:
                sock.close()
                if sock in self.cli_clients:
                    self.cli_clients.remove(sock)
                self.cli_buffers.pop(sock, None)
                return
            buffer = self.cli_buffers.get(sock, "") + chunk.decode()
            while "\n" in buffer:
                line, buffer = buffer.split("\n", 1)
                self.cli_handler.handle_cli_message(sock, line)
            self.cli_buffers[sock] = buffer
        except Exception:
            traceback.print_exc()
            sock.close()
            if sock in self.cli_clients:
                self.cli_clients.remove(sock)
            self.cli_buffers.pop(sock, None)
