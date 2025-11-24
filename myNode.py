import argparse
import json
import logging
import math
import random
import select
import socket
import sys
import time
import traceback
import uuid
from collections import Counter
from typing import Dict, List, Optional, Set, Tuple


logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(message)s")

WELL_KNOWN_PEERS = [
    ("hawk.cs.umanitoba.ca", 10000),
    ("falcon.cs.umanitoba.ca", 10000),
    ("cormorant.cs.umanitoba.ca", 10000),
    ("nuthatch.cs.umanitoba.ca", 10000),
]


def resolve_host(host: str) -> str:
    try:
        return socket.gethostbyname(host)
    except socket.gaierror:
        return host


def peer_key(host: str, port: int) -> str:
    return f"{host}:{port}"


class ConsensusState:
    def __init__(self, msg: dict):
        self.id: str = msg.get("id", str(uuid.uuid4()))
        self.omlevel: int = int(msg.get("omlevel", 0))
        self.initiator: str = msg.get("initiator", "")
        self.peers: List[str] = msg.get("peers", [])
        self.index: int = int(msg.get("index", 0))
        self.value: str = msg.get("value", "")
        self.parentid: Optional[str] = msg.get("parentid")
        self.reporter: Optional[str] = msg.get("reporter")
        self.reports: Dict[str, str] = {}
        self.resolved: Optional[str] = None
        self.subconsensus_launched: Set[str] = set()

    def expected_participants(self) -> Set[str]:
        return set(self.peers)

    def record_report(self, reporter: str, value: str):
        self.reports[reporter] = value

    def is_complete(self) -> bool:
        expected = self.expected_participants()
        return expected.issubset(set(self.reports.keys()))

    def decide(self) -> Optional[str]:
        if self.resolved is not None:
            return self.resolved
        if not self.reports:
            return None
        counts = Counter(self.reports.values())
        best_count = max(counts.values())
        winners = [v for v, c in counts.items() if c == best_count]
        self.resolved = sorted(winners)[0]
        return self.resolved


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

        self.cli_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.cli_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.cli_socket.bind(("", 0))
        self.cli_socket.listen(5)
        self.cli_port = self.cli_socket.getsockname()[1]

        self.peers: Dict[str, dict] = {}
        self.gossip_cache: Dict[str, float] = {}
        self.consensus_map: Dict[str, ConsensusState] = {}

        self.word_list: List[str] = ["" for _ in range(5)]
        self.lie_mode: bool = False
        self.lie_rate: float = 1.0

        self.cli_clients: List[socket.socket] = []
        self.cli_buffers: Dict[socket.socket, str] = {}

        self.last_heartbeat = 0.0
        self.last_cleanup = 0.0

    # -----------------------------------------------------
    # Utility helpers
    # -----------------------------------------------------
    def add_peer(self, host: str, port: int, name: Optional[str] = None):
        key = peer_key(host, port)
        if key not in self.peers:
            self.peers[key] = {"host": host, "port": port, "name": name or key, "last_seen": time.time()}
            logging.info("Added peer %s", key)
        else:
            self.peers[key]["last_seen"] = time.time()
            if name:
                self.peers[key]["name"] = name
        return key

    def known_peers(self) -> List[Tuple[str, int]]:
        return [(info["host"], info["port"]) for info in self.peers.values()]

    def mark_gossip_seen(self, gid: str) -> bool:
        now = time.time()
        self.gossip_cache = {k: v for k, v in self.gossip_cache.items() if now - v < 300}
        if gid in self.gossip_cache:
            return True
        self.gossip_cache[gid] = now
        return False

    def choose_value(self, honest_value: str) -> str:
        if not self.lie_mode:
            return honest_value
        if random.random() <= self.lie_rate:
            alt = random.choice(["alpha", "bravo", "charlie", "delta", "echo"])
            return alt
        return honest_value

    # -----------------------------------------------------
    # Gossip handling
    # -----------------------------------------------------
    def send_gossip(self, targets: List[Tuple[str, int]]):
        gid = str(uuid.uuid4())
        message = {
            "command": "GOSSIP",
            "host": self.peer_host,
            "port": self.peer_port,
            "name": socket.gethostname(),
            "id": gid,
            "cliPort": self.cli_port,
        }
        payload = json.dumps(message).encode()
        for host, port in targets:
            try:
                self.udp_socket.sendto(payload, (resolve_host(host), port))
            except OSError:
                continue

    def forward_gossip(self, msg: dict, exclude: Tuple[str, int]):
        peers = self.known_peers()
        random.shuffle(peers)
        peers = [p for p in peers if p != exclude]
        for host, port in peers[:5]:
            try:
                self.udp_socket.sendto(json.dumps(msg).encode(), (resolve_host(host), port))
            except OSError:
                continue

    def handle_gossip(self, msg: dict, addr: Tuple[str, int]):
        gid = msg.get("id", "")
        seen = self.mark_gossip_seen(gid)
        peer_name = msg.get("name")
        existing = peer_key(addr[0], addr[1]) in self.peers
        self.add_peer(addr[0], addr[1], name=peer_name)
        if not seen:
            self.forward_gossip(msg, addr)
        # Reply if new peer
        if not existing or msg.get("command") == "GOSSIP":
            self.send_gossip_reply(addr[0], addr[1], peer_name)

    def send_gossip_reply(self, host: str, port: int, name: Optional[str]):
        reply = {
            "command": "GOSSIP_REPLY",
            "host": self.peer_host,
            "port": self.peer_port,
            "name": socket.gethostname() if name is None else name,
            "cliPort": self.cli_port,
            "id": str(uuid.uuid4()),
        }
        try:
            self.udp_socket.sendto(json.dumps(reply).encode(), (resolve_host(host), port))
        except OSError:
            pass

    # -----------------------------------------------------
    # Consensus handling
    # -----------------------------------------------------
    def get_consensus(self, msg: dict) -> ConsensusState:
        cid = msg.get("id")
        if cid not in self.consensus_map:
            self.consensus_map[cid] = ConsensusState(msg)
        return self.consensus_map[cid]

    def propagate_result_upwards(self, cid: str, reporter: str, value: str):
        consensus = self.consensus_map.get(cid)
        if not consensus:
            return
        consensus.record_report(reporter, value)
        if consensus.is_complete():
            result = consensus.decide()
            if result is None:
                return
            if consensus.parentid:
                parent_reporter = consensus.reporter or reporter or consensus.initiator
                parent = self.consensus_map.get(consensus.parentid)
                if parent:
                    self.propagate_result_upwards(consensus.parentid, parent_reporter, result)
            else:
                if 0 <= consensus.index < len(self.word_list):
                    self.word_list[consensus.index] = result
                    logging.info("Consensus %s complete. Index %d set to '%s'", cid, consensus.index, result)

    def launch_subconsensus(self, parent_msg: dict, sender_key: str, received_value: str):
        parent = self.get_consensus(parent_msg)
        if sender_key in parent.subconsensus_launched or parent.omlevel <= 0:
            return
        parent.subconsensus_launched.add(sender_key)
        peers = [p for p in parent.peers if p != sender_key]
        if not peers:
            return
        new_id = str(uuid.uuid4())
        sub_msg = {
            "command": "CONSENSUS",
            "id": new_id,
            "omlevel": max(parent.omlevel - 1, 0),
            "initiator": parent.initiator,
            "peers": peers,
            "index": parent.index,
            "value": self.choose_value(received_value),
            "parentid": parent.id,
            "reporter": sender_key,
        }
        self.consensus_map[new_id] = ConsensusState(sub_msg)
        for peer in peers:
            host, port = peer.split(":")
            try:
                self.udp_socket.sendto(json.dumps(sub_msg).encode(), (resolve_host(host), int(port)))
            except OSError:
                continue
        # We also record our own report in the parent using what we forwarded
        self.propagate_result_upwards(parent.id, peer_key(self.peer_host, self.peer_port), sub_msg["value"])

    def handle_consensus(self, msg: dict, addr: Tuple[str, int]):
        sender = peer_key(addr[0], addr[1])
        self.add_peer(addr[0], addr[1], name=msg.get("initiator"))
        consensus = self.get_consensus(msg)
        value_received = msg.get("value", "")
        consensus.record_report(sender, value_received)
        if consensus.omlevel > 0:
            self.launch_subconsensus(msg, sender, value_received)
        else:
            self.propagate_result_upwards(consensus.id, sender, value_received)

    def start_root_consensus(self, index: int, value: str):
        peers = list({peer_key(self.peer_host, self.peer_port)} | set(self.peers.keys()))
        peer_count = len(peers)
        if peer_count == 0:
            logging.info("No peers available for consensus.")
            return
        m = math.floor((peer_count - 1) / 3)
        cid = str(uuid.uuid4())
        value_to_send = self.choose_value(value)
        msg = {
            "command": "CONSENSUS",
            "id": cid,
            "omlevel": m,
            "initiator": peer_key(self.peer_host, self.peer_port),
            "peers": peers,
            "index": index,
            "value": value_to_send,
            "parentid": None,
        }
        state = ConsensusState(msg)
        self.consensus_map[cid] = state
        # initiator can commit immediately
        if 0 <= index < len(self.word_list):
            self.word_list[index] = value_to_send
        # record own report
        state.record_report(peer_key(self.peer_host, self.peer_port), value_to_send)
        payload = json.dumps(msg).encode()
        for peer in peers:
            if peer == peer_key(self.peer_host, self.peer_port):
                continue
            host, port = peer.split(":")
            try:
                self.udp_socket.sendto(payload, (resolve_host(host), int(port)))
            except OSError:
                continue
        logging.info("Started consensus %s at index %d with value '%s' and m=%d", cid, index, value_to_send, m)

    # -----------------------------------------------------
    # CLI handling
    # -----------------------------------------------------
    def handle_cli_message(self, client: socket.socket, line: str):
        parts = line.strip().split()
        if not parts:
            return
        cmd = parts[0].lower()
        if cmd == "peers":
            resp_lines = []
            for info in self.peers.values():
                delta = time.time() - info.get("last_seen", 0)
                resp_lines.append(f"{info['host']}:{info['port']} (name={info.get('name','')}, last_seen={delta:.1f}s)")
            if not resp_lines:
                resp_lines = ["No peers known."]
            client.sendall(("\n".join(resp_lines) + "\n").encode())
        elif cmd == "current":
            words = ", ".join(f"[{i}] {w}" for i, w in enumerate(self.word_list))
            client.sendall((words + "\n").encode())
        elif cmd == "consensus" and len(parts) >= 3:
            try:
                idx = int(parts[1])
            except ValueError:
                client.sendall(b"Invalid index.\n")
                return
            value = " ".join(parts[2:])
            self.start_root_consensus(idx, value)
            client.sendall(b"Consensus started.\n")
        elif cmd == "lie":
            rate = 1.0
            if len(parts) > 1:
                try:
                    rate = max(0.0, min(1.0, float(parts[1]) / 100.0))
                except ValueError:
                    pass
            self.lie_mode = True
            self.lie_rate = rate
            client.sendall(f"Lying enabled at rate {self.lie_rate*100:.0f}%.\n".encode())
        elif cmd == "truth":
            self.lie_mode = False
            client.sendall(b"Lying disabled.\n")
        elif cmd == "exit":
            client.sendall(b"Goodbye.\n")
            client.close()
            if client in self.cli_clients:
                self.cli_clients.remove(client)
            self.cli_buffers.pop(client, None)
        else:
            client.sendall(b"Unknown command.\n")

    # -----------------------------------------------------
    # Main loop
    # -----------------------------------------------------
    def announce_to_well_known(self):
        self.send_gossip(WELL_KNOWN_PEERS)

    def cleanup_peers(self):
        now = time.time()
        stale = [k for k, v in self.peers.items() if now - v.get("last_seen", 0) > 120]
        for key in stale:
            logging.info("Removing stale peer %s", key)
            self.peers.pop(key, None)

    def heartbeat(self):
        peers = self.known_peers()
        random.shuffle(peers)
        self.send_gossip(peers[:5])

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
                    try:
                        data, addr = sock.recvfrom(4096)
                        msg = json.loads(data.decode())
                        if msg.get("command") == "GOSSIP":
                            self.handle_gossip(msg, addr)
                        elif msg.get("command") == "GOSSIP_REPLY":
                            self.add_peer(addr[0], addr[1], name=msg.get("name"))
                        elif msg.get("command") == "CONSENSUS":
                            self.handle_consensus(msg, addr)
                    except Exception:
                        traceback.print_exc()
                elif sock is self.cli_socket:
                    client, _ = self.cli_socket.accept()
                    client.setblocking(False)
                    self.cli_clients.append(client)
                    self.cli_buffers[client] = ""
                    client.sendall(b"Welcome to the OM node CLI. Commands: peers, current, consensus <idx> <word>, lie [pct], truth, exit\n")
                else:
                    try:
                        chunk = sock.recv(4096)
                        if not chunk:
                            sock.close()
                            self.cli_clients.remove(sock)
                            self.cli_buffers.pop(sock, None)
                            continue
                        buffer = self.cli_buffers.get(sock, "") + chunk.decode()
                        while "\n" in buffer:
                            line, buffer = buffer.split("\n", 1)
                            self.handle_cli_message(sock, line)
                        self.cli_buffers[sock] = buffer
                    except Exception:
                        traceback.print_exc()
                        sock.close()
                        if sock in self.cli_clients:
                            self.cli_clients.remove(sock)
                        self.cli_buffers.pop(sock, None)


def parse_args(argv: List[str]):
    parser = argparse.ArgumentParser(description="Oral Messaging peer node")
    parser.add_argument("port", nargs="?", type=int, help="UDP peer port (optional)")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv if argv is not None else sys.argv[1:])
    node = PeerNode(args.port)
    try:
        node.run()
    except KeyboardInterrupt:
        logging.info("Shutting down...")
    finally:
        for sock in [node.udp_socket, node.cli_socket] + node.cli_clients:
            try:
                sock.close()
            except Exception:
                pass


if __name__ == "__main__":
    main()
