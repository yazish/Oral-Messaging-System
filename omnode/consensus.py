import json
import logging
import math
import random
import uuid
from typing import Dict, List, Tuple

from .consensus_state import ConsensusState
from .utils import peer_key, resolve_host


class ConsensusEngine:
    def __init__(self, node: "PeerNode"):
        self.node = node
        self.consensus_map: Dict[str, ConsensusState] = {}

    # Value selection -------------------------------------------------
    def choose_value(self, honest_value: str) -> str:
        if not self.node.lie_mode:
            return honest_value
        if random.random() <= self.node.lie_rate:
            return "faulty_attack"
        return honest_value

    # Consensus state helpers ----------------------------------------
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
                if 0 <= consensus.index < len(self.node.word_list):
                    self.node.word_list[consensus.index] = result
                    logging.info(
                        "Consensus %s complete. Index %d set to '%s'",
                        cid,
                        consensus.index,
                        result,
                    )

    def launch_subconsensus(self, parent_msg: dict, sender_key: str, received_value: str):
        parent = self.get_consensus(parent_msg)
        if sender_key in parent.subconsensus_launched or parent.omlevel <= 0:
            return
        parent.subconsensus_launched.add(sender_key)
        peers = [p for p in parent.peers if p != sender_key]
        if not peers:
            return
        new_id = str(uuid.uuid4())
        self_value = self.choose_value(received_value)
        sub_msg = {
            "command": "CONSENSUS",
            "id": new_id,
            "omlevel": max(parent.omlevel - 1, 0),
            "initiator": parent.initiator,
            "peers": peers,
            "index": parent.index,
            "value": self_value,
            "parentid": parent.id,
            "reporter": sender_key,
            "default_value": parent.default_value,
        }
        self.consensus_map[new_id] = ConsensusState(sub_msg)
        for peer in peers:
            host, port = peer.split(":")
            peer_value = self.choose_value(received_value)
            peer_msg = dict(sub_msg)
            peer_msg["value"] = peer_value
            try:
                self.node.udp_socket.sendto(json.dumps(peer_msg).encode(), (resolve_host(host), int(port)))
            except OSError:
                continue
        self.propagate_result_upwards(parent.id, peer_key(self.node.peer_host, self.node.peer_port), self_value)

    def handle_consensus(self, msg: dict, addr: Tuple[str, int]):
        sender = peer_key(addr[0], addr[1])
        self.node.add_peer(addr[0], addr[1], name=msg.get("initiator"))
        consensus = self.get_consensus(msg)
        value_received = msg.get("value", "")
        consensus.record_report(sender, value_received)
        if consensus.omlevel > 0:
            self.launch_subconsensus(msg, sender, value_received)
        else:
            self.propagate_result_upwards(consensus.id, sender, value_received)

    def start_root_consensus(self, index: int, value: str):
        peers = list({peer_key(self.node.peer_host, self.node.peer_port)} | set(self.node.peers.keys()))
        peer_count = len(peers)
        if peer_count == 0:
            logging.info("No peers available for consensus.")
            return
        m = math.floor((peer_count - 1) / 3)
        cid = str(uuid.uuid4())
        self_value = self.choose_value(value)
        msg = {
            "command": "CONSENSUS",
            "id": cid,
            "omlevel": m,
            "initiator": peer_key(self.node.peer_host, self.node.peer_port),
            "peers": peers,
            "index": index,
            "value": self_value,
            "parentid": None,
            "default_value": value,
        }
        state = ConsensusState(msg)
        self.consensus_map[cid] = state
        if 0 <= index < len(self.node.word_list):
            self.node.word_list[index] = self_value
        self_key = peer_key(self.node.peer_host, self.node.peer_port)
        state.record_report(self_key, self_value)
        for peer in peers:
            if peer == self_key:
                continue
            host, port = peer.split(":")
            peer_value = self.choose_value(value)
            peer_msg = dict(msg)
            peer_msg["value"] = peer_value
            try:
                self.node.udp_socket.sendto(json.dumps(peer_msg).encode(), (resolve_host(host), int(port)))
            except OSError:
                continue
        logging.info(
            "Started consensus %s at index %d with value '%s' and m=%d",
            cid,
            index,
            self_value,
            m,
        )


from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from .node import PeerNode
