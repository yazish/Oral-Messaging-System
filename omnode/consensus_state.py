import uuid
from collections import Counter
from typing import Dict, List, Optional, Set


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
