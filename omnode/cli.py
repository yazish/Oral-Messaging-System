import time


class CliHandler:
    def __init__(self, node: "PeerNode"):
        self.node = node

    def handle_cli_message(self, client, line: str):
        parts = line.strip().split()
        if not parts:
            return
        cmd = parts[0].lower()
        if cmd == "peers":
            resp_lines = []
            for info in self.node.peers.values():
                delta = time.time() - info.get("last_seen", 0)
                resp_lines.append(
                    f"{info['host']}:{info['port']} (name={info.get('name','')}, last_seen={delta:.1f}s)"
                )
            if not resp_lines:
                resp_lines = ["No peers known."]
            client.sendall(("\n".join(resp_lines) + "\n").encode())
        elif cmd == "current":
            words = ", ".join(f"[{i}] {w}" for i, w in enumerate(self.node.word_list))
            client.sendall((words + "\n").encode())
        elif cmd == "consensus" and len(parts) >= 3:
            try:
                idx = int(parts[1])
            except ValueError:
                client.sendall(b"Invalid index.\n")
                return
            value = " ".join(parts[2:])
            self.node.consensus.start_root_consensus(idx, value)
            client.sendall(b"Consensus started.\n")
        elif cmd == "lie":
            rate = 1.0
            if len(parts) > 1:
                try:
                    rate = max(0.0, min(1.0, float(parts[1]) / 100.0))
                except ValueError:
                    pass
            self.node.lie_mode = True
            self.node.lie_rate = rate
            client.sendall(f"Lying enabled at rate {self.node.lie_rate*100:.0f}%.\n".encode())
        elif cmd == "truth":
            self.node.lie_mode = False
            client.sendall(b"Lying disabled.\n")
        elif cmd == "exit":
            client.sendall(b"Goodbye.\n")
            client.close()
            if client in self.node.cli_clients:
                self.node.cli_clients.remove(client)
            self.node.cli_buffers.pop(client, None)
        else:
            client.sendall(b"Unknown command.\n")


from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from .node import PeerNode
