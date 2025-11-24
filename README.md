# Oral Messaging System

This repository provides a single Python program `myNode.py` that implements the peer node described in `a3.html`:

- UDP gossip mesh for announcing peers and forwarding unique gossip messages while pruning stale entries.
- Oral Messages (OM) consensus across a five-word distributed "database" with recursive sub-consensus forwarding.
- TCP CLI for monitoring peers, toggling lying behaviour, viewing the current database, and starting a consensus.

## Requirements

- Python 3.8+
- No external dependencies beyond the Python standard library.

## Running

Start the peer with an OS-assigned UDP port:

```sh
python myNode.py
```

Or choose a specific UDP port (CLI port is still OS-assigned):

```sh
python myNode.py 16000
```

On startup the node prints both the UDP peer port and the TCP CLI port. The node automatically gossips to the well-known peers listed in `a3.html`.

### CLI Commands

Connect via `telnet` or `nc` to the printed CLI port. Supported commands:

- `peers` – list known peers and when they were last heard from.
- `current` – show the local five-word database.
- `consensus <index> <word>` – start a root consensus to set the given index.
- `lie [percent]` – enable lying for consensus values (default 100% of the time, or specify a percentage).
- `truth` – disable lying.
- `exit` – close the client connection.

The node automatically removes peers that have been silent for 2 minutes and sends heartbeat gossip every minute.

## Notes

- Consensus messages include `parentid` for tree tracking and optional `reporter` fields for propagating sub-consensus results upward.
- Gossip and consensus handling avoid crashes on malformed data and use `select` for multiplexing as required by the assignment description.
