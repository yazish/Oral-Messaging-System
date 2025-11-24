import argparse
import logging
import sys

from omnode.node import PeerNode

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(message)s")


def parse_args(argv):
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
