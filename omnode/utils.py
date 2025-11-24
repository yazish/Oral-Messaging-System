import socket


def resolve_host(host: str) -> str:
    """Resolve hostnames gracefully, returning the original host on failure."""
    try:
        return socket.gethostbyname(host)
    except socket.gaierror:
        return host


def peer_key(host: str, port: int) -> str:
    return f"{host}:{port}"
