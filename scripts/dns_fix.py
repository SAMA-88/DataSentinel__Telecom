"""If the system DNS fails, resolve names through 8.8.8.8 directly (stdlib only, no admin needed).
   Import it first:  import dns_fix"""
import random
import socket
import struct

_orig = socket.getaddrinfo


def _skip_name(d, pos):
    while True:
        n = d[pos]
        if n == 0:
            return pos + 1
        if n & 0xC0 == 0xC0:
            return pos + 2
        pos += n + 1


def _lookup(host, server="8.8.8.8"):
    q = struct.pack(">HHHHHH", random.randint(0, 65535), 0x0100, 1, 0, 0, 0)
    for part in host.rstrip(".").split("."):
        q += bytes([len(part)]) + part.encode()
    q += b"\x00" + struct.pack(">HH", 1, 1)
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.settimeout(3)
    try:
        s.sendto(q, (server, 53))
        d, _ = s.recvfrom(4096)
    finally:
        s.close()
    ips, pos = [], _skip_name(d, 12) + 4
    for _ in range(struct.unpack(">H", d[6:8])[0]):
        pos = _skip_name(d, pos)
        typ, _c, _t, rdlen = struct.unpack(">HHIH", d[pos:pos + 10])
        pos += 10
        if typ == 1 and rdlen == 4:
            ips.append(socket.inet_ntoa(d[pos:pos + 4]))
        pos += rdlen
    return ips


def _getaddrinfo(host, port, *args, **kwargs):
    try:
        return _orig(host, port, *args, **kwargs)
    except socket.gaierror:
        if not isinstance(host, str):
            raise
        for _ in range(3):
            try:
                ips = _lookup(host)
                if ips:
                    return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (ip, int(port or 0))) for ip in ips]
            except OSError:
                pass
        raise


socket.getaddrinfo = _getaddrinfo