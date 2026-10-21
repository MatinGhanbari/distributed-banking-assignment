"""A tiny TCP proxy that can simulate a network partition on one machine (bonus 2).

    p = TcpProxy(("127.0.0.1", 6001))   # forwards 127.0.0.1:<p.port> -> 127.0.0.1:6001
    p.cut()    # like iptables DROP: bytes are silently thrown away, connections stay open,
               # new connections are accepted but nothing gets through -> the peer only sees
               # timeouts, exactly like a pulled cable
    p.heal()   # traffic flows again; connections from the partition are closed (in a real
               # network they would usually time out/reset), so both sides reconnect
"""
import socket
import threading


class TcpProxy:
    def __init__(self, target, listen_host="127.0.0.1", listen_port=0, name="proxy"):
        self.target, self.name = target, name
        self.partitioned = False
        self.lock = threading.Lock()
        self.conns = set()
        self.sock = socket.socket()
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((listen_host, listen_port))
        self.sock.listen(64)
        self.port = self.sock.getsockname()[1]
        self.closed = False
        threading.Thread(target=self._accept_loop, daemon=True).start()

    @property
    def addr(self):
        return f"127.0.0.1:{self.port}"

    def cut(self):
        self.partitioned = True

    def heal(self):
        with self.lock:
            conns, self.conns = list(self.conns), set()
            self.partitioned = False
        for s in conns:
            try:
                s.close()
            except OSError:
                pass

    def close(self):
        self.closed = True
        try:
            self.sock.close()
        except OSError:
            pass
        self.heal()

    def _track(self, *socks):
        with self.lock:
            self.conns.update(socks)

    def _accept_loop(self):
        while not self.closed:
            try:
                client, _ = self.sock.accept()
            except OSError:
                return
            threading.Thread(target=self._serve, args=(client,), daemon=True).start()

    def _serve(self, client):
        self._track(client)
        upstream = None
        if self.partitioned:
            # A SYN would be dropped: keep the client hanging, never connect upstream.
            self._pump(client, None)
            return
        try:
            upstream = socket.create_connection(self.target, timeout=5)
            upstream.settimeout(None)
        except OSError:
            client.close()
            return
        self._track(upstream)
        threading.Thread(target=self._pump, args=(upstream, client), daemon=True).start()
        self._pump(client, upstream)

    def _pump(self, src, dst):
        try:
            while True:
                data = src.recv(65536)
                if not data:
                    break
                if self.partitioned or dst is None:
                    continue                      # dropped on the floor
                dst.sendall(data)
        except OSError:
            pass
        for s in (src, dst):
            if s is not None:
                try:
                    s.close()
                except OSError:
                    pass
