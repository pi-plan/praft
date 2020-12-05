import socket
from typing import List, Tuple


class Conf(object):
    def __init__(self,
                 listen_url: Tuple[str, int],
                 initial_advertise_peers: List[Tuple[str, int]] = None,
                 join_url: Tuple[str, int] = None,
                 name: str = None,
                 election_timeout: int = None,
                 heartbeat_interval: int = None,
                 debug: bool = False):
        if name is None:
            self.name: str = socket.gethostname()
        else:
            self.name: str = name
        if election_timeout is None:
            self.election_timeout: int = 1000
        else:
            self.election_timeout: str = election_timeout
        if heartbeat_interval is None:
            self.heartbeat_interval: int = 100
        else:
            self.heartbeat_interval = heartbeat_interval
        self.join_url: Tuple[str, int] = join_url
        self.initial_advertise_peers: List[Tuple[str, int]] =\
            initial_advertise_peers
        self.listen_url: Tuple[str, int] = listen_url
        self.debug = debug
