from abc import abstractmethod, ABCMeta
from praft.typing import T_URL

from praft.message import Message


class Peer(metaclass=ABCMeta):
    @abstractmethod
    def __init__(self, id: int, url: T_URL):
        pass

    @abstractmethod
    async def send(self, message: Message):
        pass

    @abstractmethod
    async def send_snap(self):
        pass

    @abstractmethod
    async def stop(self):
        pass

    @abstractmethod
    def get_id(self):
        pass

    @abstractmethod
    def get_url(self):
        pass

    @abstractmethod
    def start_election(self, message: Message):
        """
        must be synchronous
        """
        pass
