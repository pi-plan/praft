from abc import ABCMeta, abstractmethod
from typing import List

from praft.log import Entity


class StateMachine(metaclass=ABCMeta):

    @abstractmethod
    def apply(self, entity: Entity):
        pass

    @abstractmethod
    def read(self, param: object):
        pass

    @abstractmethod
    def snapshot(self) -> List[object]:
        pass

    @abstractmethod
    def load_from_snapshot(self, offset: int, snapshot: List[object]):
        """Supports segmentation"""
        pass
