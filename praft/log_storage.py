from abc import ABCMeta, abstractmethod
from typing import Iterable, NoReturn, List

from praft.log import Entity
from praft.state_machine import StateMachine


class SnapshotData(object):
    def __init__(self, offset: int, data: List[object], done: bool):
        self.offset = offset
        self.data = data
        self.done = done


class LogStorage(metaclass=ABCMeta):

    @abstractmethod
    def append(self, entity: Entity) -> NoReturn:
        """
        追加日志
        """
        pass

    @abstractmethod
    def read_from(self, index: int, length: int = None) -> Iterable[Entity]:
        """
        从指定索引开始读取日志
        raise LogHasDeleted
        """
        pass

    @abstractmethod
    def get(self, index: int) -> Entity:
        """
        获取指定索引的日志
        raise LogHasDeleted
        """
        pass

    @abstractmethod
    def get_last_entity(self) -> Entity:
        """
        获取最后一个日志
        """
        pass

    @abstractmethod
    def get_last_index(self) -> int:
        """
        获取最后一个索引
        """
        pass

    @abstractmethod
    def replication(self, entries: Iterable[Entity]):
        """日志同步，覆盖和追加"""
        pass

    @abstractmethod
    def create_snapshot(self, last_index: int, state_machine: StateMachine):
        pass

    @abstractmethod
    def read_snapshot(self) -> Iterable[SnapshotData]:
        pass

    @abstractmethod
    def reset(self, last_index: int, last_term: int):
        pass
