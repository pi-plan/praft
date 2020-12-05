from enum import Enum, unique

from praft.typing import T_URL
from praft.exceptions import MessageDataError


@unique
class EntityType(Enum):
    normal: int = 1  # 常规日志
    conf_change: int = 2  # 配置变更的

    @classmethod
    def get_type(cls, t: int) -> 'EntityType':
        for i in list(cls):
            if t == i.value:
                return i
        raise MessageDataError("field `type` not in EntityType.")


class Entity(object):
    def __init__(self, term: int, index: int, type: EntityType, data: object):
        self.term: int = term
        self.index: int = index
        self.type: EntityType = type if isinstance(type, EntityType) else \
            EntityType.get_type(type)
        if self.type is EntityType.conf_change:
            self.data: object = data if isinstance(data, ConfChangeData) else\
                ConfChangeData(**data)
        else:
            self.data: object = data


@unique
class ConfChangeType(Enum):
    add_node: int = 1  # 增加节点
    remove_node: int = 2  # 删除节点
    add_learner_node: int = 3  # 增加 learner 节点

    @classmethod
    def get_type(cls, t: int) -> 'ConfChangeType':
        for i in list(cls):
            if t == i.value:
                return i
        raise MessageDataError("field `type` not in ConfChangeType.")


class ConfChangeData(object):
    def __init__(self, type: ConfChangeType, id: int, url: T_URL):
        self.type: ConfChangeType = type if isinstance(type, ConfChangeType)\
            else ConfChangeType.get_type(type)
        self.id: int = id
        self.url: T_URL = url
