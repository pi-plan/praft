import argparse
import enum
import sys
import json
import urllib.request
import warnings

from typing import Tuple, Iterable, NoReturn, List

import tornado.ioloop
import tornado.web
import tornado.locks
import tornado.httpclient
import tornado.options

sys.path.extend("../../praft")

import praft.exceptions

from praft.conf import Conf
from praft.peer import Peer
from praft.message import Message, MessageFactory
from praft.log import Entity, EntityType, ConfChangeType
from praft.log_storage import LogStorage, SnapshotData
from praft.node import Node
from praft.state_machine import StateMachine


def parse_args():
    paser = argparse.ArgumentParser(description="used for praft")
    paser.add_argument("--debug", action="store_true",
                       help="open debug mode.")
    paser.add_argument("--heartbeat-interval", metavar=100, action="store",
                       help="Time (in milliseconds) of a heartbeat interval.",
                       default=100, type=int)
    paser.add_argument("--election-timeout", metavar="1000", action="store",
                       help="listen on for peer traffic.", default=1000,
                       type=int)
    paser.add_argument("--listen-url", metavar="0.0.0.0:9023", action="store",
                       default="0.0.0.0:9023",
                       type=lambda u: (u.split(":")[0], int(u.split(":")[1])),
                       help="listen on for peer and client traffic.")
    paser.add_argument("--initial-advertise-peers", metavar="10.0.0.0:9023,\
10.0.0.1,9023", action="store",
                       default="0.0.0.0:9023",
                       type=lambda u: [(i.split(":")[0], int(i.split(":")[1]))
                                       for i in u.split(",")],
                       help="List of this member’s peer URLs to advertise to \
the rest of the cluster.")
    paser.add_argument("--join-url", metavar="10.0.0.0:9023", action="store",
                       type=lambda u: (u.split(":")[0], int(u.split(":")[1])),
                       help="Join the cluster.")
    args = paser.parse_args()
    return args


@enum.unique
class LogAction(enum.Enum):
    ADD: int = 1  # 增加数据
    DELETE: int = 2  # 删除数据
    UPDATE: int = 3  # 修改数据
    @classmethod
    def get_type(cls, t: int) -> 'EntityType':
        for i in list(cls):
            if t == i.value:
                return i
        raise praft.exceptions.MessageDataError(
            "field `type` not in LogAction.")


class LogData(object):
    def __init__(self, action: LogAction, key: str, value: object):
        self.action: LogAction = action if isinstance(action, LogAction) else \
            LogAction.get_type(action)
        self.key: str = key
        self.value: object = value


class MessageHandler(tornado.web.RequestHandler):

    async def put(self):
        try:
            msg = json.loads(self.request.body)
            message = MessageFactory.get_instance(msg)
            resp = self.application.node.message(message)
        except (praft.exceptions.LeaderDemoted,
                praft.exceptions.NeedLeaderHandle):
            self.set_status(500)
            return
        except praft.exceptions.RoleCanNotDo as e:
            self.set_status(501)
            self.write(e.args)
            return
        except Exception as e:
            self.set_status(500)
            self.write(e.args)
            return
        self.write(message2json(resp))


class ClientHandler(tornado.web.RequestHandler):

    def get_leader_url(self):
        leader = self.application.node.get_leader()
        return "http://{}:{}".format(*leader.url)

    async def get(self, path: str):
        key = path
        try:
            result = self.application.node.read(key)
        except (praft.exceptions.LeaderDemoted,
                praft.exceptions.NeedLeaderHandle):
            self.redirect("{}{}".format(self.get_leader_url(),
                                        self.request.uri))
            return
        except praft.exceptions.RoleCanNotDo as e:
            self.set_status(501, e.args)
            return
        except Exception as e:
            self.set_status(500, e.args)
            return
        if result is None:
            self.set_status(404)
            return
        self.write(result)

    async def put(self, path: str):
        key = path
        try:
            value = json.loads(self.request.body)
            result = await self.application.node.propose(
                LogData(LogAction.ADD, key, value))
        except (praft.exceptions.LeaderDemoted,
                praft.exceptions.NeedLeaderHandle):
            self.redirect("{}{}".format(self.get_leader_url(),
                                        self.request.uri))
            return
        except praft.exceptions.RoleCanNotDo as e:
            self.set_status(501, e.args)
            return
        except Exception as e:
            self.set_status(500, e.args)
            return
        if result is None:
            self.set_status(404)
            return
        self.write(result.value)

    async def delete(self, path: str):
        key = path
        try:
            await self.application.node.propose(LogData(LogAction.DELETE, key))
        except (praft.exceptions.LeaderDemoted,
                praft.exceptions.NeedLeaderHandle):
            self.redirect("{}{}".format(self.get_leader_url(),
                                        self.request.uri))
            return
        except praft.exceptions.RoleCanNotDo as e:
            self.set_status(501, e.args)
            return
        except Exception as e:
            self.set_status(500, e.args)
            return
        self.write({"succ": True})


class LeaderTransferHandler(ClientHandler):

    async def put(self):
        try:
            resp = self.application.node.raft.leadership_transfer()
        except (praft.exceptions.LeaderDemoted,
                praft.exceptions.NeedLeaderHandle):
            self.redirect("{}{}".format(self.get_leader_url(),
                                        self.request.uri))
            return
        except praft.exceptions.RoleCanNotDo as e:
            self.set_status(501)
            self.write(e.args)
            return
        except Exception as e:
            self.set_status(500)
            self.write(e.args)
            return
        self.write({"transferring": bool(resp)})


class RemoveNodeHandler(ClientHandler):

    async def delete(self):
        try:
            value = json.loads(self.request.body)
            url = (value["hostname"], int(value["port"]))
            await self.application.node.remove_node(url)
        except (praft.exceptions.LeaderDemoted,
                praft.exceptions.NeedLeaderHandle):
            self.redirect("{}{}".format(self.get_leader_url(),
                                        self.request.uri))
            return
        except praft.exceptions.RoleCanNotDo as e:
            self.set_status(501, e.args)
            return
        except Exception as e:
            self.set_status(500, e.args)
            return
        self.write({"succ": True})


def message2json(msg: object) -> dict:
    result = {}
    if not hasattr(msg, "__dict__"):
        return msg
    for k, v in msg.__dict__.items():
        if isinstance(v, enum.Enum):
            result[k] = v.value
        elif hasattr(v, '__dict__'):
            result[k] = message2json(v)
        elif isinstance(v, list) or isinstance(v, tuple):
            result[k] = [message2json(i) for i in v]
        else:
            result[k] = v
    return result


class HTTPJsonPeer(Peer):
    def __init__(self, id: int, url: Tuple[str, int]):
        self.id: int = id
        self.url: Tuple[str, int] = url
        self.http_url = "http://{}:{}/_praft/messages".format(*self.url)
        self.header = {}

    def get_id(self):
        return self.id

    def get_url(self):
        return self.url

    def paser_response(self, body) -> Message:
        return json.loads(body, object_hook=MessageFactory.get_instance)

    async def send(self, message: Message):
        body = json.dumps(message2json(message))
        req = tornado.httpclient.HTTPRequest(self.http_url, "PUT", self.header,
                                             body)
        try:
            response = await tornado.httpclient.AsyncHTTPClient().fetch(req)
        except Exception as e:
            warnings.warn(e)
            return None
        return self.paser_response(response.body)

    async def stop(self):
        pass

    async def send_snap(self):
        pass

    def start_election(self, message) -> Message:
        body = json.dumps(message2json(message))
        req = urllib.request.Request(self.http_url, body.encode(),
                                     method="PUT")
        try:
            response = urllib.request.urlopen(req)
        except Exception as e:
            warnings.warn(e)
            return None
        return self.paser_response(response.read())


class ListLogStorage(LogStorage):
    def __init__(self):
        self.logs = []
        self.last_index = None
        self.last_term = None
        self.snapshot = None

    def append(self, entity: Entity) -> NoReturn:
        self.logs.append(entity)

    def read_from(self, index: int, length: int = None) -> Iterable[Entity]:
        if not self.logs:
            return []
        if index > 0 and self.logs[0].index > index:
            raise praft.exceptions.LogHasDeleted()
        else:
            start = max(0, index - self.logs[0].index)
            if start > len(self.logs):
                return []
            end = start + length if length else None
            return self.logs[start:end]

    def get_last_entity(self) -> Entity:
        if self.logs:
            return self.logs[-1]
        else:
            return None

    def get_last_index(self) -> int:
        if self.logs:
            return self.logs[-1].index
        else:
            return self.last_index

    def get(self, index: int) -> Entity:
        if not self.logs:
            return None
        if self.logs[0].index > index:
            raise praft.exceptions.LogHasDeleted()
        else:
            target = (index - self.logs[0].index)
            if target > len(self.logs):
                return None
            return self.logs[target]

    def replication(self, entries: Iterable[Entity]):
        if not entries:
            return
        if not self.logs:
            self.logs = entries
            return
        first = self.logs[0]
        jump = 0
        if first.index != entries[0].index:
            jump = entries[0].index - first.index
        self.logs = self.logs[:jump] + entries

    def create_snapshot(self, index: int, state_machine: StateMachine):
        self.snapshot = list(state_machine.snapshot())

    def read_snapshot(self) -> Iterable[SnapshotData]:
        return [SnapshotData(0, self.snapshot, True)]

    def reset(self, last_index: int, last_term: int):
        self.logs = []
        self.last_index = last_index
        self.last_term = last_term


class DictStateMachine(StateMachine):

    def __init__(self):
        self.dict: dict = {}
        self.dict["_self"] = {}
        self.dict["_self"]["peers"] = []

    def apply(self, entity: Entity):
        if entity.type is not EntityType.normal:
            self.self_host_apply(entity)
            return
        data = entity.data if isinstance(entity.data, LogData) else \
            LogData(**entity.data)
        key = data.key
        if data.action is LogAction.ADD or data.action is LogAction.UPDATE:
            self.dict[key] = data.value
        elif data.action is LogAction.DELETE:
            del(self.dict[key])

    def self_host_apply(self, entity: Entity):
        """
        自身的一些数据的托管
        """
        if entity.type is EntityType.conf_change:
            conf = entity.data
            if conf.type is ConfChangeType.add_node:
                self.dict["_self"]["peers"].append((conf.id, tuple(conf.url)))
            elif conf.type is ConfChangeType.remove_node:
                print(conf.id, conf.url)
                self.dict["_self"]["peers"].remove((conf.id, tuple(conf.url)))

    def read(self, param: object):
        return self.dict.get(param, None)

    def snapshot(self) -> List[object]:
        return self.dict.items()

    def load_from_snapshot(self, offset: int, snapshot: List[object]):
        for key, value in snapshot:
            self.dict[key] = value


class Application(tornado.web.Application):

    def __init__(self, raft_node: Node):
        self.node = raft_node
        handlers = [(r"/_praft/messages", MessageHandler),
                    (r"/_praft/leadership_transfer", LeaderTransferHandler),
                    (r"/_praft/remove_node", RemoveNodeHandler),
                    (r"(.*)", ClientHandler)
                    ]
        super().__init__(handlers=handlers)


async def main():
    args = vars(parse_args())
    conf = Conf(**args)
    state_machine = DictStateMachine()
    log_storage = ListLogStorage()
    node = Node(conf, HTTPJsonPeer, state_machine, log_storage)
    node.start()
    app = Application(node)
    app.listen(conf.listen_url[1], conf.listen_url[0])
    shutdown_event = tornado.locks.Event()
    await shutdown_event.wait()


if __name__ == "__main__":
    tornado.ioloop.IOLoop.current().run_sync(main)
