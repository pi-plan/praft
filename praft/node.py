import asyncio
from typing import Callable, NoReturn

from praft.log import Entity, EntityType, ConfChangeData, ConfChangeType
from praft.peer import Peer
from praft.conf import Conf
from praft.exceptions import PRaftException
from praft.typing import T_URL
from praft.message import Message
from praft.state import State, Follower, Learner
from praft.state_machine import StateMachine
from praft.log_storage import LogStorage


class Node(object):

    def __init__(self,
                 conf: Conf,
                 peer_factory: Callable[[int, T_URL], Peer],
                 state_machine: StateMachine,
                 log_storage: LogStorage):
        self.id = id
        self.conf: Conf = conf
        self.peer_factory: Callable[[int, T_URL], Peer] = peer_factory
        self.raft: State = None
        self.state_machine: StateMachine = state_machine
        self.log_storage: LogStorage = log_storage

    def bootstrap(self):
        if self.conf.listen_url not in self.conf.initial_advertise_peers:
            raise PRaftException("listen_url isn`t in initial_advertise_peers")
        self.id =\
            self.conf.initial_advertise_peers.index(self.conf.listen_url) + 1

        self.raft = Follower.new(self.id, self.conf, self.peer_factory,
                                 self.state_machine, self.log_storage)
        self.raft.add_state_observer(self.state_change_callback)  # 注册角色切换的观察者
        for i, v in enumerate(self.conf.initial_advertise_peers):
            conf_data = ConfChangeData(ConfChangeType.add_node, i + 1, v)
            entity = Entity(1, i + 1, EntityType.conf_change, conf_data)
            self.raft.logs_append(entity)
        self.raft.commit(i + 1)
        self.raft.apply()

    def join_bootstrap(self):
        self.raft = Learner.new(0, self.conf, self.peer_factory,
                                self.state_machine, self.log_storage)
        self.raft.add_state_observer(self.state_change_callback)
        self.raft.join(self.conf.listen_url, self.conf.join_url)

    def start(self):
        if self.conf.join_url:
            self.join_bootstrap()
        else:
            self.bootstrap()
        self.raft.tick()
        if self.conf.debug:
            asyncio.get_running_loop().call_later(3, self.debug_info_dump)

    def debug_info_dump(self):
        print("[{}] \
term:{}, latest log: {}, applied: {}, commited: {}, peers:{}".format(
              type(self.raft).__name__,
              self.raft.current_term,
              self.raft.logs.get_last_index(),
              self.raft.last_applied,
              self.raft.commit_index,
              [i.get_url() for i in self.raft.peers]))
        asyncio.get_running_loop().call_later(3, self.debug_info_dump)

    def message(self, msg: Message) -> object:
        return self.raft.message(msg)

    def get_leader(self) -> T_URL:
        return self.raft.get_leader()

    def read(self, param: object):
        return self.raft.read(param)

    def remove_node(self, url):
        return self.raft.remove_node(url)

    async def propose(self, msg: object):
        result = await self.raft.propose(msg)
        return result

    def raft_update(self, new_state: State) -> NoReturn:
        self.raft = new_state

    def state_change_callback(self, old_state: State, new_state: State):
        # 有些请求发出去了，在回应之前已经做了角色变更。
        if self.raft != new_state \
                and self.raft.current_term <= new_state.current_term:
            self.raft_update(new_state)
