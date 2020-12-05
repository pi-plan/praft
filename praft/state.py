import asyncio
import copy
import random
import sys
import time

from typing import Callable, Dict, List, NoReturn
from abc import ABCMeta, abstractclassmethod, abstractmethod

import praft.exceptions

from praft.typing import T_URL
from praft.log import Entity, EntityType, ConfChangeType, ConfChangeData
from praft.message import Message, MessageType, HeartBeatReq, HeartBeatRes,\
    PrevVoteReq, PrevVoteRes, VoteReq, VoteRes, StartElectionReq,\
    StartElectionRes, InstallSnapshotReq, InstallSnapshotRes, LearnReq,\
    LearnRes, LearnerJoinReq, LearnerJoinRes, ErrorMessage
from praft.peer import Peer
from praft.conf import Conf
from praft.state_machine import StateMachine
from praft.log_storage import LogStorage


class StateResult(object):
    def __init__(self, state: 'State', data: dict):
        self.state: 'State' = state
        self.data: object = data


class State(metaclass=ABCMeta):
    def __init__(self):
        self.id: int = None
        self.conf: Conf = None
        self.peer_factory: Callable[[int, T_URL], Peer] = None
        self.state_machine: StateMachine = None
        self.peers: List[Peer] = []

        self.current_term: int = 0
        self.leader_id = 0
        self.voted_for: int = None
        self.logs: LogStorage = None
        self.commit_index: int = 0  # 已知的已经提交的最大 index
        self.last_applied: int = 0  # 已经被应用到本机的 index
        self.election_timeout: int = 0  # 对配置时间随机增加 10%

        # 最近一次确认 leader 之后，选举超时次数, 用于 prevote 的时候判断,
        # 每次被宿主同步日志的时候归零。
        self.election_timeout_time: int = 0

        # state 变更事件观察者
        self.observers: List[Callable[[State, State], NoReturn]] = []
        self.role_changed = False  # 角色变化之后，对象没有被回收。

        # 最后一次快照的信息
        self.snapshot_installing = False
        self.snapshot_last_included_index: int = 0
        self.snapshot_last_included_term: int = 0

    @classmethod
    def new(cls,
            id: int,
            conf: Conf,
            peer_factory: Callable[[int, T_URL], Peer],
            state_machine: StateMachine,
            log_storage: LogStorage):
        state = cls()
        state.id = id
        state.conf: Conf = conf
        state.peer_factory: Callable[[int, T_URL], Peer] = peer_factory
        state.state_machine: StateMachine = state_machine
        state.peers: List[Peer] = []

        state.current_term: int = 0
        state.voted_for: int = None
        state.logs: LogStorage = log_storage
        return state

    def get_quorum(self):
        if hasattr(self, "learners"):
            return int((len(self.peers) - len(self.learners) + 1) / 2) + 1
        else:
            return int((len(self.peers) + 1) / 2) + 1

    def get_leader(self):
        if self.leader_id < 1:
            raise praft.exceptions.NotFoundLeader()
        for peer in self.peers:
            if peer.get_id() == self.leader_id:
                return peer
        raise praft.exceptions.NotFoundLeader()

    def copy(self, old_state: 'State') -> NoReturn:
        properties = vars(old_state)
        del(properties["role_changed"])
        for p, v in properties.items():
            if hasattr(self, p):
                setattr(self, p, v)

    @abstractclassmethod
    def become(cls, old_state: 'State') -> 'State':
        pass

    @abstractmethod
    def message(self, msg: Message):
        pass

    def add_peer(self, peer: Peer):
        if peer.get_id() == self.id:
            return
        self.peers.append(peer)

    def commit(self, index: int):
        self.commit_index = index

    def apply(self):
        if self.commit_index == self.last_applied:
            return
        for i in self.logs.read_from(
                self.last_applied + 1,
                (self.commit_index - self.last_applied) + 1):
            if i.index > self.commit_index:
                break
            if i.type is EntityType.conf_change:
                self.apply_config_change(i)
                # 继续执行，也在日志中存储，方便监控和给新加入的节点获取.
            self.state_machine.apply(i)
            self.last_applied = i.index
            if not (self.last_applied % 3):  # x 条日志压缩一下
                self.create_snapshot(i.index, i.term)

    @abstractmethod
    def tick(self) -> StateResult:
        pass

    def read(self, param) -> object:
        raise praft.exceptions.NeedLeaderHandle("Need leader to handler.")

    def propose(self, data: object):
        raise praft.exceptions.NeedLeaderHandle("Need leader to handler.")

    def remove_node(self, url: T_URL):
        raise praft.exceptions.NeedLeaderHandle("Need leader to handler.")

    def leadership_transfer(self):
        raise praft.exceptions.NeedLeaderHandle("Need leader to handler.")

    def create_snapshot(self, index: int, term: int):
        self.snapshot_last_included_index = index
        self.snapshot_last_included_term = term
        self.logs.create_snapshot(self.last_applied, self.state_machine)

    async def send_snapshot(self, peer: Peer):
        for i in self.logs.read_snapshot():
            conf = {
                "election_timeout": self.conf.election_timeout,
                "heartbeat_interval": self.conf.heartbeat_interval,
            }
            peers = []
            for j in self.peers:
                peers.append((j.get_id(), *j.get_url()))
            peers.append((self.id, *self.conf.listen_url))  # 增加自己
            msg = InstallSnapshotReq.new(self.current_term, self.leader_id,
                                         self.snapshot_last_included_index,
                                         self.snapshot_last_included_term,
                                         i.offset, i.data, i.done, conf, peers,
                                         self.id, peer.id)
            await peer.send(msg)
        if hasattr(self, "next_index"):
            self.next_index[peer.get_id()] = self.snapshot_last_included_index

    def update_term(self, term: int):
        self.current_term = term
        self.voted_for = 0

    def logs_append(self, entity: Entity):
        self.logs.append(entity)

    def get_heartbeat_interval(self):
        return self.conf.heartbeat_interval / 1000

    def get_election_timeout(self) -> int:
        if not self.election_timeout:
            slat = self.conf.election_timeout * 0.1
            self.election_timeout = self.conf.election_timeout +\
                random.randint(0 - slat, slat)
        return self.election_timeout / 1000

    def apply_config_change(self, entity: Entity):
        if entity.type is not EntityType.conf_change:
            return
        data = entity.data
        if data.type is ConfChangeType.add_node:
            self.add_peer(self.peer_factory(data.id, data.url))
        if data.type is ConfChangeType.remove_node:
            for i in self.peers:
                if i.get_id() is data.id:
                    self.peers.remove(i)
                    break

    def get_now_millisecond(self) -> int:
        return int(time.time() * 1000)

    def state_change_notify(self, new_state: 'State'):
        for observer in self.observers:
            observer(self, new_state)
        self.role_changed = True

    def add_state_observer(self, handler: Callable[['State', 'State'],
                                                   NoReturn]):
        self.observers.append(handler)


class Learner(State):
    def __init__(self):
        self.join_url: T_URL = None
        self.listen_url: T_URL = None
        super().__init__()

    @classmethod
    def become(cls, s):
        pass

    def tick(self):
        pass

    def join(self, listen_url: T_URL, join_url: T_URL):
        self.join_url = join_url
        self.listen_url = listen_url
        teacher = self.peer_factory(0, self.join_url)
        msg = LearnReq.new(0, self.listen_url)
        asyncio.get_running_loop().call_soon(
            lambda: asyncio.create_task(self.learn(teacher, msg)))

    async def learn(self, peer: Peer, message: Message):
        res = await peer.send(message)
        if not res or not hasattr(res, "confirmed") or not res.confirmed:
            sys.exit("can't join {}:{}".format(*self.join_url))

    def message(self, msg: Message):
        if msg.type is MessageType.HEARTBEAT_REQ:
            return self.heartbeat(msg)
        elif msg.type is MessageType.INSTALL_SNAPSHOT_REQ:
            return self.install_snapshot(msg)
        else:
            return ErrorMessage.new(self.current_term, 100001,
                                    "unknown message type.", self.id,
                                    msg.come_from)

    def install_snapshot(self, msg: InstallSnapshotReq):
        result = InstallSnapshotRes.new(self.current_term)
        if msg.term < self.current_term:
            return result
        self.current_term = msg.term
        self.snapshot_installing = not msg.done
        self.snapshot_last_included_index = msg.last_included_index
        self.snapshot_last_included_term = msg.last_included_term
        if msg.offset == 0:
            self.logs.reset(msg.last_included_index, msg.last_included_term)
        self.state_machine.load_from_snapshot(msg.offset, msg.data)
        self.peers.clear()
        for i in msg.peers:
            self.add_peer(self.peer_factory(i[0], i[1:]))
        self.conf.election_timeout = msg.conf["election_timeout"]
        self.conf.heartbeat_interval = msg.conf["heartbeat_interval"]
        self.leader_id = msg.leader_id
        if msg.done:
            asyncio.get_running_loop().call_soon(
                lambda: asyncio.create_task(self.join_to_leader()))
        return result

    async def join_to_leader(self):
        leader = None
        for i in self.peers:
            if i.get_id() == self.leader_id:
                leader = i
                break
        if not leader:
            sys.exit("can't join leader id: {}".format(self.leader_id))
        msg = LearnerJoinReq.new(self.current_term, self.listen_url)
        res = await leader.send(msg)
        if not res or not isinstance(res, LearnerJoinRes) or not res.id:
            sys.exit("can't join leader {}:{}".format(*leader.get_url()))

        self.id = res.id

    def heartbeat(self, msg: HeartBeatReq) -> Message:
        # 因为 leader 的term 比自己小，响应后 leader 会转变为 follower
        if msg.term < self.current_term:
            return HeartBeatRes.new(self.current_term, False, self.id,
                                    msg.come_from)
        self.latext_heart_beat_time = self.get_now_millisecond()
        if msg.term > self.current_term:
            self.update_term(msg.term)
        # 走到这里说明已经确认和接受 leader 了
        self.leader_id = msg.leader_id
        if self.snapshot_installing:
            return HeartBeatRes.new(self.current_term, False, self.id,
                                    msg.come_from)
        if msg.prev_log_index > 0:
            try:
                prev_log = self.logs.get(msg.prev_log_index)
                if not prev_log or prev_log.term != msg.prev_log_term:
                    return HeartBeatRes.new(self.current_term, False, self.id,
                                            msg.come_from)
            except praft.exceptions.LogHasDeleted:
                for i in msg.entries:
                    if i.index < self.snapshot_last_included_index:
                        continue
                    if i.index == self.snapshot_last_included_index:
                        if i.term != self.snapshot_last_included_term:
                            return HeartBeatRes.new(self.current_term, False,
                                                    self.id, msg.come_from)
        self.logs.replication(msg.entries)
        commited = min(msg.leader_commit, msg.entries[-1].index) if \
            msg.entries else msg.leader_commit

        if commited > self.commit_index:
            self.commit(commited)
            self.apply()
        if not msg.is_learner:
            follower = Follower.become(self)
            self.state_change_notify(follower)
        return HeartBeatRes.new(self.current_term, True, self.id,
                                msg.come_from)


class Follower(State):

    def __init__(self):
        self.latext_heart_beat_time = 0
        super().__init__()

    @classmethod
    def become(cls, old_state: State) -> 'Follower':
        s = cls()
        s.copy(old_state)
        loop = asyncio.get_running_loop()
        loop.call_later(s.get_election_timeout(), s.tick)
        return s

    def message(self, msg: Message):
        if msg.type is MessageType.PREV_VOTE_REQ:
            return self.pre_vote(msg)
        elif msg.type is MessageType.VOTE_REQ:
            return self.vote(msg)
        elif msg.type is MessageType.HEARTBEAT_REQ:
            return self.heartbeat(msg)
        elif msg.type is MessageType.START_ELECTION_REQ:
            return self.start_election(msg)
        elif msg.type is MessageType.INSTALL_SNAPSHOT_REQ:
            return self.install_snapshot(msg)
        elif msg.type is MessageType.LEARN_REQ:
            return self.learn_req(msg)
        else:
            return ErrorMessage.new(self.current_term, 100001,
                                    "unknown message type.", self.id,
                                    msg.come_from)

    def pre_vote(self, msg: PrevVoteReq) -> Message:
        if self.current_term > msg.term:
            return PrevVoteRes.new(self.current_term, False, self.id,
                                   msg.come_from)
        if self.election_timeout_time < 1:  # 有过超时才会投出选票
            return PrevVoteRes.new(self.current_term, False, self.id,
                                   msg.come_from)
        if self.voted_for and self.voted_for != msg.come_from:
            return PrevVoteRes.new(self.current_term, False, self.id,
                                   msg.come_from)
        # 都通过了，开始校验日志
        latest_log = self.logs.get_last_entity()
        if msg.last_log_index >= max(latest_log.index,
                                     self.snapshot_last_included_index) and\
                msg.last_log_term >= max(latest_log.term,
                                         self.snapshot_last_included_term):
            return PrevVoteRes.new(self.current_term, True, self.id,
                                   msg.come_from)
        return PrevVoteRes.new(self.current_term, False, self.id,
                               msg.come_from)

    def vote(self, msg: VoteReq) -> Message:
        if self.current_term > msg.term:
            return VoteRes.new(self.current_term, False, self.id,
                               msg.come_from)

        # 当 term 比较大的时候更新 term
        if msg.term > self.current_term:
            self.update_term(msg.term)

        if self.voted_for and self.voted_for != msg.come_from:
            return VoteRes.new(self.current_term, False, self.id,
                               msg.come_from)
        # 都通过了，开始校验日志
        latest_log = self.logs.get_last_entity()
        if msg.last_log_index >= max(latest_log.index,
                                     self.snapshot_last_included_index) and\
                msg.last_log_term >= max(latest_log.term,
                                         self.snapshot_last_included_term):
            self.voted_for = msg.come_from
            return VoteRes.new(self.current_term, True, self.id,
                               msg.come_from)
        return VoteRes.new(self.current_term, False, self.id,
                           msg.come_from)

    def heartbeat(self, msg: HeartBeatReq) -> Message:
        # 因为 leader 的term 比自己小，响应后 leader 会转变为 follower
        if msg.term < self.current_term:
            return HeartBeatRes.new(self.current_term, False, self.id,
                                    msg.come_from)
        self.latext_heart_beat_time = self.get_now_millisecond()
        if msg.term > self.current_term:
            self.update_term(msg.term)
        # 走到这里说明已经确认和接受 leader 了
        self.leader_id = msg.leader_id
        if self.snapshot_installing:
            return HeartBeatRes.new(self.current_term, False, self.id,
                                    msg.come_from)
        if msg.prev_log_index > 0:
            try:
                prev_log = self.logs.get(msg.prev_log_index)
                if not prev_log or prev_log.term != msg.prev_log_term:
                    return HeartBeatRes.new(self.current_term, False, self.id,
                                            msg.come_from)
            except praft.exceptions.LogHasDeleted:
                for i in msg.entries:
                    if i.index < self.snapshot_last_included_index:
                        continue
                    if i.index == self.snapshot_last_included_index:
                        if i.term != self.snapshot_last_included_term:
                            return HeartBeatRes.new(self.current_term, False,
                                                    self.id, msg.come_from)

        self.logs.replication(msg.entries)
        commited = min(msg.leader_commit, msg.entries[-1].index) if \
            msg.entries else msg.leader_commit

        if commited > self.commit_index:
            self.commit(commited)
            self.apply()
        return HeartBeatRes.new(self.current_term, True, self.id,
                                msg.come_from)

    def start_election(self, msg: Message):
        result = StartElectionRes.new(self.current_term, False, self.id,
                                      msg.come_from)
        if self.current_term != msg.term:
            return result

        if self.logs.get_last_index() < msg.leader_latest_index:
            return result

        if self.commit_index < msg.leader_commit:
            return result
        # skip prevote
        candidate = Candidate.become(self)
        self.state_change_notify(candidate)
        loop = asyncio.get_running_loop()
        loop.call_soon(lambda: loop.create_task(candidate.election()))
        result.confirmed = True
        del(self)
        return result

    def learn_req(self, msg: LearnReq):
        peer = self.peer_factory(0, msg.url)
        asyncio.get_running_loop().call_soon(
            lambda: asyncio.create_task(self.send_snapshot(peer)))
        return LearnRes.new(self.current_term, True)

    def install_snapshot(self, msg: InstallSnapshotReq):
        result = InstallSnapshotRes(self.current_term)
        if msg.term < self.current_term:
            return result
        self.current_term = msg.term
        self.snapshot_installing = msg.done
        self.snapshot_last_included_index = msg.last_included_index
        self.snapshot_last_included_term = msg.last_included_term
        if msg.offset == 0:
            self.logs.reset(msg.last_included_index, msg.last_included_term)
        self.state_machine.load_from_snapshot(msg.offset, msg.data)

    def tick(self):
        if self.role_changed:
            return
        self.apply()
        now = self.get_now_millisecond()
        loop = asyncio.get_running_loop()
        if self.latext_heart_beat_time + self.get_election_timeout() * 1000\
                > now:
            loop.call_later(self.get_election_timeout(), self.tick)
        else:
            candidate = PreCandidate.become(self)
            self.state_change_notify(candidate)
            loop.call_soon(lambda: loop.create_task(candidate.election()))
            del(self)


class PreCandidate(State):
    def __init__(self, **kvargs):
        self.vote_timeout: float = 0
        self.vote_result = []
        self.election_start_at = 0

        # 此次选举是否已经结束，处理异步请求回调的重复处理
        self.election_end = False
        super().__init__(**kvargs)

    def reset(self):
        self.vote_timeout: float = random.randint(150, 500) / 1000  # 投票超时
        self.vote_result = []
        self.election_start_at = 0
        self.election_end = False
        self.election_timeout_time += 1

    def get_vote_timeout(self) -> float:
        return self.vote_timeout

    def apply(self):
        raise praft.exceptions.RoleCanNotDo()

    def commit(self):
        raise praft.exceptions.RoleCanNotDo()

    def add_peer(self):
        raise praft.exceptions.RoleCanNotDo()

    @classmethod
    def become(cls, old_state: State) -> 'PreCandidate':
        if not isinstance(old_state, Follower):
            # 只能从 follower 转变过来。
            raise praft.exceptions.UnknownError()
        candidate = cls()
        candidate.copy(old_state)
        candidate.reset()
        return candidate

    def message(self, msg: Message):
        # 所有预投票都返回失败
        if msg.type is MessageType.PREV_VOTE_REQ:
            return PrevVoteRes.new(self.current_term,
                                   msg.term >= self.current_term, self.id,
                                   msg.come_from)
        if (msg.term > self.current_term) or \
                (msg.term == self.current_term and msg.type is
                 MessageType.HEARTBEAT_REQ):
            follower = Follower.become(self)
            self.election_end = True
            self.state_change_notify(follower)
            del(self)
            return follower.message(msg)
        # 其他的请求一律都失败
        if msg.type is MessageType.HEARTBEAT_REQ:
            return HeartBeatRes.new(self.current_term, False, self.id,
                                    msg.come_from)
        elif msg.type is MessageType.VOTE_REQ:
            return VoteRes.new(self.current_term, False, self.id,
                               msg.come_from)
        else:
            return ErrorMessage.new(self.current_term, 100001,
                                    "unknown message type.", self.id,
                                    msg.come_from)

    async def election(self):
        self.election_start_at = self.get_now_millisecond()
        asyncio.get_running_loop().call_later(
            self.get_vote_timeout(), self.tick)
        await self.vote()

    async def vote(self):
        # 首先预投票
        self.vote_result.append(True)  # 投给自己
        last_log = self.logs.get_last_entity()
        message = PrevVoteReq.new(self.current_term + 1, self.id,
                                  last_log.index, last_log.term)
        for peer in self.peers:
            asyncio.create_task(self.talk_peer(peer, copy.copy(message)))

    async def talk_peer(self, peer: Peer, message: Message):
        message.set_from_to(self.id, peer.get_id())
        res = await peer.send(message)
        if self.election_end:
            return
        if not res:
            return

        if res.term > self.current_term:
            follower = Follower.become(self)
            self.election_end = True
            self.state_change_notify(follower)
            del(self)
            return

        self.vote_result.append(res.vote_granted)
        if self.vote_result.count(True) > self.get_quorum():
            self.vote_succ()

    def tick(self) -> StateResult:
        if self.role_changed:
            return
        if self.election_end:
            return
        now = self.get_now_millisecond()
        if self.election_start_at + self.get_vote_timeout() > now:
            return

        # 这里能选举通过主要是为了兼容单台的场景
        if self.vote_result.count(True) < self.get_quorum():
            self.reset()
            loop = asyncio.get_running_loop()
            loop.call_soon(lambda: asyncio.create_task(self.election()))
            return
        else:
            self.vote_succ()

    def vote_succ(self):
        candidate = Candidate.become(self)
        self.election_end = True
        self.state_change_notify(candidate)
        candidate.reset()
        asyncio.get_running_loop().call_soon(
            lambda: asyncio.create_task(candidate.election()))
        del(self)


class Candidate(PreCandidate):

    def reset(self):
        self.current_term += 1
        super().reset()

    @classmethod
    def become(cls, pre_candidate: PreCandidate) -> 'Candidate':
        candidate = cls()
        candidate.copy(pre_candidate)
        candidate.reset()
        return candidate

    async def election(self):
        self.vote_result.append(True)
        self.election_start_at = self.get_now_millisecond()
        asyncio.get_running_loop().call_later(
            self.get_vote_timeout(), self.tick)
        asyncio.create_task(self.vote())

    async def vote(self):
        # 开始正式投票
        last_log = self.logs.get_last_entity()
        message = VoteReq.new(self.current_term, self.id,
                              last_log.index, last_log.term)
        for peer in self.peers:
            asyncio.create_task(self.talk_peer(peer, copy.copy(message)))

    async def talk_peer(self, peer: Peer, message: Message):
        message.set_from_to(self.id, peer.get_id())
        res = await peer.send(message)
        if self.election_end:
            return
        if not res:
            return

        if res.term > self.current_term:
            follower = Follower.become(self)
            self.election_end = True
            self.state_change_notify(follower)
            del(self)
            return

        self.vote_result.append(res.vote_granted)
        if self.vote_result.count(True) > self.get_quorum():
            self.vote_succ()
        return res

    def tick(self) -> StateResult:
        if self.role_changed:
            return

        if self.election_end:
            return

        now = self.get_now_millisecond()
        if self.election_start_at + self.get_vote_timeout() > now:
            return

        # 这里能选举通过主要是为了兼容单台的场景
        if self.vote_result.count(True) < self.get_quorum():
            self.reset()
            loop = asyncio.get_running_loop()
            loop.call_soon(lambda: asyncio.create_task(self.election()))
        else:
            self.vote_succ()

    def vote_succ(self):
        leader = Leader.become(self)
        self.election_end = True
        self.state_change_notify(leader)
        leader.elected()
        del(self)


class Leader(State):
    def __init__(self, **kvargs):
        self.next_index: Dict[int, int] = dict()
        self.match_index: Dict[int, int] = dict()
        self.learners: List[Peer] = []
        super().__init__(**kvargs)

    @classmethod
    def become(cls, candidate: Candidate) -> 'Leader':
        s = cls()
        s.copy(candidate)
        s.leader_id = s.id
        return s

    def read(self, param) -> object:
        return self.state_machine.read(param)

    def commit(self, index: int):
        self.commit_index = index

    def elected(self):
        # 当选后开始同步
        last_log = self.logs.get_last_entity()
        message = HeartBeatReq.new(self.current_term, self.id, last_log.index,
                                   last_log.term, self.commit_index, [])
        loop = asyncio.get_running_loop()
        for peer in self.peers:
            loop.call_soon(lambda: asyncio.create_task(
                self.peer_log_append(peer, copy.copy(message))))
        loop.call_later(self.get_heartbeat_interval(), self.tick)

    async def peer_log_append(self, peer: Peer, message: HeartBeatReq = None,
                              loop: bool = True) -> bool:
        if message is None:
            try:
                message = self.create_peer_message(peer)
            except praft.exceptions.LogHasDeleted:
                return False

        message.set_from_to(self.id, peer.id)
        res = await peer.send(message)
        if not res:
            return False
        if res.term > self.current_term:
            follower = Follower.become(self)
            self.state_change_notify(follower)
            del(self)
            return

        match_index = message.prev_log_index
        next_index = match_index + 1
        if res.success:
            if len(message.entries):
                match_index = message.entries[-1].index
                next_index = match_index + 1
            self.next_index[peer.get_id()] = next_index
            self.match_index[peer.get_id()] = match_index
            return True
        else:
            if loop:
                next_index = max(0, message.prev_log_index - 50)
                self.next_index[peer.get_id()] = next_index
                if self.role_changed:
                    return False
                if next_index <= self.snapshot_last_included_index:
                    # 安装快照
                    asyncio.create_task(self.send_snapshot(peer))
            return False

    def create_peer_message(self, peer: Peer) -> HeartBeatReq:
        next_index = self.next_index.get(peer.id, 0)
        entries = self.logs.read_from(next_index, 50)
        if entries:
            if (entries[0].index - 1) == 0:
                prev_log_index = 0
                prev_log_term = 0
            else:
                prev_log = self.logs.get(entries[0].index - 1)
                prev_log_index = prev_log.index
                prev_log_term = prev_log.term
        else:
            last_log = self.logs.get_last_entity()
            prev_log_index = last_log.index
            prev_log_term = last_log.term

        is_learner = False
        if peer in self.learners:
            is_learner = True
            if (peer.get_id() in self.match_index.keys()) and\
                    self.match_index[peer.get_id()] >= \
                    self.logs.get_last_index():
                is_learner = False
                self.learners.remove(peer)

        message = HeartBeatReq.new(self.current_term, self.id,
                                   prev_log_index,
                                   prev_log_term,
                                   self.commit_index, entries, is_learner)
        return message

    def message(self, msg: Message):
        if msg.type is MessageType.PREV_VOTE_REQ:
            return PrevVoteRes.new(self.current_term, False, self.id,
                                   msg.come_from)
        if msg.term > self.current_term:
            follower = Follower.become(self)
            self.state_change_notify(follower)
            return follower.message(msg)
        elif msg.type is MessageType.VOTE_REQ:
            return VoteRes.new(self.current_term, False, self.id,
                               msg.come_from)
        elif msg.type is MessageType.HEARTBEAT_REQ:
            return HeartBeatRes.new(self.current_term, False, self.id,
                                    msg.come_from)
        elif msg.type is MessageType.LEARN_REQ:
            return self.learn_req(msg)
        elif msg.type is MessageType.LEARNER_JOIN_REQ:
            return self.learner_join(msg)
        else:
            return ErrorMessage.new(self.current_term, 100001,
                                    "unknown message type.", self.id,
                                    msg.come_from)

    def learn_req(self, msg: LearnReq):
        peer = self.peer_factory(0, msg.url)
        asyncio.get_running_loop().call_soon(
            lambda: asyncio.create_task(self.send_snapshot(peer)))
        return LearnRes.new(self.current_term, True)

    def get_next_peer_id(self):
        max_id = 0
        for i in self.peers:
            max_id = max(max_id, i.get_id(), self.id)
        return max_id + 1

    def learner_join(self, msg: LearnerJoinReq):
        learner_id = self.get_next_peer_id()
        for i in self.peers:
            if i.get_url() == msg.url:
                return LearnerJoinRes.new(self.current_term, learner_id)

        conf_data = ConfChangeData(
            ConfChangeType.add_node, learner_id, msg.url)
        target_index = self.logs.get_last_index() + 1
        entity = Entity(self.current_term, target_index,
                        EntityType.conf_change, conf_data)
        self.logs.append(entity)
        self.commit(target_index)
        self.apply()
        return LearnerJoinRes.new(self.current_term, learner_id)

    def leadership_transfer(self):
        target_id = 0
        latest_index = self.logs.get_last_index()
        for peer_id, index in self.match_index.items():
            if index >= self.last_applied and index >= latest_index:
                target_id = peer_id
                break
        if not target_id:
            raise praft.exceptions.CanNotDoOperation()

        target_peer = None
        for peer in self.peers:
            if peer.get_id() is target_id:
                target_peer = peer

        if not target_peer:
            raise praft.exceptions.CanNotDoOperation()

        msg = StartElectionReq.new(self.current_term, self.commit_index,
                                   latest_index, self.id, peer.id)
        res = peer.start_election(msg)
        return res.confirmed

    async def remove_node(self, url: T_URL):
        remove_node = None
        for i in self.peers:
            if i.get_url() == url:
                remove_node = i
                break
        if not remove_node:
            return

        target_index = self.logs.get_last_index() + 1
        entity = Entity(self.current_term, target_index,
                        EntityType.conf_change,
                        ConfChangeData(ConfChangeType.remove_node,
                                       remove_node.get_id(),
                                       remove_node.get_url()))
        self.logs.append(entity)
        self.match_index[self.id] = target_index
        tasks = set()
        try:
            for peer in self.peers:
                tasks.add(asyncio.create_task(self.peer_log_append(peer)))
            for peer_result in asyncio.as_completed(tasks):
                peer_result = await peer_result
                succ_peer_num = sum(map(lambda x: x >= target_index,
                                        self.match_index.values()))
                if succ_peer_num >= self.get_quorum():
                    break
            # 所有的请求都结束之后重新计算
            succ_peer_num = sum(map(lambda x: x >= target_index,
                                    self.match_index.values()))
            if succ_peer_num >= self.get_quorum():
                self.commit(target_index)
                self.apply()
            return
        except praft.exceptions.LeaderDemoted:
            raise praft.exceptions.LeaderDemoted()

    async def propose(self, data: object) -> object:
        target_index = self.logs.get_last_index() + 1
        entity = Entity(self.current_term, target_index, EntityType.normal,
                        data)
        self.logs.append(entity)
        self.match_index[self.id] = target_index
        tasks = set()
        try:
            for peer in self.peers:
                tasks.add(asyncio.create_task(self.peer_log_append(peer)))
            for peer_result in asyncio.as_completed(tasks):
                peer_result = await peer_result
                succ_peer_num = sum(map(lambda x: x >= target_index,
                                        self.match_index.values()))
                if succ_peer_num >= self.get_quorum():
                    break
            # 所有的请求都结束之后重新计算
            succ_peer_num = sum(map(lambda x: x >= target_index,
                                    self.match_index.values()))
            if succ_peer_num >= self.get_quorum():
                self.commit(target_index)
                self.apply()
            return data

        except praft.exceptions.LeaderDemoted:
            raise praft.exceptions.LeaderDemoted()

    def tick(self):
        if self.role_changed:
            return
        self.apply()
        loop = asyncio.get_running_loop()
        for peer in self.peers:
            asyncio.create_task(self.peer_log_append(peer))

        loop.call_later(
            self.get_heartbeat_interval(), self.tick)
