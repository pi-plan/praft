from enum import Enum, unique
from abc import ABCMeta, abstractmethod
from typing import List, Tuple

from praft.log import Entity
from praft.exceptions import MessageDataError


@unique
class MessageType(Enum):
    ERROR: int = 0  # 遇到异常
    HEARTBEAT_REQ: int = 1  # 心跳
    HEARTBEAT_RES: int = 2
    LEARN_REQ: int = 3  # learner 开始学习
    LEARN_RES: int = 4
    PREV_VOTE_REQ: int = 5  # 预投票
    PREV_VOTE_RES: int = 6
    VOTE_REQ: int = 7  # 投票
    VOTE_RES: int = 8
    START_ELECTION_REQ: int = 9  # 开始选举
    START_ELECTION_RES: int = 10
    INSTALL_SNAPSHOT_REQ: int = 11  # 安装快照
    INSTALL_SNAPSHOT_RES: int = 12
    LEARNER_JOIN_REQ: int = 13  # 加入集群
    LEARNER_JOIN_RES: int = 14

    @classmethod
    def get_type(cls, t: int) -> 'MessageType':
        for i in list(cls):
            if t == i.value:
                return i
        raise MessageDataError("field `type` not in MessageType.")


class Message(metaclass=ABCMeta):
    type: MessageType
    come_from: int
    to: int
    term: int  # leader’s term

    @classmethod
    @abstractmethod
    def new(cls) -> 'Message':
        pass

    def set_from_to(self, come_from: int = None, to: int = None):
        if come_from:
            self.come_from = come_from

        if to:
            self.to = to


class ErrorMessage(Message):
    error_no: int
    data: Message

    @classmethod
    def new(cls, term: int, error_no: int, data: str,
            come_from: int = None, to: int = None) -> 'ErrorMessage':
        msg = cls()
        msg.type = MessageType.ERROR
        msg.term = term
        msg.error_no = error_no
        msg.data = data
        msg.come_from = come_from
        msg.to = to
        return msg


class HeartBeatReq(Message):
    leader_id: int  # leader id
    prev_log_index: int  # index of log entry immediately preceding new ones
    prev_log_term: int  # term of prevLogIndex entry
    # log entries to store.
    # empty for heartbeat; may send more than one for efficiency.
    entries: List[Entity]
    leader_commit: int
    is_learner: bool

    @classmethod
    def new(cls, term: int, leader_id: int, prev_log_index: int,
            prev_log_term: int, leader_commit: int, entries: List[object],
            is_learner: bool = False,
            come_from: int = None, to: int = None) -> 'HeartBeatReq':
        msg = cls()
        msg.type = MessageType.HEARTBEAT_REQ
        msg.term = term
        msg.leader_id = leader_id
        msg.prev_log_index = prev_log_index
        msg.prev_log_term = prev_log_term
        msg.leader_commit = leader_commit
        if entries and isinstance(entries[0], dict):
            msg.entries = [Entity(i["term"], i["index"], i["type"], i["data"])
                           for i in entries]
        else:
            msg.entries = entries
        msg.is_learner = is_learner
        msg.come_from = come_from
        msg.to = to
        return msg


class HeartBeatRes(Message):
    success: bool

    @classmethod
    def new(cls, term: int, success: bool,
            come_from: int = None, to: int = None) -> 'HeartBeatRes':
        msg = cls()
        msg.type = MessageType.HEARTBEAT_RES
        msg.term = term
        msg.success = success
        msg.come_from = come_from
        msg.to = to
        return msg


class LearnReq(Message):
    url: Tuple[str, int]

    @classmethod
    def new(cls, term: int, url: Tuple[str, int]) -> 'LearnReq':
        msg = cls()
        msg.type = MessageType.LEARN_REQ
        msg.term = term
        msg.url = url
        return msg


class LearnRes(Message):
    confirmed: bool

    @classmethod
    def new(cls, term: int, confirmed: bool) -> 'LearnRes':
        msg = cls()
        msg.type = MessageType.LEARN_RES
        msg.term = term
        msg.confirmed = confirmed
        return msg


class VoteReq(Message):
    candidate_id: int
    last_log_index: int
    last_log_term: int

    @classmethod
    def new(cls, term: int, candidate_id: int, last_log_index: int,
            last_log_term: int,
            come_from: int = None, to: int = None) -> 'VoteReq':
        msg = cls()
        msg.type = MessageType.VOTE_REQ
        msg.term = term
        msg.candidate_id = candidate_id
        msg.last_log_index = last_log_index
        msg.last_log_term = last_log_term
        msg.come_from = come_from
        msg.to = to
        return msg


class VoteRes(Message):
    vote_granted: bool  # true means candidate received vote

    @classmethod
    def new(cls, term: int, vote_granted: bool,
            come_from: int = None, to: int = None) -> 'VoteRes':
        msg = cls()
        msg.type = MessageType.VOTE_RES
        msg.term = term
        msg.vote_granted = vote_granted
        msg.come_from = come_from
        msg.to = to
        return msg


class PrevVoteReq(Message):
    candidate_id: int
    last_log_index: int
    last_log_term: int

    @classmethod
    def new(cls, term: int, candidate_id: int, last_log_index: int,
            last_log_term: int,
            come_from: int = None, to: int = None) -> 'PrevVoteReq':
        msg = cls()
        msg.type = MessageType.PREV_VOTE_REQ
        msg.term = term
        msg.candidate_id = candidate_id
        msg.last_log_index = last_log_index
        msg.last_log_term = last_log_term
        msg.come_from = come_from
        msg.to = to
        return msg


class PrevVoteRes(Message):
    vote_granted: bool  # true means candidate received vote

    @classmethod
    def new(cls, term: int, vote_granted: bool,
            come_from: int = None, to: int = None) -> 'PrevVoteRes':
        msg = cls()
        msg.type = MessageType.PREV_VOTE_RES
        msg.term = term
        msg.vote_granted = vote_granted
        msg.come_from = come_from
        msg.to = to
        return msg


class StartElectionReq(Message):
    leader_commit: int
    leader_latest_index: int

    @classmethod
    def new(cls, term: int, leader_commit: int, leader_latest_index: int,
            come_from: int = None, to: int = None) -> 'StartElectionReq':
        msg = cls()
        msg.type = MessageType.START_ELECTION_REQ
        msg.term = term
        msg.leader_commit = leader_commit
        msg.leader_latest_index = leader_latest_index
        msg.come_from = come_from
        msg.to = to
        return msg


class StartElectionRes(Message):
    confirmed: bool

    @classmethod
    def new(cls, term: int, confirmed: bool,
            come_from: int = None, to: int = None) -> 'StartElectionRes':
        msg = cls()
        msg.type = MessageType.START_ELECTION_RES
        msg.term = term
        msg.confirmed = confirmed
        msg.come_from = come_from
        msg.to = to
        return msg


class InstallSnapshotReq(Message):
    leader_id: int
    last_included_index: int
    last_included_term: int
    offset: int
    data: List[object]
    done: bool
    conf: dict
    peers: List[Tuple[str, int]]

    @classmethod
    def new(cls, term, leader_id: int, last_included_index: int,
            last_included_term: int, offset: int, data: List[object],
            done: bool, conf: dict, peers: List[Tuple[int, str, int]],
            come_from: int = None, to: int = None) -> 'InstallSnapshotReq':
        msg = cls()
        msg.type = MessageType.INSTALL_SNAPSHOT_REQ
        msg.term = term
        msg.leader_id = leader_id
        msg.last_included_index = last_included_index
        msg.last_included_term = last_included_term
        msg.offset = offset
        msg.data = data
        msg.done = done
        msg.conf = conf
        msg.peers = peers
        msg.come_from = come_from
        msg.to = to
        return msg


class InstallSnapshotRes(Message):
    @classmethod
    def new(cls, term: int) -> 'InstallSnapshotRes':
        msg = cls()
        msg.type = MessageType.INSTALL_SNAPSHOT_RES
        msg.term = term
        return msg


class LearnerJoinReq(Message):
    url: Tuple[str, int]

    @classmethod
    def new(cls, term: int, url: Tuple[str, int]) -> 'LearnerJoinReq':
        msg = cls()
        msg.term = term
        msg.type = MessageType.LEARNER_JOIN_REQ
        msg.url = url
        return msg


class LearnerJoinRes(Message):
    id: int

    @classmethod
    def new(cls, term: int, id: int) -> 'LearnerJoinRes':
        msg = cls()
        msg.type = MessageType.LEARNER_JOIN_RES
        msg.term = term
        msg.id = id
        return msg


class MessageFactory(object):

    msg_type2class = {
        MessageType.HEARTBEAT_REQ: HeartBeatReq,
        MessageType.HEARTBEAT_RES: HeartBeatRes,
        MessageType.LEARN_REQ: LearnReq,
        MessageType.LEARN_RES: LearnRes,
        MessageType.PREV_VOTE_REQ: PrevVoteReq,
        MessageType.PREV_VOTE_RES: PrevVoteRes,
        MessageType.VOTE_REQ: VoteReq,
        MessageType.VOTE_RES: VoteRes,
        MessageType.START_ELECTION_REQ: StartElectionReq,
        MessageType.START_ELECTION_RES: StartElectionRes,
        MessageType.INSTALL_SNAPSHOT_REQ: InstallSnapshotReq,
        MessageType.INSTALL_SNAPSHOT_RES: InstallSnapshotRes,
        MessageType.LEARNER_JOIN_REQ: LearnerJoinReq,
        MessageType.LEARNER_JOIN_RES: LearnerJoinRes,
    }

    @classmethod
    def get_instance(cls, data: dict):
        for i in ["type", "term"]:
            if i not in data.keys():
                raise MessageDataError("field `{}` not in message.".format(i))
        msg_type = MessageType.get_type(data["type"])
        del(data["type"])
        return cls.msg_type2class[msg_type].new(**data)
