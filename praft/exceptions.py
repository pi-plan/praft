class PRaftException(Exception):
    """
    Base Exception
    """
    pass


class UnknownError(PRaftException):
    pass


class MessageDataError(PRaftException):
    pass


class LeaderDemoted(PRaftException):
    pass


class NeedLeaderHandle(PRaftException):
    pass


class RoleCanNotDo(PRaftException):
    pass


class NotFoundLeader(PRaftException):
    pass


class CanNotDoOperation(PRaftException):
    pass


class LogHasDeleted(PRaftException):
    pass
