from enum import Enum, auto
from typing import NotRequired, TypedDict, Union


class MsgFormat(Enum):
    CONFIG_LEAF = auto()
    CONFIG_NODE = auto()
    CONFIG_NONE = auto()
    DELETE = auto()
    STATE_LEAF = auto()
    STATE_NODE = auto()
    COMMAND = auto()
    TALK = auto()


class ConfigData(TypedDict):
    title: str
    details: str


class ConfigLeafMsg(TypedDict):
    user: str
    host: str
    description: str
    enabled: NotRequired[bool]
    delay: NotRequired[int]
    count: NotRequired[int]
    filter: NotRequired[str]
    guidance: NotRequired[list[ConfigData]]
    displays: NotRequired[list[ConfigData]]
    commands: NotRequired[list[ConfigData]]
    actions: NotRequired[list[ConfigData]]


class ConfigNodeMsg(TypedDict):
    user: str
    host: str
    guidance: NotRequired[list[ConfigData]]
    displays: NotRequired[list[ConfigData]]
    commands: NotRequired[list[ConfigData]]
    actions: NotRequired[list[ConfigData]]


class DeleteMsg(TypedDict):
    user: str
    host: str
    delete: str


class TimeData(TypedDict):
    seconds: int
    nano: int


class StateLeafMsg(TypedDict):
    severity: str
    message: str
    value: str
    time: TimeData
    current_severity: str
    current_message: str
    mode: NotRequired[str]
    latch: NotRequired[bool]


class StateNodeMsg(TypedDict):
    severity: str
    mode: NotRequired[str]


class CommandMsg(TypedDict):
    user: str
    host: str
    command: str


class TalkMsg(TypedDict):
    severity: str
    standout: bool
    talk: str


ConfigMsg = Union[ConfigLeafMsg, ConfigNodeMsg, DeleteMsg, None]
ConfigStateMsg = Union[
    ConfigLeafMsg, ConfigNodeMsg, DeleteMsg, StateLeafMsg, StateNodeMsg, None
]

Msg = Union[
    ConfigLeafMsg,
    ConfigNodeMsg,
    DeleteMsg,
    StateLeafMsg,
    StateNodeMsg,
    CommandMsg,
    TalkMsg,
    None,
]
