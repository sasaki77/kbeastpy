from typing import Optional, TypedDict, Union


class ConfigData(TypedDict):
    title: str
    details: str


class ConfigLeafMsg(TypedDict):
    user: str
    host: str
    description: str
    delay: Optional[int]
    count: Optional[int]
    filter: Optional[str]
    guidance: Optional[list[ConfigData]]
    displays: Optional[list[ConfigData]]
    commands: Optional[list[ConfigData]]
    actions: Optional[list[ConfigData]]


class ConfigNodeMsg(TypedDict):
    user: str
    host: str
    guidance: Optional[list[ConfigData]]
    displays: Optional[list[ConfigData]]
    commands: Optional[list[ConfigData]]
    actions: Optional[list[ConfigData]]


class DeleteMsg(TypedDict):
    user: str
    host: str
    delete: str


class TimeData(TypedDict):
    seconds: int
    nano: int


class StateLeafMsg(TypedDict):
    severity: str
    latch: bool
    message: str
    value: str
    time: TimeData
    current_severity: str
    current_message: str
    mode: str


class StateNodeMsg(TypedDict):
    severity: str
    mode: str


class CommandMsg(TypedDict):
    user: str
    host: str
    command: str


class TalkMsg(TypedDict):
    severity: str
    standout: bool
    talk: str


ConfigMsg = Union[ConfigLeafMsg, ConfigNodeMsg, DeleteMsg, None]
