
from enum import Enum

class MessageType(Enum):
    EMPTY = 0
    STATE = 1
    SESSION = 2
    INFO = 3
    CARS = 4
    PITS = 5
    REGISTER_PROVIDER = 6
    MANIFESTS = 7
    STATE_DELTA = 8
    

class Message:
    type = None
    timestamp = 0
    payload = None
    
    def __init__(self, type=None, payload=None) -> None:
        self.type = type        
        self.payload = payload


