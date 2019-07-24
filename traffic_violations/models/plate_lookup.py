from dataclasses import dataclass
from typing import List, Optional

@dataclass
class PlateLookup:
    """ Represents a plate query to be submitted to the open data apis """
    created_at: str
    message_id: Optional[str]
    message_type: Optional[str]

    plate: str
    plate_types: List[str]
    state: str

    username: Optional[str]
