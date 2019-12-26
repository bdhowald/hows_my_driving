from dataclasses import dataclass
from typing import List, Optional

@dataclass(frozen=True)
class PlateQuery:
    """ Represents a plate query to be submitted to the open data apis """
    created_at: str
    message_id: Optional[int]
    message_source: str
    plate: str
    plate_types: Optional[List[str]]
    state: str
    username: Optional[str]

