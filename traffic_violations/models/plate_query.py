from dataclasses import dataclass
from typing import Optional

@dataclass(frozen=True)
class PlateQuery:
    """ Represents a plate query to be submitted to the open data apis """
    created_at: str
    message_source: str
    plate: str
    state: str
    message_id: Optional[int] = None
    plate_types: Optional[str] = None
    username: Optional[str] = None

