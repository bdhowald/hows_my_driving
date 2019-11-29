from dataclasses import dataclass
from typing import List, Optional

@dataclass
class PlateQuery:
    """ Represents a plate query to be submitted to the open data apis """
    plate: str
    plate_types: Optional[List[str]]
    state: str

