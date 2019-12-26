from dataclasses import dataclass
from typing import List, Optional

@dataclass(frozen=True)
class Vehicle:
    """ Represents a potential vehicle to be queried """

    valid_plate: bool
    original_string: Optional[str] = None
    plate: Optional[str] = None
    plate_types: Optional[str] = None
    state: Optional[str] = None
