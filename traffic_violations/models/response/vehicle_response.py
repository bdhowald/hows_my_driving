from dataclasses import dataclass
from typing import Any, List


@dataclass
class VehicleResponse:
    """ Represents the output of a vehicle lookup."""

    response_parts: List[Any]