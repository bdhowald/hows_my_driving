from dataclasses import dataclass
from typing import Any, List


@dataclass
class NonVehicleResponse:
    """ Represents the output of an event with no detected vehicles."""

    response_parts: List[Any]