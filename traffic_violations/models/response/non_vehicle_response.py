from dataclasses import dataclass
from typing import Any


@dataclass
class NonVehicleResponse:
    """ Represents the output of an event with no detected vehicles."""

    response_parts: list[Any]