from dataclasses import dataclass
from typing import Any


@dataclass
class VehicleResponse:
    """ Represents the output of a vehicle lookup."""

    response_parts: list[Any]