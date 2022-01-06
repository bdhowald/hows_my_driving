from dataclasses import dataclass
from typing import Optional, Tuple

from traffic_violations.models.camera_streak_data import CameraStreakData
from traffic_violations.models.fine_data import FineData


@dataclass(frozen=True)
class OpenDataServicePlateLookup:
    """ Represents the results of a plate query submitted to the open data apis """
    boroughs: list[Tuple[str, int]]
    fines: FineData
    num_violations: int
    plate: str
    plate_types: Optional[str]
    state: str
    violations: list[Tuple[str, int]]
    years: list[Tuple[str, int]]

    camera_streak_data: dict[str, CameraStreakData] = None
