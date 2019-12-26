from dataclasses import dataclass
from typing import Optional

@dataclass(frozen=True)
class CameraStreakData:
    """ Data representing a streak of camera violations over a
        12-month rolling period.
    """

    max_streak: int
    min_streak_date: Optional[str] = None
    max_streak_date: Optional[str] = None
