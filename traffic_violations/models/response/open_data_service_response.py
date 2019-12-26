from dataclasses import dataclass
from typing import List, Optional, Tuple

from traffic_violations.models.response.open_data_service_plate_lookup import OpenDataServicePlateLookup


@dataclass(frozen=True)
class OpenDataServiceResponse:
    """ Represents a response from the NYC open data portal """
    success: bool

    data: Optional[OpenDataServicePlateLookup] = None
    message: Optional[str] = None