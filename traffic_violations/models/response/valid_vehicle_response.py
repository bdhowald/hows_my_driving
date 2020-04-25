from dataclasses import dataclass

from traffic_violations.models.response.open_data_service_plate_lookup \
    import OpenDataServicePlateLookup
from traffic_violations.models.response.vehicle_response \
    import VehicleResponse


@dataclass
class ValidVehicleResponse(VehicleResponse):
    """ Represents the output of a valid vehicle lookup:
    
    · its open data results, if a lookup was successful
    · its response string
    """

    error_on_lookup: bool
    plate_lookup: OpenDataServicePlateLookup
    success_on_lookup: bool
