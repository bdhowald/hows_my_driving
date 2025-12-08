from sqlalchemy import Column
from sqlalchemy import Index
from sqlalchemy import String
from sqlalchemy.dialects import mysql

from traffic_violations.models.base import Base

class GeocoderLocationType(Base):
    """Represents a Geocode Location Type"""

    __tablename__ = 'geocoder_location_types'

    # columns
    id = Column(mysql.TINYINT, primary_key=True)
    geocoder_id = Column(mysql.TINYINT, nullable=False)
    name = Column(String(20), nullable=False)

  # indices
    __table_args__ = (
        Index('geocoder_location_types_geocoder_id_fk_1', 'geocoder_id'),
    )
