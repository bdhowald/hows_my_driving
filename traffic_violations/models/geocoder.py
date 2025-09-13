from sqlalchemy import Column
from sqlalchemy import Index
from sqlalchemy import Integer
from sqlalchemy import String

from traffic_violations.models.base import Base

class Geocoder(Base):
  """Represents a geocoding service"""

  __tablename__ = 'geocoder'

  # columns
  id = Column(Integer, primary_key=True)
  name = Column(String(32), nullable=False)
