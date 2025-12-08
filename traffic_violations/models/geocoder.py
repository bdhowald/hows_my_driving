from sqlalchemy import Column
from sqlalchemy import String
from sqlalchemy.dialects import mysql


from traffic_violations.models.base import Base

class Geocoder(Base):
  """Represents a geocoding service"""

  __tablename__ = 'geocoder'

  # columns
  id = Column(mysql.TINYINT, primary_key=True)
  name = Column(String(32), nullable=False)
