from sqlalchemy import Boolean, Column, Index, Integer, String
from traffic_violations.models.base import Base


class Covid19CameraOffender(Base):
    __tablename__ = 'covid_19_camera_offenders'

    # columns
    id = Column(Integer, primary_key=True)
    plate_id = Column(String(20), nullable=False)
    state = Column(String(2), nullable=False)
    red_light_camera_violations = Column(Integer, nullable=False)
    speed_camera_violations = Column(Integer, nullable=False)
    count_as_queried = Column(Boolean, default=True, nullable=False)
