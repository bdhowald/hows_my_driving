from sqlalchemy import Column, Index, Integer, String
from sqlalchemy.dialects.mysql import TINYINT

from traffic_violations.models.base import Base


class RepeatCameraOffender(Base):
    __tablename__ = 'repeat_camera_offenders'

    # columns
    id = Column(Integer, primary_key=True)
    plate_id = Column(String(20), nullable=False)
    state = Column(String(2), nullable=False)
    total_camera_violations = Column(Integer, nullable=False)
    red_light_camera_violations = Column(Integer, nullable=False)
    speed_camera_violations = Column(Integer, nullable=False)
    times_featured = Column(TINYINT, nullable=False, default=0)

    # indices
    __table_args__ = (
        Index('total_camera_violations_index', 'total_camera_violations'),
        Index('red_light_camera_violations_index', 'red_light_camera_violations'),
        Index('speed_camera_violations_index', 'speed_camera_violations'),
        Index('plate_id_state_index', 'plate_id', 'state'),
        Index('state_index', 'state'),
        Index('times_featured_index', 'times_featured'),
    )
