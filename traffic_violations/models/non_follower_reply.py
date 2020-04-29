from sqlalchemy import Column, Integer, String
from sqlalchemy.dialects.mysql import BIGINT

from traffic_violations.models.base import Base


class NonFollowerReply(Base):
    """Represents a reply to a non-follower of @HowsMyDrivingNY
    
    Used to keep track of which lookups still need to be responded to."""

    __tablename__ = 'non_follower_replies'

    # columns
    id = Column(Integer, primary_key=True)

    created_at = Column(BIGINT, nullable=False)
    event_type = Column(String(20), nullable=False)
    event_id = Column(BIGINT, nullable=False)
    in_reply_to_message_id = Column(BIGINT, nullable=True)
    user_handle = Column(String(30), nullable=False)
    user_id = Column(BIGINT, nullable=False)