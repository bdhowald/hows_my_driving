from sqlalchemy import Boolean, Column, DateTime, Integer, String
from sqlalchemy.dialects.mysql import BIGINT, INTEGER

from traffic_violations.models.base import Base


class TwitterEvent(Base):
    """ Represents a record of a twitter mention of @HowsMyDrivingNY """

    __tablename__ = 'twitter_events'

    # columns
    id = Column(Integer, primary_key=True)
    event_type = Column(String(20), nullable=False)
    event_id = Column(BIGINT, nullable=False)
    user_handle = Column(String(30), nullable=False)
    user_id = Column(BIGINT, nullable=False)
    event_text = Column(String(560), nullable=False)
    created_at = Column(BIGINT, nullable=False)
    in_reply_to_message_id = Column(BIGINT, nullable=True)
    last_failed_at_time = Column(DateTime, nullable=True)
    location = Column(String(100), nullable=True)
    num_times_failed = Column(INTEGER(unsigned=True), default=0, nullable=False)
    user_mention_ids = Column(String(560), nullable=True)
    user_mentions = Column(String(560), nullable=True)
    response_in_progress = Column(Boolean, default=False, nullable=False)
    responded_to = Column(Boolean, default=False, nullable=False)
    error_on_lookup = Column(Boolean, default=False, nullable=False)
    is_duplicate = Column(Boolean, default=False, nullable=False)
    detected_via_account_activity_api = Column(Boolean, default=True, nullable=False)
    user_favorited_non_follower_reply = Column(Boolean, default=False, nullable=False)
