import json
import os

from typing import NamedTuple

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm.query import Query
from sqlalchemy.orm.scoping import ScopedSession
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.ext.declarative import declarative_base

# from common import settings
# from common.db.utils import serialization

_DB_CONN_CACHE = None
_DB_READONLY_SESSION = None


class DatabaseConnection(NamedTuple):
    engine: Engine
    session: ScopedSession

password_str = os.environ[
                'MYSQL_PASSWORD'] if 'MYSQL_PASSWORD' in os.environ else ''
MYSQL_URI = f"mysql+pymysql://{os.environ['MYSQL_USER']}:{password_str}@localhost/{os.environ['MYSQL_DATABASE']}?charset=utf8mb4"


def _create_mysql_engine(readonly: bool = False) -> Engine:
    """Creates an engine wrapping MySQL db.
    :return: an engine around the MySQL db.
    """
    return create_engine(
        MYSQL_URI,
        convert_unicode=True)


def _create_mysql_scoped_session(engine: Engine) -> ScopedSession:
    """Returns a scoped_session to the MySQL db connected to the given
    engine.
    :param engine: an engine connected to the target db.
    :return: a scoped_session to the db connected to the engine.
    """
    return scoped_session(
        sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=engine,
            expire_on_commit=False))


def init_database() -> DatabaseConnection:
    global _DB_CONN_CACHE  # pylint: disable=global-statement
    if not _DB_CONN_CACHE:
        engine = _create_mysql_engine()
        session = _create_mysql_scoped_session(engine=engine)
        db = DatabaseConnection(engine=engine, session=session)
        _DB_CONN_CACHE = db

    return _DB_CONN_CACHE


DeclarativeBase = declarative_base()
DeclarativeBase.query: Query = init_database().session.query_property()