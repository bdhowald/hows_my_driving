import logging
import os

from sqlalchemy import create_engine

LOG = logging.getLogger(__name__)


class DbService:

    def get_connection(self):
        connection = DbService.get_engine().connect()
        LOG.debug('engine connecting')

        return connection

    @classmethod
    def get_engine(cls):
        if hasattr(cls, 'engine'):
            return cls.engine
        else:

            password_str = os.environ[
                'MYSQL_PASSWORD'] if 'MYSQL_PASSWORD' in os.environ else ''

            # Create a engine for connecting to MySQL
            cls.engine = create_engine(
                f"mysql+pymysql://{os.environ['MYSQL_USER']}:{password_str}@localhost/{os.environ['MYSQL_DATABASE']}?charset=utf8")

            return cls.engine
