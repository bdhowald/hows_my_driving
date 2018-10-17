import pdb
import re
import os

from sqlalchemy import create_engine

class DbService:

    def __init__(self, logger):
        self.logger = logger

    def __enter__(self):
        self.connection = DbService.get_engine().connect()
        self.logger.debug('engine connecting')

        return self.connection

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.logger.debug('engine disconnecting')
        self.connection.close()


    @classmethod
    def get_engine(cls):
        if hasattr(cls, 'engine'):
            return cls.engine
        else:

            password_str = os.environ['MYSQL_PASSWORD'] if 'MYSQL_PASSWORD' in os.environ else ''

            # Create a engine for connecting to MySQL
            cls.engine = create_engine('mysql+pymysql://{}:'.format(os.environ['MYSQL_USER']) + password_str + '@localhost/{}?charset=utf8'.format(os.environ['MYSQL_DATABASE']))

            return cls.engine

