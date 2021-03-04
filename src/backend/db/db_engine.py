# coding=utf-8
import os

from geoip import open_database
from sqlalchemy import create_engine

GEOIP_DATABASE = os.path.join(
    os.path.dirname(
        os.path.abspath(__file__),
    ),
    'geoip_db.mmdb',
)
POSTGRES_USER = os.environ.get('POSTGRES_USER')
POSTGRES_DB = os.environ.get('POSTGRES_DB')
POSTGRES_PASSWORD = os.environ.get('POSTGRES_PASSWORD')
POSTGRES_HOST = os.environ.get('POSTGRES_HOST')
POSTGRES_CONNECTION_PATH = f'postgresql://{POSTGRES_USER}:' \
                           f'{POSTGRES_PASSWORD}@' \
                           f'{POSTGRES_HOST}/{POSTGRES_DB}'


class Engine:
    __instance = None

    @classmethod
    def get_instance(cls):
        if not cls.__instance:
            cls.__instance = create_engine(POSTGRES_CONNECTION_PATH)
        return cls.__instance


class GeoIPEngine:
    __instance = None

    @classmethod
    def get_instance(cls):
        if not cls.__instance:
            cls.__instance = open_database(GEOIP_DATABASE)
        return cls.__instance
