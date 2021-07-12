# coding=utf-8
import os

from geoip import open_database
from sqlalchemy import create_engine

GEOIP_PATH = os.environ.get('GEOIP_PATH')
if GEOIP_PATH:
    GEOIP_DATABASE = GEOIP_PATH
else:
    GEOIP_DATABASE = os.path.join(
        os.path.dirname(
            os.path.abspath(__file__),
        ),
        'geoip_db.mmdb',
    )

SQLITE_PATH = os.environ.get('SQLITE_PATH')
if SQLITE_PATH:
    SQLITE_CONNECTION_STRING = f'sqlite:///{SQLITE_PATH}'
else:
    SQLITE_CONNECTION_STRING = 'sqlite:////data/mirrors.db'


class Engine:
    __instance = None

    @classmethod
    def get_instance(cls):
        if not cls.__instance:
            cls.__instance = create_engine(SQLITE_CONNECTION_STRING)
        return cls.__instance


class GeoIPEngine:
    __instance = None

    @classmethod
    def get_instance(cls):
        if not cls.__instance:
            cls.__instance = open_database(GEOIP_DATABASE)
        return cls.__instance
