# coding=utf-8
import os

from flask import Flask
from geoip2.database import Reader
from sqlalchemy import create_engine
from flask_caching import Cache

GEOIP_PATH = os.environ.get('GEOIP_PATH')
ASN_PATH = os.environ.get('ASN_PATH')
SQLITE_PATH = os.environ.get('SQLITE_PATH')
REDIS_URI = os.environ.get('REDIS_URI')
REDIS_DB = 0

if GEOIP_PATH:
    GEOIP_DATABASE = GEOIP_PATH
else:
    GEOIP_DATABASE = os.path.join(
        os.path.dirname(
            os.path.abspath(__file__),
        ),
        'geoip_db.mmdb',
    )
if ASN_PATH is not None:
    ASN_DATABASE = ASN_PATH
else:
    ASN_DATABASE = os.path.join(
        os.path.dirname(
            os.path.abspath(__file__),
        ),
        'asn_db.mmdb',
    )

if SQLITE_PATH is not None:
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
            cls.__instance = Reader(GEOIP_DATABASE)
        return cls.__instance


class AsnEngine:
    __instance = None

    @classmethod
    def get_instance(cls):
        if not cls.__instance:
            cls.__instance = Reader(ASN_DATABASE)
        return cls.__instance


class FlaskCacheEngine:
    __instance = None

    cache_config = {
        'CACHE_TYPE': 'RedisCache',
        'CACHE_REDIS_URL': f'{REDIS_URI}',
        'CACHE_REDIS_DB': REDIS_DB,
    }

    @classmethod
    def get_instance(cls, app: Flask = None):
        if cls.__instance is None:
            cls.__instance = Cache(config=cls.cache_config)
        if app is not None:
            cls.__instance.init_app(app)
        return cls.__instance

