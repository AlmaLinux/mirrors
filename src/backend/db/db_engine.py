# coding=utf-8
import json
import os

import maxminddb
from flask import Flask
from flask_caching import Cache
from sqlalchemy import create_engine

GEOIP_PATH = os.environ.get('GEOIP_PATH')
ASN_PATH = os.environ.get('ASN_PATH')
SQLITE_PATH = os.environ.get('SQLITE_PATH')
REDIS_URI = os.environ.get('REDIS_URI')
REDIS_URI_RO = os.environ.get('REDIS_URI_RO')
REDIS_DB = 0
CONTINENT_PATH = os.environ.get('CONTINENT_PATH')

if GEOIP_PATH:
    GEOIP_DATABASE = GEOIP_PATH
else:
    GEOIP_DATABASE = os.path.join(
        os.path.dirname(
            os.path.abspath(__file__),
        ),
        'standard_location.mmdb',
    )
if ASN_PATH is not None:
    ASN_DATABASE = ASN_PATH
else:
    ASN_DATABASE = os.path.join(
        os.path.dirname(
            os.path.abspath(__file__),
        ),
        'asn.mmdb',
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


class ContinentEngine:
    __instance = None

    @classmethod
    def get_instance(cls):
        if not cls.__instance:
            f = open(CONTINENT_PATH)
            cls.__instance = json.load(f)
        return cls.__instance


class GeoEngine:
    __instance = None

    @classmethod
    def get_instance(cls):
        if not cls.__instance:
            cls.__instance = maxminddb.Reader(GEOIP_DATABASE)
        return cls.__instance


class AsnEngine:
    __instance = None

    @classmethod
    def get_instance(cls):
        if not cls.__instance:
            cls.__instance = maxminddb.Reader(ASN_DATABASE)
        return cls.__instance


class FlaskCacheEngine:
    __instance = None
    __instance_ro = None

    @classmethod
    def get_instance(cls, app: Flask = None, ro: bool = False):
        if cls.__instance is None:
            cls.__instance = Cache(
                config={
                    'CACHE_TYPE': 'RedisCache',
                    'CACHE_REDIS_URL': REDIS_URI,
                    'CACHE_REDIS_DB': REDIS_DB,
                }
            )
        if cls.__instance_ro is None:
            cls.__instance_ro = Cache(
                config={
                    'CACHE_TYPE': 'RedisCache',
                    'CACHE_REDIS_URL': REDIS_URI_RO,
                    'CACHE_REDIS_DB': REDIS_DB,
                }
            )
        if app is not None:
            cls.__instance.init_app(app)
            cls.__instance_ro.init_app(app)
        if ro:
            return cls.__instance_ro
        else:
            return cls.__instance
