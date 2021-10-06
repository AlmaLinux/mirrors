# coding=utf-8
import json

from typing import (
    AnyStr,
    Optional,
    List,
)

from db.db_engine import RedisEngine
from db.models import (
    MirrorData,
    DataClassesJSONEncoder,
)
from common.sentry import (
    get_logger,
)

logger = get_logger(__name__)

CACHE_EXPIRED_TIME = 24 * 3600  # 24 hours


def get_mirrors_from_cache(
        key: AnyStr,
) -> Optional[List[MirrorData]]:
    """
    Get a cached list of mirrors for specified IP
    """
    key = str(key)
    redis_engine = RedisEngine.get_instance()
    mirrors_string = redis_engine.get(key)
    if mirrors_string is not None:
        mirrors_json = json.loads(
            mirrors_string,
        )
        return [MirrorData.load_from_json(mirror_json)
                for mirror_json in mirrors_json]


def set_mirrors_to_cache(
        key: AnyStr,
        mirrors: List[MirrorData],
) -> None:
    """
    Save a mirror list for specified IP to cache
    """
    key = str(key)
    redis_engine = RedisEngine.get_instance()
    mirrors = json.dumps(mirrors, cls=DataClassesJSONEncoder)
    redis_engine.set(
        key,
        mirrors,
        CACHE_EXPIRED_TIME,
    )


def get_url_types_from_cache() -> List[AnyStr]:
    redis_engine = RedisEngine.get_instance()
    url_types_string = redis_engine.get('url_types')
    if url_types_string is not None:
        return json.loads(url_types_string)


def set_url_types_to_cache(url_types: List[AnyStr]):
    redis_engine = RedisEngine.get_instance()
    redis_engine.set('url_types', json.dumps(url_types), CACHE_EXPIRED_TIME)
