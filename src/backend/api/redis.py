# coding=utf-8
import json

from typing import AnyStr, Optional, List, Union, Dict

from db.db_engine import RedisEngine

CACHE_EXPIRED_TIME = 24 * 3600  # 24 hours


def get_mirrors_from_cache(
        key: AnyStr
) -> Optional[List[Dict[AnyStr, Union[AnyStr, float, Dict, List]]]]:
    """
    Get a cached list of mirrors for specified IP
    """
    key = str(key)
    redis_engine = RedisEngine.get_instance()
    mirrors_string = redis_engine.get(key)
    if mirrors_string is not None:
        return json.loads(mirrors_string)


def set_mirrors_to_cache(
        key: AnyStr,
        mirrors: List[Dict[AnyStr, Union[AnyStr, float, Dict, List]]],
) -> None:
    """
    Save a mirror list for specified IP to cache
    """
    key = str(key)
    redis_engine = RedisEngine.get_instance()
    redis_engine.set(
        key,
        json.dumps(mirrors),
        CACHE_EXPIRED_TIME,
    )
