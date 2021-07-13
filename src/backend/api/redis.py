# coding=utf-8
import json

from typing import AnyStr, Optional, List, Union, Dict

from db.db_engine import RedisEngine

CACHE_EXPIRED_TIME = 24 * 3600  # 24 hours


def _get_value(key: AnyStr) -> Optional[AnyStr]:
    """
    Get a Redis value by a key
    """
    redis_engine = RedisEngine.get_instance()

    return redis_engine.get(key)


def _set_value(
        key: AnyStr,
        value: AnyStr,
        exp_time: int = CACHE_EXPIRED_TIME,
):
    """
    Set a Redis value by a key and with expired time
    """
    redis_engine = RedisEngine.get_instance()
    redis_engine.set(
        name=key,
        value=value,
        ex=exp_time,
    )


def get_mirrors_from_cache(
        key: AnyStr
) -> Optional[List[Dict[AnyStr, Union[AnyStr, float, Dict, List]]]]:
    """
    Get a cached list of mirrors for specified IP
    """
    redis_engine = RedisEngine.get_instance()
    redis_key = _get_value(key)
    if redis_key is not None:
        mirrors = redis_engine.zrange(redis_key, 0, -1)
        return [json.loads(mirror) for mirror in mirrors]
    else:
        redis_engine.zremrangebyscore(key, 0, '+inf')


def set_mirrors_to_cache(
        key: AnyStr,
        mirrors: List[Dict[AnyStr, Union[AnyStr, float, Dict, List]]],
) -> None:
    """
    Save a mirror list for specified IP to cache
    """
    redis_engine = RedisEngine.get_instance()
    _set_value(
        key=key,
        value=key,
    )
    # remove old record in case it exists
    redis_engine.zremrangebyscore(key, 0, 'inf+')
    redis_engine.zadd(
        key,
        {json.dumps(mirror): index for index, mirror in enumerate(mirrors)},
    )
