# coding=utf-8
import asyncio
import itertools
import os
import time
from inspect import signature
from urllib.parse import urljoin

import dateparser

from api.mirror_processor import MirrorProcessor
from db.db_engine import FlaskCacheEngine
from yaml_snippets.utils import (
    get_config,
    get_mirrors_info,
)
from api.redis import (
    _generate_redis_key_for_the_mirrors_list,
    MIRRORS_LIST_EXPIRED_TIME,
)
from api.utils import (
    get_aws_subnets,
    get_azure_subnets,
)
from api.handlers import get_all_mirrors_db

from db.models import (
    Url,
    Mirror,
    Subnet,
)
from db.utils import session_scope
from common.sentry import get_logger

logger = get_logger(__name__)
cache = FlaskCacheEngine.get_instance()


LENGTH_GEO_MIRRORS_LIST = 10
LENGTH_CLOUD_MIRRORS_LIST = 10
SERVICE_CONFIG_PATH = os.path.join(
    os.environ['CONFIG_ROOT'],
    'mirrors/updates/config.yml'
)
SERVICE_CONFIG_JSON_SCHEMA_DIR_PATH = os.path.join(
    os.environ['SOURCE_PATH'],
    'src/backend/yaml_snippets/json_schemas/service_config'
)
MIRROR_CONFIG_JSON_SCHEMA_DIR_PATH = os.path.join(
    os.environ['SOURCE_PATH'],
    'src/backend/yaml_snippets/json_schemas/mirror_config'
)

from flask_bs4 import Bootstrap

from flask import (
    Flask
)

app = Flask('app')
app.url_map.strict_slashes = False
Bootstrap(app)
logger = get_logger(__name__)
# init_sentry_client()
cache = FlaskCacheEngine.get_instance(app)

async def update_mirrors_handler() -> str:
    main_config = get_config(
        logger=logger,
        path_to_config=SERVICE_CONFIG_PATH,
        path_to_json_schema=SERVICE_CONFIG_JSON_SCHEMA_DIR_PATH,
    )
    mirrors_dir = os.path.join(
        os.getenv('CONFIG_ROOT'),
        'mirrors/updates',
        main_config.mirrors_dir,
    )
    all_mirrors = get_mirrors_info(
        mirrors_dir=mirrors_dir,
        logger=logger,
        main_config=main_config,
        path_to_json_schema=MIRROR_CONFIG_JSON_SCHEMA_DIR_PATH,
    )

    time1 = time.time()
    message = 'Update of the mirrors list is finished at "%s"'
    try:
        logger.info('Update of the mirrors list is started')
        mirror_check_sem = asyncio.Semaphore(20)
        mirrors_len = len(all_mirrors)
        async with mirror_check_sem, MirrorProcessor(
                logger=logger,
        ) as mirror_processor:  # type: MirrorProcessor
            mirror_iso_uris = mirror_processor.get_mirror_iso_uris(
                versions=set(main_config.versions) -
                         set(main_config.duplicated_versions),
                arches=main_config.arches
            )
            subnets = await get_aws_subnets(
                http_session=mirror_processor.client
            )
            subnets.update(
                await get_azure_subnets(
                    http_session=mirror_processor.client
                ),
            )
            tasks = [
                check_mirror(
                    mirror_check_sem=mirror_check_sem,
                    mirror_info=mirror_info,
                    main_config=main_config,
                    mirror_iso_uris=mirror_iso_uris,
                    subnets=subnets
                )
                for mirror_info in all_mirrors
            ]
        await asyncio.gather(*tasks)

        with session_scope() as db_session:
            db_session.query(Mirror).delete()
            db_session.query(Url).delete()
            db_session.query(Subnet).delete()
            for mirror_info in all_mirrors:
                urls_to_create = [
                    Url(
                        url=url,
                        type=url_type,
                    ) for url_type, url in mirror_info.urls.items()
                ]
                db_session.add_all(urls_to_create)
                mirror_to_create = Mirror(
                    name=mirror_info.name,
                    continent=mirror_info.geolocation.continent,
                    country=mirror_info.geolocation.country,
                    state=mirror_info.geolocation.state_province,
                    city=mirror_info.geolocation.city,
                    ip=mirror_info.ip,
                    ipv6=mirror_info.ipv6,
                    latitude=mirror_info.location.latitude,
                    longitude=mirror_info.location.longitude,
                    status=mirror_info.status,
                    update_frequency=dateparser.parse(
                        mirror_info.update_frequency
                    ),
                    sponsor_name=mirror_info.sponsor_name,
                    mirror_url=mirror_info.mirror_url,
                    iso_url=mirror_info.iso_url,
                    sponsor_url=mirror_info.sponsor_url,
                    email=mirror_info.email,
                    cloud_type=mirror_info.cloud_type,
                    cloud_region=mirror_info.cloud_region,
                    urls=urls_to_create,
                    private=mirror_info.private,
                    monopoly=mirror_info.monopoly,
                    asn=','.join(mirror_info.asn),
                    has_full_iso_set=mirror_info.has_full_iso_set,
                )
                if mirror_info.subnets:
                    subnets_to_create = [
                        Subnet(
                            subnet=subnet,
                        ) for subnet in mirror_info.subnets
                    ]
                    db_session.add_all(subnets_to_create)
                    mirror_to_create.subnets = subnets_to_create
                db_session.add(mirror_to_create)
        # update all mirrors lists in the Redis cache
        for args in itertools.product(
                (True, False),
                repeat=len(signature(get_all_mirrors_db).parameters),
        ):
            cache_key = _generate_redis_key_for_the_mirrors_list(*args)
            cache.delete(cache_key)
            get_all_mirrors_db(*args)
    finally:
        logger.info(
            'Update of the mirrors list is finished at "%s"',
            time.time() - time1,
            )
    return message % (time.time() - time1)


async def check_mirror(mirror_check_sem, mirror_info, main_config, mirror_iso_uris, subnets):
    async with mirror_check_sem:
        async with MirrorProcessor(
                logger=logger,
        ) as mirror_processor:
            await mirror_processor.set_ip_for_mirror(
                mirror_info=mirror_info
            )
            if mirror_info.ip not in ('Unknown', None):
                await mirror_processor.set_status_of_mirror(
                    main_config=main_config,
                    mirror_info=mirror_info,
                )
            await mirror_processor.set_iso_url(
                mirror_info=mirror_info
            )
            if mirror_info.status in ('ok', 'expired') and mirror_info.ip not in ('Unknown', None):
                await mirror_processor.set_subnets_for_cloud_mirror(
                    subnets=subnets,
                    mirror_info=mirror_info
                )
                await mirror_processor.set_ipv6_support_of_mirror(
                    mirror_info=mirror_info
                )

                await mirror_processor.set_geo_and_location_data_from_db(
                    mirror_info=mirror_info
                )
                if not mirror_info.private and mirror_info.cloud_type in ('', None):
                    await mirror_processor.set_mirror_have_full_iso_set(
                        mirror_info=mirror_info,
                        mirror_iso_uris=mirror_iso_uris
                    )
                await mirror_processor.set_location_data_from_online_service(
                    mirror_info=mirror_info
                )


async def update_mirrors():
    task = asyncio.create_task(update_mirrors_handler())
    await task


loop = asyncio.get_event_loop()
loop.run_until_complete(update_mirrors())
