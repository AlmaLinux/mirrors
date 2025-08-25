# coding=utf-8
import asyncio
import itertools
import os
import time
from inspect import signature
from ipaddress import (
    ip_network,
    IPv4Network,
    IPv6Network
)

import dateparser
from flask import (
    Flask
)
from flask_bs4 import Bootstrap
from sqlalchemy import insert

from api.handlers import (
    get_all_mirrors_db,
    SERVICE_CONFIG_PATH,
    MIRROR_CONFIG_JSON_SCHEMA_DIR_PATH,
    SERVICE_CONFIG_JSON_SCHEMA_DIR_PATH,
)
from api.mirror_processor import MirrorProcessor
from api.utils import (
    get_aws_subnets,
    get_azure_subnets,
    get_gcp_subnets,
    get_oci_subnets
)
from common.sentry import get_logger, init_sentry_client
from db.db_engine import FlaskCacheEngine, REDIS_URI
from db.models import (
    Url,
    ModuleUrl,
    Mirror,
    Subnet,
    SubnetInt,
    mirrors_subnets_int
)
from db.utils import session_scope
from yaml_snippets.utils import (
    get_config,
    get_mirrors_info,
)

app = Flask('app')
app.url_map.strict_slashes = False
Bootstrap(app)
logger = get_logger(__name__)
if os.getenv('SENTRY_DSN'):
    init_sentry_client()
cache = FlaskCacheEngine.get_instance(app=app, ro=False)


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
        mirror_check_sem = asyncio.Semaphore(100)
        async with mirror_check_sem, MirrorProcessor(
                logger=logger,
        ) as mirror_processor:  # type: MirrorProcessor
            mirror_iso_uris = mirror_processor.get_mirror_iso_uris(
                versions=(
                    set(main_config.versions) -
                    set(main_config.duplicated_versions)
                ),
                arches=main_config.arches,
            )
            subnets = await get_aws_subnets(
                http_session=mirror_processor.client
            )
            subnets.update(
                await get_gcp_subnets(
                    http_session=mirror_processor.client
                ),
            )
            subnets.update(
                await get_oci_subnets(
                    http_session=mirror_processor.client
                ),
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
            db_session.query(SubnetInt).delete()
            for mirror_info in all_mirrors:
                urls_to_create = [
                    Url(
                        url=url,
                        type=url_type,
                    ) for url_type, url in mirror_info.urls.items()
                ]
                module_urls_to_create = [
                    ModuleUrl(
                        url=url,
                        type=url_type,
                        module=module
                    )
                    for module, url_info in mirror_info.module_urls.items()
                    for url_type, url in url_info.items()
                ]
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
                    iso_url_kitten=mirror_info.iso_url_kitten,
                    sponsor_url=mirror_info.sponsor_url,
                    email=mirror_info.email,
                    cloud_type=mirror_info.cloud_type,
                    cloud_region=mirror_info.cloud_region,
                    urls=urls_to_create,
                    module_urls=module_urls_to_create,
                    private=mirror_info.private,
                    monopoly=mirror_info.monopoly,
                    asn=','.join(mirror_info.asn),
                    has_full_iso_set=mirror_info.has_full_iso_set,
                    has_optional_modules=mirror_info.has_optional_modules
                )
                
                db_session.add(mirror_to_create)
                db_session.flush()
                
                if mirror_info.subnets:
                    subnets_to_create = [
                        Subnet(
                            subnet=subnet,
                        ) for subnet in mirror_info.subnets
                    ]
                    db_session.add_all(subnets_to_create)
                    mirror_to_create.subnets = subnets_to_create
                    
                    # convert IP ranges/CIDRs to integers and store in cache so we can
                    # check if IPs are within subnets faster than the ipaddress module
                    subnets_int_to_create = []
                    for subnet in mirror_info.subnets:
                        subnet = ip_network(subnet)
                        if isinstance(subnet, IPv4Network) or isinstance(subnet, IPv6Network):
                            # convert to str so sqlalchemy "magic" doesn't try to make sqlite use it as an int, which ipv6 overflows
                            start = str(int(subnet.network_address))
                            end = str(int(subnet.broadcast_address))

                            # Using sqlalchemy ORM here is incredibly slow and problematic so we do it a bit more direct
                            # insert the subnets first, returning their row ids so we can create the proper entries in the association table/FKs
                            result = db_session.execute(insert(SubnetInt).values(
                                subnet_start = start,
                                subnet_end = end
                            ))
                            result = db_session.execute(insert(mirrors_subnets_int).values(
                                mirror_id = mirror_to_create.id,
                                subnet_int_id = result.inserted_primary_key[0]
                            ))

        db_session.commit()

        # update all mirrors lists in the Redis cache
        for args in itertools.product(
                (True, False),
                repeat=len(signature(get_all_mirrors_db).parameters)-1,
        ):
            get_all_mirrors_db(bypass_cache=True, *args)
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
            await mirror_processor.set_iso_url_kitten(
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
