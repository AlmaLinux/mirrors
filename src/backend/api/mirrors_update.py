#!/usr/bin/env python3
import asyncio
import aiodns
import os

import aiohttp
import dateparser

from aiohttp import ClientSession
from sqlalchemy.orm import Session
from api.redis import (
    set_mirror_flapped,
    get_mirror_flapped
)
from api.utils import (
    get_geo_data_by_ip,
    get_coords_by_city
)

from common.sentry import get_logger
from urllib3.exceptions import HTTPError

from yaml_snippets.utils import WHITELIST_MIRRORS
from yaml_snippets.data_models import (
    GeoLocationData,
    MirrorData,
    LocationData,
    MainConfig,
)
from db.models import (
    Mirror,
    Url,
    Subnet,
)
from yaml_snippets.utils import mirror_available

# set User-Agent for python-requests
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/56.0.2924.76 Safari/537.36',
    "Upgrade-Insecure-Requests": "1",
    "DNT": "1",
    "Accept": "text/html,application/xhtml+xml,"
              "application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate"
}
ISO_FILES_TEMPLATES = (
    'AlmaLinux-{version}-{arch}-boot.iso',
    'AlmaLinux-{version}-{arch}-dvd.iso',
    'AlmaLinux-{version}-{arch}-minimal.iso',
    'AlmaLinux-{version}-{arch}-boot.iso.manifest',
    'AlmaLinux-{version}-{arch}-dvd.iso.manifest',
    'AlmaLinux-{version}-{arch}-minimal.iso.manifest',
    'CHECKSUM',
)

AIOHTTP_TIMEOUT = 30


logger = get_logger(__name__)


def _get_mirror_iso_uris(
        versions: list[str],
        arches: list[str],
) -> list[str]:
    result = []
    for version in versions:
        for arch in arches:
            for iso_file_template in ISO_FILES_TEMPLATES:
                iso_file = iso_file_template.format(
                    version=f'{version}{"-1" if "beta" in version else ""}',
                    arch=arch,
                )
                result.append(
                    f'{version}/isos/{arch}/{iso_file}'
                )
    return result


async def _has_mirror_iso_uris(
        mirror_iso_uri: str,
        mirror_url: str,
        http_session: ClientSession,
        iso_check_sem: asyncio.Semaphore,
):
    async with iso_check_sem:
        iso_url = os.path.join(mirror_url, mirror_iso_uri)
        try:
            await http_session.head(
                iso_url,
                headers=HEADERS,
                timeout=AIOHTTP_TIMEOUT,
                raise_for_status=True
            )
            logger.info('ISO artifact "%s" is available', iso_url)
            return True
        except (
                asyncio.exceptions.TimeoutError,
                HTTPError,
                aiohttp.ClientError,
        ):
            logger.info('ISO artifact "%s" is unavailable', iso_url)
            return False


async def set_repo_status(
    mirror_info: MirrorData,
    allowed_outdate: str,
    mirror_url: str,
    http_session: ClientSession
) -> None:
    """
    Set status of a mirror
    :param mirror_info: info about a mirror
    :param allowed_outdate: allowed mirror lag
    :param mirror_url: workable mirror's URL which
                       uses one of required protocols
    :param http_session: async http session
    """

    if mirror_info.private:
        mirror_info.status = "ok"
        return
    if await get_mirror_flapped(mirror_name=mirror_info.name):
        mirror_info.status = "flapping"
        return
    timestamp_url = os.path.join(
        mirror_url,
        'TIME',
    )
    try:
        async with http_session.get(
            timestamp_url,
            headers=HEADERS,
            timeout=AIOHTTP_TIMEOUT,
            raise_for_status=True
        ) as resp:
            timestamp_response = await resp.text()
    except (
            asyncio.exceptions.TimeoutError,
            HTTPError,
            aiohttp.ClientError,
    ):
        logger.warning(
            'Mirror "%s" has no timestamp file by url "%s"',
            mirror_info.name,
            timestamp_url,
        )
        mirror_info.status = "expired"
        return
    try:
        mirror_should_updated_at = dateparser.parse(
            f'now-{allowed_outdate} UTC'
        ).timestamp()
        try:
            mirror_last_updated = float(timestamp_response)
        except ValueError:
            logger.warning(
                'Mirror "%s" has broken timestamp file by url "%s"',
                mirror_info.name,
                timestamp_url,
            )
            mirror_info.status = "expired"
            return
        if mirror_last_updated > mirror_should_updated_at:
            mirror_info.status = "ok"
        else:
            mirror_info.status = "expired"
        return
    except AttributeError:
        mirror_info.status = "expired"
        return


async def update_mirror_in_db(
        mirror_info: MirrorData,
        db_session: Session,
        http_session: ClientSession,
        sem: asyncio.Semaphore,
        main_config: MainConfig,
        mirror_iso_uris: list[str]
) -> None:
    """
    Update record about a mirror in DB in background thread.
    The function remove old record about a mirror and add new record if
        a mirror is actual
    :param mirror_info: extracted info about a mirror from yaml files
    :param db_session: session to DB
    :param http_session: async HTTP session
    :param sem: asyncio Semaphore object
    :param main_config: main config of the mirrors service
    :param mirror_iso_uris: full set ISO URIs for all version and arches
    """

    mirror_info = await set_geo_data(mirror_info, sem)
    mirror_name = mirror_info.name
    urls = mirror_info.urls
    mirror_url = next(
        url for url_type, url in urls.items()
        if url_type in main_config.required_protocols
    )
    if mirror_name in WHITELIST_MIRRORS:
        mirror_info.status = "ok"
        is_available = True
    else:
        mirror_name, is_available = await mirror_available(
            mirror_info=mirror_info,
            http_session=http_session,
            logger=logger,
            main_config=main_config,
        )
    if not is_available:
        await set_mirror_flapped(mirror_name=mirror_name)
        return

    iso_check_sem = asyncio.Semaphore(25)
    has_mirror_iso_full_set_results = await asyncio.gather(*(
        asyncio.ensure_future(
            _has_mirror_iso_uris(
                mirror_iso_uri=mirror_iso_uri,
                mirror_url=mirror_url,
                http_session=http_session,
                iso_check_sem=iso_check_sem,
            )
        ) for mirror_iso_uri in mirror_iso_uris
    ))
    logger.info(
        'ISO results "%s" for mirror "%s"',
        has_mirror_iso_full_set_results,
        mirror_name,
    )
    mirror_info.has_full_iso_set = not any(
        not has_flag for has_flag in has_mirror_iso_full_set_results
    )
    await set_repo_status(
        mirror_info=mirror_info,
        allowed_outdate=main_config.allowed_outdate,
        mirror_url=mirror_url,
        http_session=http_session,
    )
    urls_to_create = [
        Url(
            url=url,
            type=url_type,
        ) for url_type, url in mirror_info.urls.items()
    ]
    for url_to_create in urls_to_create:
        db_session.add(url_to_create)
    mirror_to_create = Mirror(
        name=mirror_info.name,
        continent=mirror_info.geolocation.continent,
        country=mirror_info.geolocation.country,
        state=mirror_info.geolocation.state,
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
        sponsor_url=mirror_info.sponsor_url,
        email=mirror_info.email,
        cloud_type=mirror_info.cloud_type,
        cloud_region=mirror_info.cloud_region,
        urls=urls_to_create,
        private=mirror_info.private,
        monopoly=mirror_info.monopoly,
        asn=','.join(mirror_info.asn),
    )
    if mirror_info.subnets:
        subnets_to_create = [
            Subnet(
                subnet=subnet,
            ) for subnet in mirror_info.subnets
        ]
        for subnet_to_create in subnets_to_create:
            db_session.add(subnet_to_create)
        mirror_to_create.subnets = subnets_to_create
    logger.debug(
        'Mirror "%s" is created',
        mirror_name,
    )
    db_session.add(mirror_to_create)
    logger.debug(
        'Mirror "%s" is added',
        mirror_name,
    )


async def set_geo_data(
        mirror_info: MirrorData,
        sem: asyncio.Semaphore,
) -> MirrorData:
    """
    Set geo data by IP of a mirror
    :param mirror_info: Dictionary with info about a mirror
    :param sem: asyncio Semaphore
    """
    mirror_name = mirror_info.name
    if mirror_info.private:
        ipv6 = False
        ip = '0.0.0.0'
        match = None
    else:
        resolver = aiodns.DNSResolver(timeout=5, tries=2)
        try:
            dns = await resolver.query(mirror_name, 'A')
            match = [
                {
                    'match': _match,
                    'ip': record.host,
                } for record in dns
                if (_match := get_geo_data_by_ip(record.host)) is not None
            ]
        except aiodns.error.DNSError:
            logger.warning('Can\'t get IP of mirror %s', mirror_name)
            match = None
            ip = '0.0.0.0'
        try:
            dns = await resolver.query(mirror_name, 'AAAA')
            if dns:
                ipv6 = True
            else:
                ipv6 = False
        except aiodns.error.DNSError:
            ipv6 = False
    logger.info('Set geo data for mirror "%s"', mirror_name)
    if not match:
        state = 'Unknown'
        city = 'Unknown'
        country = 'Unknown'
        continent = 'Unknown'
        ip = 'Unknown'
        location = LocationData(
            latitude=-91,  # outside range of latitude (-90 to 90)
            longitude=-181,  # outside range of longitude (-180 to 180)
        )
    else:
        continent, country, state, city, \
        latitude, longitude = match[0]['match']
        location = LocationData(
            latitude=latitude,
            longitude=longitude,
        )
        ip = match[0]['ip']
    # try to get geo data from yaml
    try:
        continent = mirror_info.geolocation.continent or continent
        country = mirror_info.geolocation.country or country
        state = mirror_info.geolocation.state or state or ''
        city = mirror_info.geolocation.city or city or ''
        # we don't need to do lookups except when geolocation is set in yaml
        if mirror_info.geolocation:
            latitude, longitude = await get_coords_by_city(
                city=city, state=state, country=country, sem=sem
            )
            if (0.0, 0.0) != (latitude, longitude):
                location = LocationData(
                    latitude=latitude,
                    longitude=longitude
                )
    except TypeError:
        logger.error(
            'Nominatim likely blocked us'
        )
    mirror_info.location = location
    mirror_info.geolocation = GeoLocationData(
        continent=continent,
        country=country,
        state=state,
        city=city,
    )
    mirror_info.ip = ip
    mirror_info.ipv6 = ipv6
    return mirror_info
