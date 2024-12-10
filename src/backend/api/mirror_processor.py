import asyncio
from urllib.parse import urljoin
from asyncio import CancelledError
from asyncio.exceptions import TimeoutError
from logging import Logger

from typing import Optional, Union

import dateparser
from aiodns import DNSResolver
from aiodns.error import DNSError
from aiohttp import (
    ClientSession,
    ClientError,
    ClientTimeout,
    ServerDisconnectedError,
    TCPConnector,
    ClientResponse,
)
from aiohttp.web_exceptions import HTTPError
from aiohttp_retry import (
    ExponentialRetry,
    RetryClient,
)
from aiohttp_retry.types import ClientType
from pycountry import countries

from api.redis import (
    get_geolocation_from_cache,
    set_geolocation_to_cache,
    FLAPPED_EXPIRED_TIME,
)
from api.utils import get_geo_data_by_ip
from db.db_engine import FlaskCacheEngine
from yaml_snippets.data_models import (
    GeoLocationData,
    MirrorData,
    LocationData,
    MainConfig,
)
from yaml_snippets.utils import (
    HEADERS,
    mirror_available,
    optional_modules_available,
    is_url_available,
    WHITELIST_MIRRORS,
    check_tasks,
    get_mirror_url,
)


class MirrorProcessor:

    client_session = None  # type: ClientSession
    client = None  # type: ClientType
    dns_resolver = None  # type: DNSResolver
    tcp_connector = None  # type: TCPConnector

    iso_files_templates = (
        'AlmaLinux-{version}-{arch}-boot.iso',
        'AlmaLinux-{version}-{arch}-dvd.iso',
        'AlmaLinux-{version}-{arch}-minimal.iso',
        'AlmaLinux-{version}-{arch}-boot.iso.manifest',
        'AlmaLinux-{version}-{arch}-dvd.iso.manifest',
        'AlmaLinux-{version}-{arch}-minimal.iso.manifest',
        'CHECKSUM',
    )

    __class_objects__ = (
        'client_session',
        'dns_resolver',
        'tcp_connector',
        'client',
    )

    nominatim_url = 'https://nominatim.openstreetmap.org'

    def __setattr__(self, key, value):
        if key in self.__class_objects__:
            if key not in self.__dict__ or self.__dict__[key] is None:
                self.__dict__[key] = value
        else:
            self.__dict__[key] = value

    def __init__(self, logger: Logger):
        self.logger = logger  # type: Logger
        self.dns_resolver = DNSResolver(timeout=5, tries=2)
        self.tcp_connector = TCPConnector(
            limit=10000,
            limit_per_host=20,
            force_close=True,
        )
        self.retry_options = ExponentialRetry(
            attempts=2,
            exceptions={
                ServerDisconnectedError,
                TimeoutError,
            },
        )
        self.client_session = ClientSession(
            timeout=ClientTimeout(total=15, connect=10),
            connector=self.tcp_connector,
            headers=HEADERS,
            raise_for_status=True,
        )
        self.client = RetryClient(
            client_session=self.client_session,
            retry_options=self.retry_options,
            raise_for_status=True,
        )
        self.cache = FlaskCacheEngine.get_instance()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.client.close()

    async def request(
            self,
            method: str,
            url: str,
            params: Optional[dict[str, Union[str, int]]] = None,
            headers: Optional[dict[str, Union[str, int]]] = None,
            data: Optional[dict[str, Union[str, int]]] = None,
            **kwargs,
    ) -> ClientResponse:
        if params is None:
            params = {}
        if headers is None:
            headers = {}

        return await self.client.request(
            method=method,
            url=url,
            params=params,
            json=data,
            headers=headers,
            **kwargs,
        )

    async def set_subnets_for_cloud_mirror(
            self,
            subnets: dict[str, list[str]],
            mirror_info: MirrorData,
    ):
        self.logger.info(
            'Set subnets for mirror "%s"',
            mirror_info.name,
        )
        cloud_regions = mirror_info.cloud_region.lower().split(',')
        cloud_type = mirror_info.cloud_type
        if not cloud_regions or cloud_type not in ('aws', 'azure'):
            return
        mirror_info.subnets = [
            subnet for cloud_region in cloud_regions if cloud_region in subnets
            for subnet in subnets[cloud_region]
        ]

    async def set_ip_for_mirror(
            self,
            mirror_info: MirrorData,
    ):
        self.logger.info('Set IPs for mirror "%s"', mirror_info.name)
        ip = 'Unknown'
        try:
            dns = await self.dns_resolver.query(mirror_info.name, 'A')
            ip = ','.join(str(record.host) for record in dns)
        except DNSError as err:
            self.logger.warning(
                'Can not get IP of mirror "%s"',
                mirror_info.name,
            )
            mirror_info.status = f'Unknown IP (reason: {err})'
        mirror_info.ip = ip

    async def set_iso_url(
            self,
            mirror_info: MirrorData,
    ):
        self.logger.info('Set iso URL for "%s"', mirror_info.name)
        mirror_info.iso_url = urljoin(
            mirror_info.mirror_url + '/',
            '%s/isos/%s',
            )

    async def set_geo_and_location_data_from_db(
            self,
            mirror_info: MirrorData,
    ):
        self.logger.info(
            'Set geodata for mirror "%s" from offline DB',
            mirror_info.name,
        )
        geo_location_data = GeoLocationData()
        location = LocationData()
        try:
            match = next(
                geo_data for ip in mirror_info.ip.split(',')
                if (geo_data := get_geo_data_by_ip(ip)) is not None
            )
            (
                geo_location_data.continent,
                geo_location_data.country,
                geo_location_data.state_province,
                geo_location_data.city,
                location.latitude,
                location.longitude,
            ) = match
        except StopIteration:
            if not mirror_info.private:
                self.logger.warning(
                    'Mirror "%s" does not have geo data for any of its IPs',
                    mirror_info.name,
                )
                mirror_info.status = 'Unknown geodata for any IP of the mirror'
        mirror_info.location = location
        mirror_info.geolocation.update_from_existing_object(geo_location_data)
        try:
            if mirror_info.geolocation.country == 'Unknown':
                return
            if len(mirror_info.geolocation.country) == 2:
                mirror_info.geolocation.__dict__['country'] = \
                    mirror_info.geolocation.country.upper()
            else:
                country = countries.get(
                    name=mirror_info.geolocation.country,
                )
                mirror_info.geolocation.__dict__['country'] = \
                    country.alpha_2
        except LookupError:
            pass

    async def set_location_data_from_online_service(
            self,
            mirror_info: MirrorData,
    ):
        if mirror_info.status != 'ok':
            return
        if mirror_info.geolocation.are_mandatory_fields_empty():
            self.logger.info(
                'Mirror "%s" has empty mandatory geo field. City: "%s",'
                'Country: "%s", State: "%s"',
                mirror_info.name,
                mirror_info.geolocation.city,
                mirror_info.geolocation.country,
                mirror_info.geolocation.state_province,
            )
            return
        self.logger.info(
            'Set geodata for mirror "%s" from online DB',
            mirror_info.name,
        )
        redis_key = (
            f'nominatim_{mirror_info.geolocation.country}_'
            f'{mirror_info.geolocation.state_province}_'
            f'{mirror_info.geolocation.city}'
        )
        cached_latitude, cached_longitude = get_geolocation_from_cache(
            key=redis_key,
            cache=self.cache,
        )
        if cached_latitude is not None and cached_longitude is not None:
            mirror_info.location = LocationData(
                latitude=cached_latitude,
                longitude=cached_longitude,
            )
            return
        params = {
            'city': mirror_info.geolocation.city,
            'state': mirror_info.geolocation.state_province,
            'country': mirror_info.geolocation.country,
            'format': 'json',
            'limit': 1,
        }
        try:
            result = await (await self.request(
                method='get',
                url=f'{self.nominatim_url}/search',
                params=params,
                headers=HEADERS,
            )).json()
            if result:
                latitude = result[0]['lat']
                longitude = result[0]['lon']
                set_geolocation_to_cache(
                    key=redis_key,
                    cache=self.cache,
                    latitude=latitude,
                    longitude=longitude,
                )
                mirror_info.location = LocationData(
                    latitude=result[0]['lat'],
                    longitude=result[0]['lon'],
                )
        except (
                TimeoutError,
                HTTPError,
                ValueError,
                ClientError,
                CancelledError,
        ) as err:
            self.logger.warning(
                'Cannot get geodata for mirror'
                ' "%s" from online DB because "%s"',
                mirror_info.name,
                str(err) or str(type(err)),
                )

    async def set_ipv6_support_of_mirror(
            self,
            mirror_info: MirrorData,
    ):
        self.logger.info(
            'Check that mirror "%s" supports IPv6',
            mirror_info.name,
        )
        try:
            mirror_info.ipv6 = bool(
                await self.dns_resolver.query(mirror_info.name, 'AAAA')
            )
        except DNSError:
            mirror_info.ipv6 = False

    async def set_status_of_mirror(
            self,
            mirror_info: MirrorData,
            main_config: MainConfig,
    ):
        self.logger.info(
            'Set status for mirror "%s"',
            mirror_info.name,
        )
        redis_key = f'mirror_offline_{mirror_info.name}'
        if (redis_value := self.cache.get(key=redis_key)) is not None:
            mirror_info.status = redis_value
            return False
        if mirror_info.private or mirror_info.name in WHITELIST_MIRRORS:
            self.logger.info(
                'Mirror "%s" is private or in exclusion list',
                mirror_info.name,
            )
            mirror_info.status = "ok"
            return
        result, reason = await is_url_available(
            url=mirror_info.mirror_url,
            http_session=self.client,
            logger=self.logger,
            is_get_request=True,
            success_msg=None,
            success_msg_vars=None,
            error_msg='Mirror "%(mirror_name)s" '
                      'is not available by url "%(url)s" '
                      'because "%(err)s"',
            error_msg_vars={
                'mirror_name': mirror_info.name,
                'url': mirror_info.mirror_url,
            },
        )
        if not result:
            self.logger.info(
                'Mirror "%s" is not available',
                mirror_info.name,
            )
            self.cache.set(
                key=redis_key,
                value=reason,
                timeout=FLAPPED_EXPIRED_TIME,
            )
            mirror_info.status = reason
            return
        if await self.is_mirror_expired(
                mirror_info=mirror_info,
                main_config=main_config,
        ):
            self.logger.info(
                'Mirror "%s" is expired',
                mirror_info.name,
            )
            mirror_info.status = 'expired'
            return
        result, reason = await mirror_available(
            mirror_info=mirror_info,
            http_session=self.client,
            main_config=main_config,
            logger=self.logger,
        )
        if not result:
            self.logger.info(
                'Mirror "%s" is not available',
                mirror_info.name,
            )
            self.cache.set(
                key=redis_key,
                value=reason,
                timeout=FLAPPED_EXPIRED_TIME,
            )
            mirror_info.status = reason
            return
        self.logger.info(
            'Mirror "%s" is actual',
            mirror_info.name,
        )
        mirror_info.status = 'ok'
        
        for module in main_config.optional_module_versions.keys():
            await optional_modules_available(
                mirror_info=mirror_info,
                http_session=self.client,
                main_config=main_config,
                logger=self.logger,
                module=module
            )

    async def is_mirror_expired(
            self,
            mirror_info: MirrorData,
            main_config: MainConfig,
    ):
        mirror_should_updated_at = dateparser.parse(
            f'now-{main_config.allowed_outdate} UTC'
        ).timestamp()
        timestamp_url = urljoin(
            get_mirror_url(
                main_config=main_config,
                mirror_info=mirror_info,
            ) + '/',
            'TIME',
            )
        try:
            result = await (await self.request(
                url=str(timestamp_url),
                method='get',
                headers=HEADERS,
            )).text()
        except (
                TimeoutError,
                HTTPError,
                ClientError,
                CancelledError,
                # E.g. repomd.xml is broken.
                # It can't be decoded in that case
                UnicodeError,
        ) as err:
            self.logger.warning(
                'Mirror "%s" has no timestamp file by url "%s" because "%s"',
                mirror_info.name,
                timestamp_url,
                str(err) or str(type(err)),
                )
            return True
        try:
            mirror_last_updated = float(result)
        except ValueError:
            self.logger.warning(
                'Mirror "%s" has broken timestamp file by url "%s"',
                mirror_info.name,
                timestamp_url,
            )
            return True
        return mirror_last_updated < mirror_should_updated_at

    def get_mirror_iso_uris(
            self,
            versions: set[str],
            arches: dict[str, list[str]],
            duplicated_versions
    ) -> list[str]:
        result = []
        for version in versions:
            base_version = next(
                (
                    i for i in arches
                    if version.startswith(i)
                ),
                version,
            )
            for arch in arches[base_version]:
                for iso_file_template in self.iso_files_templates:
                    iso_file = iso_file_template.format(
                        version=f'{version}'
                                f'{"-1" if "beta" in version else ""}',
                        arch=arch,
                    )
                    result.append(
                        f'{version}/isos/{arch}/{iso_file}'
                    )
        return result

    async def set_mirror_have_full_iso_set(
            self,
            mirror_info: MirrorData,
            mirror_iso_uris: list[str],
    ):
        error_msg = (
            'ISO artifact by URL "%(url)s" '
            'is unavailable because "%(err)s"'
        )
        iso_semaphore = asyncio.Semaphore(3)
        tasks = [asyncio.ensure_future(
            is_url_available(
                url=(url := urljoin(
                    mirror_info.mirror_url + '/',
                    iso_uri,
                    )),
                http_session=self.client,
                logger=self.logger,
                is_get_request=False,
                success_msg=None,
                success_msg_vars=None,
                error_msg=error_msg,
                error_msg_vars={
                    'url': url,
                },
                sem = iso_semaphore
            )
        ) for iso_uri in mirror_iso_uris]

        self.logger.info(
            'Set the mirrors have full ISO set is started for mirror "%s"',
            mirror_info.name,
        )
        result, _ = await check_tasks(tasks)
        mirror_info.has_full_iso_set = result
