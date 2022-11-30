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
    ServerDisconnectedError,
    TCPConnector,
    ClientResponse,
)
from aiohttp.web_exceptions import HTTPError
from aiohttp_retry import (
    ExponentialRetry,
    RetryClient,
)

from api.redis import (
    set_mirror_flapped,
    get_mirror_flapped,
)
from api.utils import get_geo_data_by_ip
from yaml_snippets.data_models import (
    GeoLocationData,
    MirrorData,
    LocationData,
    MainConfig,
)
from yaml_snippets.utils import (
    HEADERS,
    mirror_available,
    is_url_available,
    WHITELIST_MIRRORS,
)


class MirrorProcessor:

    client_session = None  # type: ClientSession
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
            attempts=3,
            exceptions={
                ServerDisconnectedError,
                TimeoutError,
            },
        )
        self.client_session = ClientSession(
            conn_timeout=15,
            connector=self.tcp_connector,
            headers=HEADERS,
            raise_for_status=True,
        )
        self.client = RetryClient(
            client_session=self.client_session,
            retry_options=self.retry_options,
            raise_for_status=True,
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.client_session.close()

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
        if data is None:
            data = {}

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

    @staticmethod
    def get_mirror_url(
            main_config: MainConfig,
            mirror_info: MirrorData,
    ):
        return next(
            url for url_type, url in mirror_info.urls.items()
            if url_type in main_config.required_protocols
        )

    async def set_ip_for_mirror(
            self,
            mirror_info: MirrorData,
    ):
        self.logger.info('Set IPs for mirror "%s"', mirror_info.name)
        ip = 'Unknown'
        try:
            dns = await self.dns_resolver.query(mirror_info.name, 'A')
            ip = ','.join(str(record.host) for record in dns)
        except DNSError:
            self.logger.warning(
                'Can not get IP of mirror "%s"',
                mirror_info.name,
            )
        mirror_info.ip = ip

    async def set_geo_data_from_offline_database(
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
                geo_location_data.state,
                geo_location_data.city,
                location.latitude,
                location.longitude,
            ) = match
        except StopIteration:
            self.logger.warning(
                'Mirror "%s" does not have geo data for any its IP',
                mirror_info.name,
            )
        mirror_info.location = location
        mirror_info.geolocation.continent = geo_location_data.continent
        mirror_info.geolocation.country = geo_location_data.country
        mirror_info.geolocation.state = geo_location_data.state
        mirror_info.geolocation.city = geo_location_data.city

    async def set_geo_data_from_online_service(
            self,
            mirror_info: MirrorData,
    ):
        if mirror_info.status != 'ok':
            return
        if mirror_info.geolocation.are_mandatory_fields_empty():
            return
        self.logger.info(
            'Set geodata for mirror "%s" from online DB',
            mirror_info.name,
        )
        params = {
            'city': mirror_info.geolocation.city,
            'state': mirror_info.geolocation.state,
            'country': mirror_info.geolocation.country,
            'format': 'json',
            'limit': 1,
        }
        location = LocationData()
        try:
            result = await (await self.request(
                method='get',
                url=f'{self.nominatim_url}/search',
                params=params,
                headers=HEADERS,
            )).json()
            if result:
                location.latitude = result[0]['lat']
                location.latitude = result[0]['lon']
        except (
            TimeoutError,
            HTTPError,
            ValueError,
        ) as err:
            self.logger.warning(
                'Cannot get geodata for mirror'
                ' "%s" from online DB because "%s"',
                mirror_info.name,
                str(err) or type(err),
            )
        except CancelledError:
            pass
        mirror_info.location = location

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
        if await get_mirror_flapped(mirror_name=mirror_info.name):
            return False
        self.logger.info(
            'Set status for mirror "%s"',
            mirror_info.name,
        )
        if mirror_info.private or mirror_info.name in WHITELIST_MIRRORS:
            self.logger.info(
                'Mirror "%s" is private or in exclusion list',
                mirror_info.name,
            )
            mirror_info.status = "ok"
            return
        mirror_name, is_available = await mirror_available(
            mirror_info=mirror_info,
            http_session=self.client_session,
            main_config=main_config,
            logger=self.logger,
        )
        if not is_available:
            self.logger.info(
                'Mirror "%s" is not available',
                mirror_info.name,
            )
            await set_mirror_flapped(mirror_name=mirror_info.name)
            mirror_info.status = 'flapping'
            return
        is_mirror_expired = await self.is_mirror_expired(
            mirror_info=mirror_info,
            main_config=main_config,
        )
        if is_mirror_expired:
            self.logger.info(
                'Mirror "%s" is expired',
                mirror_info.name,
            )
            mirror_info.status = 'expired'
        else:
            self.logger.info(
                'Mirror "%s" is actual',
                mirror_info.name,
            )
            mirror_info.status = 'ok'

    async def is_mirror_expired(
            self,
            mirror_info: MirrorData,
            main_config: MainConfig,
    ):
        mirror_should_updated_at = dateparser.parse(
            f'now-{main_config.allowed_outdate} UTC'
        ).timestamp()
        timestamp_url = urljoin(
            self.get_mirror_url(
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
                str(err) or type(err),
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
            arches: list[str],
    ) -> list[str]:
        result = []
        for version in versions:
            for arch in arches:
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
            main_config: MainConfig,
            mirror_iso_uris: list[str],
    ):
        success_msg = 'ISO artifact by URL "%(url)s" is available'
        error_msg = (
            'ISO artifact by URL "%(url)s" '
            'is unavailable because "%(err)s"'
        )
        mirror_url = self.get_mirror_url(
            main_config=main_config,
            mirror_info=mirror_info,
        )
        tasks = [asyncio.ensure_future(
            is_url_available(
                url=(
                    url := urljoin(mirror_url  + '/', iso_uri)
                ),
                http_session=self.client_session,
                logger=self.logger,
                is_get_request=False,
                success_msg=None,
                success_msg_vars=None,
                error_msg=None,
                error_msg_vars=None,
            )
        ) for iso_uri in mirror_iso_uris]

        self.logger.info(
            'Set the mirrors have full ISO set is started for mirror "%s"',
            mirror_info.name,
        )
        async def _check_tasks(
                created_tasks: list[asyncio.Task],
        ) -> bool:
            done_tasks, pending_tasks = await asyncio.wait(
                created_tasks,
                return_when=asyncio.FIRST_COMPLETED,
            )
            for future in done_tasks:
                if not future.result():
                    for pending_task in pending_tasks:
                        pending_task.cancel()
                    return False
            if not pending_tasks:
                return True
            return await _check_tasks(
                pending_tasks,
            )

        mirror_info.has_full_iso_set = await _check_tasks(tasks)
