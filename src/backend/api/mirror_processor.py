import asyncio
from asyncio import CancelledError, sleep
from asyncio.exceptions import TimeoutError
from logging import Logger
from pathlib import Path

from typing import Optional, Union

import geopy
from aiodns import DNSResolver
from aiodns.error import DNSError
from aiohttp import (
    ClientSession,
    ClientError,
    ServerDisconnectedError,
    ClientTimeout,
    TCPConnector,
    ClientResponse,
)
from aiohttp.web_exceptions import HTTPError
from aiohttp_retry import ExponentialRetry, RetryClient
from geopy.adapters import AioHTTPAdapter
from geopy.exc import GeocoderServiceError

from api.redis import get_geolocation_from_cache, set_geolocation_to_cache, \
    set_mirror_flapped
from api.utils import get_geo_data_by_ip
from yaml_snippets.data_models import (
    GeoLocationData,
    MirrorData,
    LocationData, MainConfig,
)
from yaml_snippets.utils import HEADERS, mirror_available, is_url_available


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
            if self.__dict__[key] is None:
                self.__dict__[key] = value
        else:
            self.__dict__[key] = value

    def __init__(self, logger: Logger):
        self.logger = logger
        self.dns_resolver = DNSResolver(timeout=5, tries=2)
        self.tcp_connector = TCPConnector(
            limit=10000,
            limit_per_host=100,
            keepalive_timeout=10 * 60,  # 10 minutes
        )
        self.retry_options = ExponentialRetry(
            attempts=2,
            exceptions={
                ServerDisconnectedError,
            },
        )
        self.client_session = ClientSession(
            timeout=ClientTimeout(
                total=15,
            ),
            connector=self.tcp_connector,
            headers=HEADERS,
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

    @staticmethod
    async def set_subnets_for_cloud_mirror(
            subnets: dict[str, list[str]],
            mirror_info: MirrorData,
    ):
        cloud_regions = mirror_info.cloud_region.lower().split(',')
        cloud_type = mirror_info.cloud_type
        if not cloud_regions or cloud_type not in ('aws', 'azure'):
            return
        mirror_info.subnets = [
            subnet for cloud_region in cloud_regions
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

    async def set_geo_data_from_offline_database(
            self,
            mirror_info: MirrorData,
    ):
        geo_location_data = GeoLocationData()
        location = LocationData()
        ip = 'Unknown'
        try:
            dns = await self.dns_resolver.query(mirror_info.name, 'A')
            match = next(
                {
                    'geo_data': geo_data,
                    'ip': record.host,
                } for record in dns
                if (geo_data := get_geo_data_by_ip(record.host)) is not None
            )
            ip = match['ip']
            (
                geo_location_data.continent,
                geo_location_data.country,
                geo_location_data.state,
                geo_location_data.city,
                location.latitude,
                location.longitude,
            ) = match['geo_data']
        except DNSError:
            self.logger.warning(
                'Can not get IP of mirror "%s"',
                mirror_info.name,
            )
        except StopIteration:
            self.logger.warning(
                'Mirror "%s" does not have geo data for any its IP',
                mirror_info.name,
            )
        mirror_info.ip = ip
        mirror_info.location = location
        mirror_info.geolocation.update_from_existing_object(
            geo_location_data=geo_location_data,
        )

    async def set_geo_data_from_online_service(
            self,
            city: str,
            state: str,
            country: str,
            mirror_info: MirrorData,
    ):
        params = {
            'city': city,
            'state': state,
            'country': country,
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
            if result is not None:
                location.latitude = result['lat']
                location.latitude = result['lon']
        except (
            TimeoutError,
            CancelledError,
            HTTPError,
            ClientError,
            ValueError,
        ):
            pass
        mirror_info.location = location

    async def set_ipv6_support_of_mirror(
            self,
            mirror_info: MirrorData,
    ):
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
        mirror_name, is_available = await mirror_available(
            mirror_info=mirror_info,
            http_session=self.client_session,
            main_config=main_config,
            logger=self.logger,
        )
        if mirror_info.private:
            mirror_info.status = "ok"
            return
        if not is_available:
            await set_mirror_flapped(mirror_name=mirror_name)
            mirror_info.status = 'flapping'
            return

    def get_mirror_iso_uris(
            self,
            versions: list[str],
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
            http_session: ClientSession,
            mirror_info: MirrorData,
            mirror_url: str,
            mirror_iso_uris: list[str],
    ):

        success_msg = 'ISO artifact by URL "%(url)s" is available'
        error_msg = (
            'ISO artifact by URL "%(url)s" '
            'is unavailable because "%(err)s"'
        )
        tasks = [asyncio.ensure_future(
            is_url_available(
                url=(
                    url := Path(mirror_url).joinpath(iso_uri)
                ),
                http_session=http_session,
                logger=self.logger,
                is_get_request=False,
                success_msg=success_msg,
                success_msg_vars={
                    'url': url,
                },
                error_msg=error_msg,
                error_msg_vars={
                    'url': url,
                },
            )
        ) for iso_uri in mirror_iso_uris]

        async def _check_tasks(
                created_tasks: list[asyncio.Task],
        ) -> bool:
            done_tasks, pending_tasks = await asyncio.wait(
                created_tasks,
                return_when=asyncio.FIRST_COMPLETED,
            )
            for future in done_tasks:
                if not future.result():
                    return False
            if not pending_tasks:
                return True
            return await _check_tasks(
                pending_tasks,
            )

        mirror_info.has_full_iso_set = await _check_tasks(tasks)
