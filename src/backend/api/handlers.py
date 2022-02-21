# coding=utf-8
import asyncio
import os
import random
from collections import defaultdict

from aiohttp import ClientSession, TCPConnector
from sqlalchemy.orm import Session, joinedload

from api.exceptions import UnknownRepositoryOrVersion
from api.mirrors_update import update_mirror_in_db
from yaml_snippets.utils import (
    get_config,
    get_mirrors_info,
)
from api.redis import (
    set_mirrors_to_cache,
    get_url_types_from_cache,
    set_url_types_to_cache,
    set_mirror_list,
    get_mirror_list,
    get_mirrors_from_cache,
)
from api.utils import (
    get_geo_data_by_ip,
    get_aws_subnets,
    get_azure_subnets,
    set_subnets_for_hyper_cloud_mirror,
    sort_mirrors_by_distance_and_country,
    randomize_mirrors_within_distance,
)
from yaml_snippets.data_models import (
    RepoData,
    MainConfig,
    MirrorData,
)
from db.models import (
    Url,
    Mirror,
    get_asn_by_ip,
    is_ip_in_any_subnet,
    Subnet,
)
from db.utils import session_scope
from sqlalchemy.sql.expression import (
    null,
    false,
    or_,
)
from common.sentry import (
    get_logger,
)

logger = get_logger(__name__)


LENGTH_GEO_MIRRORS_LIST = 10
LENGTH_CLOUD_MIRRORS_LIST = 5
SERVICE_CONFIG_PATH = os.path.join(
    os.environ['SOURCE_PATH'],
    'mirrors/updates/config.yml'
)
SERVICE_CONFIG_JSON_SCHEMA_PATH = os.path.join(
    os.environ['SOURCE_PATH'],
    'src/backend/yaml_snippets/json_schemas/service_config.json'
)
MIRROR_CONFIG_JSON_SCHEMA_PATH = os.path.join(
    os.environ['SOURCE_PATH'],
    'src/backend/yaml_snippets/json_schemas/mirror_config.json'
)


async def _get_nearest_mirrors_by_network_data(
        ip_address: str,
        without_private_mirrors: bool = True,
) -> list[MirrorData]:
    """
    The function returns mirrors which are in the same subnet or have the same
    ASN as a request's IP
    """
    match = get_geo_data_by_ip(ip_address)
    asn = get_asn_by_ip(ip_address)
    suitable_mirrors = []
    mirrors = await get_mirror_list(
        without_private_mirrors=without_private_mirrors,
    )
    if not mirrors:
        await refresh_mirrors_cache(
            without_private_mirrors=without_private_mirrors,
        )
        mirrors = await get_mirror_list(
            without_private_mirrors=without_private_mirrors,
        )
    for mirror in mirrors:
        if mirror.status != "ok":
            continue
        if (asn and asn == mirror.asn) or is_ip_in_any_subnet(
            ip_address=ip_address,
            subnets=mirror.subnets,
        ):
            suitable_mirrors.append(mirror)
    if 1 <= len(suitable_mirrors) < LENGTH_CLOUD_MIRRORS_LIST\
            and match is not None:
        continent, country, _, _, latitude, longitude = match
        suitable_mirrors.extend(
            mirror['mirror'] for mirror in
            sort_mirrors_by_distance_and_country(
                request_geo_data=(latitude, longitude),
                mirrors=[mirror for mirror in mirrors
                         if mirror not in suitable_mirrors],
                country=country,
            )[:LENGTH_CLOUD_MIRRORS_LIST - len(suitable_mirrors)]
        )
    return suitable_mirrors


async def _get_nearest_mirrors_by_geo_data(
        ip_address: str,
        without_private_mirrors: bool = True,
) -> list[MirrorData]:
    """
    # TODO: docstring is obsolete
    The function returns N nearest mirrors towards a request's IP
    Firstly, it searches first N mirrors inside a request's country
    Secondly, it searches first N nearest mirrors by distance
        inside a request's continent
    Thirdly, it searches first N nearest mirrors by distance in the world
    Further the functions concatenate lists and return first
        N elements of a summary list
    """
    match = get_geo_data_by_ip(ip_address)
    mirrors = await get_all_mirrors(
        are_ok_and_not_from_clouds=True,
        without_private_mirrors=without_private_mirrors,
    )
    # We return all of mirrors if we can't
    # determine geo data of a request's IP
    if match is None:
        return mirrors
    continent, country, state, city, latitude, longitude = match

    # sort mirrors by distance and randomize those within specified distance
    # to avoid the same mirrors handling the majority of traffic especially
    # within larger cities
    if city or state or country:
        sorted_mirrors = sort_mirrors_by_distance_and_country(
            request_geo_data=(latitude, longitude),
            mirrors=mirrors,
            country=country,
        )
        mirrors = randomize_mirrors_within_distance(
            mirrors=sorted_mirrors,
            country=country,
        )
    # if we don't have city, country or state data for a requesting IP
    # then geoip isn't very accurate anyway so let's give it a random mirror
    # to spread the load. many IPs are missing this data and this prevents
    # all of those requests from disproportionately hitting mirrors near
    # the geographical center of the US
    else:
        random.shuffle(mirrors)

    return mirrors[:LENGTH_GEO_MIRRORS_LIST]


async def _get_nearest_mirrors(
        ip_address: str,
        without_private_mirrors: bool = True,
) -> list[MirrorData]:
    """
    Get nearest mirrors by geo-data or by subnet/ASN
    """
    if os.getenv('DISABLE_CACHING_NEAREST_MIRRORS'):
        suitable_mirrors = None
    else:
        suitable_mirrors = await get_mirrors_from_cache(ip_address)
    if suitable_mirrors is not None:
        return suitable_mirrors
    suitable_mirrors = await _get_nearest_mirrors_by_network_data(
        ip_address=ip_address,
        without_private_mirrors=without_private_mirrors,
    )
    if not suitable_mirrors:
        suitable_mirrors = await _get_nearest_mirrors_by_geo_data(
            ip_address=ip_address,
            without_private_mirrors=without_private_mirrors,
        )
    await set_mirrors_to_cache(
        ip_address,
        suitable_mirrors,
    )
    return suitable_mirrors


async def _process_mirror(
        subnets: dict[str, list[str]],
        mirror_info: MirrorData,
        versions: list[str],
        repos: list[RepoData],
        allowed_outdate: str,
        db_session: Session,
        http_session: ClientSession,
        arches: list[str],
        required_protocols: list[str],
        nominatim_sem: asyncio.Semaphore,
        mirror_check_sem: asyncio.Semaphore
):
    set_subnets_for_hyper_cloud_mirror(
        subnets=subnets,
        mirror_info=mirror_info,

    )
    async with mirror_check_sem:
        await update_mirror_in_db(
            mirror_info=mirror_info,
            versions=versions,
            repos=repos,
            allowed_outdate=allowed_outdate,
            db_session=db_session,
            http_session=http_session,
            arches=arches,
            required_protocols=required_protocols,
            sem=nominatim_sem
        )


async def update_mirrors_handler() -> str:
    config = get_config(
        logger=logger,
        path_to_config=SERVICE_CONFIG_PATH,
        path_to_json_schema=SERVICE_CONFIG_JSON_SCHEMA_PATH,
    )
    mirrors_dir = os.path.join(
        os.getenv('CONFIG_ROOT'),
        'mirrors/updates',
        config.mirrors_dir,
    )
    all_mirrors = get_mirrors_info(
        mirrors_dir=mirrors_dir,
        logger=logger,
        path_to_json_schema=MIRROR_CONFIG_JSON_SCHEMA_PATH,
    )

    # semaphore for nominatim
    nominatim_sem = asyncio.Semaphore(1)

    with session_scope() as db_session:
        db_session.query(Mirror).delete()
        db_session.query(Url).delete()
        db_session.query(Subnet).delete()
        mirror_check_sem = asyncio.Semaphore(100)
        conn = TCPConnector(limit=10000, force_close=True, use_dns_cache=False)
        async with ClientSession(
                connector=conn,
                headers={"Connection": "close"}
        ) as http_session:
            subnets = await get_aws_subnets(http_session=http_session)
            subnets.update(await get_azure_subnets(http_session=http_session))
            await asyncio.gather(*(
                asyncio.ensure_future(
                    _process_mirror(
                        subnets=subnets,
                        mirror_info=mirror_info,
                        versions=config.versions,
                        repos=config.repos,
                        allowed_outdate=config.allowed_outdate,
                        db_session=db_session,
                        http_session=http_session,
                        arches=config.arches,
                        required_protocols=config.required_protocols,
                        nominatim_sem=nominatim_sem,
                        mirror_check_sem=mirror_check_sem,
                    )
                ) for mirror_info in all_mirrors
            ))
        db_session.flush()
    # update all mirrors list in the redis cache
    await refresh_mirrors_cache(
        are_ok_and_not_from_clouds=True,
        without_private_mirrors=True,
    )
    await refresh_mirrors_cache(
        are_ok_and_not_from_clouds=False,
        without_private_mirrors=False,
    )
    await refresh_mirrors_cache(
        are_ok_and_not_from_clouds=False,
        without_private_mirrors=True,
    )
    await refresh_mirrors_cache(
        are_ok_and_not_from_clouds=True,
        without_private_mirrors=False,
    )
    return 'Done'


async def refresh_mirrors_cache(
        are_ok_and_not_from_clouds: bool = False,
        without_private_mirrors: bool = True,
):
    mirrors = await get_all_mirrors_db(
        are_ok_and_not_from_clouds=are_ok_and_not_from_clouds,
        without_private_mirrors=without_private_mirrors,
    )
    mirror_list = [mirror.to_json() for mirror in mirrors]
    await set_mirror_list(
        mirrors=mirror_list,
        are_ok_and_not_from_clouds=are_ok_and_not_from_clouds,
        without_private_mirrors=without_private_mirrors,
    )


async def get_all_mirrors(
        are_ok_and_not_from_clouds: bool = False,
        without_private_mirrors: bool = True,
) -> list[MirrorData]:
    mirrors = await get_mirror_list(
        are_ok_and_not_from_clouds=are_ok_and_not_from_clouds,
        without_private_mirrors=without_private_mirrors,
    )
    if not mirrors:
        await refresh_mirrors_cache(
            are_ok_and_not_from_clouds=are_ok_and_not_from_clouds,
        )
        mirrors = await get_mirror_list(
            are_ok_and_not_from_clouds=are_ok_and_not_from_clouds,
        )

    return [mirror for mirror in mirrors]


async def get_all_mirrors_db(
        are_ok_and_not_from_clouds: bool = False,
        without_private_mirrors: bool = True,
) -> list[MirrorData]:
    mirrors_list = []
    with session_scope() as session:
        mirrors_query = session.query(
            Mirror
        ).options(
            joinedload(Mirror.urls),
            joinedload(Mirror.subnets)
        ).order_by(
            Mirror.continent,
            Mirror.country,
        )
        if without_private_mirrors:
            mirrors_query = mirrors_query.filter(
                or_(
                    Mirror.private == false(),
                    Mirror.private == null()
                ),
            )
        if are_ok_and_not_from_clouds:
            mirrors_query = mirrors_query.filter(
                Mirror.status == 'ok',
                Mirror.cloud_type == '',
            )
        mirrors = mirrors_query.all()
        for mirror in mirrors:
            mirror_data = mirror.to_dataclass()
            mirrors_list.append(mirror_data)
    return mirrors_list


async def get_mirrors_list(
        ip_address: str,
        version: str,
        repository: str,
) -> str:
    mirrors_list = []
    config = get_config(
        logger=logger,
        path_to_config=SERVICE_CONFIG_PATH,
        path_to_json_schema=SERVICE_CONFIG_JSON_SCHEMA_PATH,
    )
    versions = [str(version) for version in config.versions]
    if version not in versions:
        try:
            version = next(ver for ver in versions if version.startswith(ver))
        except StopIteration:
            raise UnknownRepositoryOrVersion(
                'Unknown version "%s". Allowed list of versions "%s"',
                version,
                ', '.join(versions),
            )
    repos = {
        repo.name: repo for repo in config.repos
    }  # type: dict[str, RepoData]
    if repository not in repos:
        raise UnknownRepositoryOrVersion(
            'Unknown repository "%s". Allowed list of repositories "%s"',
            repository,
            ', '.join(repos.keys()),
        )
    repo_path = repos[repository].path
    nearest_mirrors = await _get_nearest_mirrors(
        ip_address=ip_address,
        without_private_mirrors=False,
    )
    for mirror in nearest_mirrors:
        mirror_url = mirror.urls.get(config.required_protocols[0]) or \
                     mirror.urls.get(config.required_protocols[1])
        full_mirror_path = os.path.join(
            mirror_url,
            version,
            repo_path
        )
        mirrors_list.append(full_mirror_path)

    return '\n'.join(mirrors_list)


def _set_isos_link_for_mirror(
        mirror_info: MirrorData,
        version: str,
        arch: str,
        config: MainConfig,
):
    urls = mirror_info.urls
    mirror_url = next(
        address for protocol_type, address in
        urls.items()
        if protocol_type in config.required_protocols
    )
    mirror_info.isos_link = os.path.join(
        mirror_url,
        str(version),
        'isos',
        arch,
    )


async def get_isos_list_by_countries(
        arch: str,
        version: str,
        ip_address: str,
        config: MainConfig,
) -> tuple[dict[str, list[MirrorData]], list[MirrorData]]:
    mirrors_by_countries = defaultdict(list)
    for mirror_info in await get_all_mirrors():
        # Hyper clouds (like AWS/Azure) don't have ISOs, because they traffic
        # is too expensive
        if mirror_info.cloud_type in ('aws', 'azure'):
            continue

        _set_isos_link_for_mirror(
            mirror_info=mirror_info,
            version=version,
            arch=arch,
            config=config,
        )
        mirrors_by_countries[
            mirror_info.geolocation.country
        ].append(mirror_info)
    nearest_mirrors = await _get_nearest_mirrors(ip_address=ip_address)
    for nearest_mirror in nearest_mirrors:
        # Hyper clouds (like AWS/Azure) don't have ISOs, because they traffic
        # is too expensive
        if nearest_mirror.cloud_type in ('aws', 'azure'):
            continue
        _set_isos_link_for_mirror(
            mirror_info=nearest_mirror,
            version=version,
            arch=arch,
            config=config,
        )
    return mirrors_by_countries, nearest_mirrors


def get_main_isos_table(config) -> dict[str, list[str]]:
    result = defaultdict(list)
    for arch in config.arches:
        result[arch] = [version for version in config.versions
                        if version not in config.duplicated_versions]

    return result


async def get_url_types() -> list[str]:
    url_types = await get_url_types_from_cache()
    if url_types is not None:
        return url_types
    with session_scope() as session:
        url_types = sorted(value[0] for value in session.query(
            Url.type
        ).distinct())
        await set_url_types_to_cache(url_types)
        return url_types
