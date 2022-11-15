# coding=utf-8
import asyncio
import os
import random
from collections import defaultdict
from pathlib import Path

from aiohttp import (
    ClientSession,
    TCPConnector,
)
from sqlalchemy.orm import Session, joinedload

from api.exceptions import UnknownRepoAttribute
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


async def _get_nearest_mirrors_by_network_data(
        ip_address: str,
        without_private_mirrors: bool = True,
) -> list[MirrorData]:
    """
    The function returns mirrors which are in the same subnet or have the same
    ASN as a request's IP
    """

    def _is_additional_mirrors_suitable(
            mirror_data: MirrorData,
            main_list_of_mirrors: list[MirrorData]
    ) -> bool:
        """
        An additional mirror is a mirror
        which is fresh (not outdated), not flapping and public, because
        all suitable private mirrors we already found,
        using ASN or subnets data
        """
        return mirror_data.status == 'ok' and \
            not mirror_data.private and \
            mirror_data not in main_list_of_mirrors

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
        if (asn is not None and asn in mirror.asn) or is_ip_in_any_subnet(
            ip_address=ip_address,
            subnets=mirror.subnets,
        ):
            if mirror.monopoly:
                return [mirror]
            else:
                suitable_mirrors.append(mirror)
    if 1 <= len(suitable_mirrors) < LENGTH_CLOUD_MIRRORS_LIST\
            and match is not None:
        continent, country, _, _, latitude, longitude = match
        not_sorted_additional_mirrors = [
            mirror for mirror in mirrors if _is_additional_mirrors_suitable(
                mirror_data=mirror,
                main_list_of_mirrors=suitable_mirrors,
            )
        ]
        sorted_additional_mirrors = sort_mirrors_by_distance_and_country(
            request_geo_data=(latitude, longitude),
            mirrors=not_sorted_additional_mirrors,
            country=country,
        )
        randomized_additional_mirrors = randomize_mirrors_within_distance(
            mirrors=sorted_additional_mirrors,
            country=country,
        )[:LENGTH_CLOUD_MIRRORS_LIST - len(suitable_mirrors)]
        suitable_mirrors.extend(randomized_additional_mirrors)
    return suitable_mirrors


async def _get_nearest_mirrors_by_geo_data(
        ip_address: str,
        without_private_mirrors: bool = True,
) -> list[MirrorData]:
    """
    The function returns nearest N mirrors to a client
    Read comments below to get more information
    """
    match = get_geo_data_by_ip(ip_address)
    mirrors = await get_all_mirrors(
        are_ok_and_not_from_clouds=True,
        without_private_mirrors=without_private_mirrors,
    )
    # We return all mirrors if we can't
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
    Get the nearest mirrors by geo-data or by subnet/ASN
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
            without_private_mirrors=True,
        )
    await set_mirrors_to_cache(
        ip_address,
        suitable_mirrors,
    )
    return suitable_mirrors


async def _process_mirror(
        subnets: dict[str, list[str]],
        mirror_info: MirrorData,
        db_session: Session,
        http_session: ClientSession,
        nominatim_sem: asyncio.Semaphore,
        mirror_check_sem: asyncio.Semaphore,
        main_config: MainConfig,
):
    set_subnets_for_hyper_cloud_mirror(
        subnets=subnets,
        mirror_info=mirror_info,

    )
    async with mirror_check_sem:
        await update_mirror_in_db(
            mirror_info=mirror_info,
            db_session=db_session,
            http_session=http_session,
            sem=nominatim_sem,
            main_config=main_config,
        )


async def update_mirrors_handler() -> str:

    config = get_config(
        logger=logger,
        path_to_config=SERVICE_CONFIG_PATH,
        path_to_json_schema=SERVICE_CONFIG_JSON_SCHEMA_DIR_PATH,
    )
    mirrors_dir = os.path.join(
        os.getenv('CONFIG_ROOT'),
        'mirrors/updates',
        config.mirrors_dir,
    )
    all_mirrors = get_mirrors_info(
        mirrors_dir=mirrors_dir,
        logger=logger,
        path_to_json_schema=MIRROR_CONFIG_JSON_SCHEMA_DIR_PATH,
    )

    # semaphore for nominatim
    nominatim_sem = asyncio.Semaphore(1)

    pid_file_path = Path(os.getenv('MIRRORS_UPDATE_PID'))
    if Path(os.getenv('MIRRORS_UPDATE_PID')).exists():
        return 'Update is already running'
    try:
        pid_file_path.write_text(str(os.getpid()))
        with session_scope() as db_session:
            db_session.query(Mirror).delete()
            db_session.query(Url).delete()
            db_session.query(Subnet).delete()
            mirror_check_sem = asyncio.Semaphore(100)
            conn = TCPConnector(
                limit=10000,
                force_close=True,
                use_dns_cache=False,
            )
            async with ClientSession(
                    connector=conn,
                    headers={"Connection": "close"}
            ) as http_session:
                subnets = await get_aws_subnets(http_session=http_session)
                subnets.update(
                    await get_azure_subnets(http_session=http_session),
                )
                await asyncio.gather(*(
                    asyncio.ensure_future(
                        _process_mirror(
                            subnets=subnets,
                            mirror_info=mirror_info,
                            db_session=db_session,
                            http_session=http_session,
                            nominatim_sem=nominatim_sem,
                            mirror_check_sem=mirror_check_sem,
                            main_config=config,
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
    finally:
        os.remove(pid_file_path)
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


def _is_vault_repo(
        version: str,
        vault_versions: list[str],
        repo: RepoData,
) -> bool:
    """
    Check that the repo is vault or not.
      The function returns True if repo is vault and
      returns False if the one isn't vault
    :param version: version of requested a mirrors list
    :param vault_versions: the list of global vault versions
    :param repo: repo of requested a mirrors list
    """

    if version in vault_versions or repo.vault:
        return True
    return False


def get_allowed_arch(
        arch: str,
        arches: list[str],
) -> str:
    if arch not in arches:
        raise UnknownRepoAttribute(
            'Unknown architecture "%s". Allowed list of arches "%s"',
            arch,
            ', '.join(arches),
        )
    return arch


def get_allowed_version(
        versions: list[str],
        vault_versions: list[str],
        duplicated_versions: dict[str, str],
        version: str,
) -> str:

    if version not in versions and version not in vault_versions:
        try:
            major_version = next(
                ver for ver in duplicated_versions if version.startswith(ver)
            )
            return duplicated_versions[major_version]
        except StopIteration:
            raise UnknownRepoAttribute(
                'Unknown version "%s". Allowed list of versions "%s"',
                version,
                ', '.join(versions + vault_versions),
            )
    elif version in versions and version in duplicated_versions:
        return duplicated_versions[version]
    else:
        return version


async def get_mirrors_list(
        ip_address: str,
        version: str,
        repository: str,
) -> str:
    mirrors_list = []
    config = get_config(
        logger=logger,
        path_to_config=SERVICE_CONFIG_PATH,
        path_to_json_schema=SERVICE_CONFIG_JSON_SCHEMA_DIR_PATH,
    )
    versions = config.versions
    duplicated_versions = config.duplicated_versions
    vault_versions = config.vault_versions
    vault_mirror = config.vault_mirror
    repos = {
        repo.name: repo for repo in config.repos
    }  # type: dict[str, RepoData]
    if repository not in repos:
        raise UnknownRepoAttribute(
            'Unknown repository "%s". Allowed list of repositories "%s"',
            repository,
            ', '.join(repos.keys()),
        )
    version = get_allowed_version(
        versions=versions,
        vault_versions=vault_versions,
        duplicated_versions=duplicated_versions,
        version=version,
    )
    repo = repos[repository]
    repo_path = repo.path

    # if a client requests global vault version or vault repo
    if _is_vault_repo(
        version=version,
        vault_versions=vault_versions,
        repo=repo
    ):
        return os.path.join(
            vault_mirror,
            version,
            repo_path,
        )
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
    version = get_allowed_version(
        versions=config.versions,
        # ISOs are stored only for active versions (non-vault)
        vault_versions=[],
        duplicated_versions=config.duplicated_versions,
        version=version,
    )
    arch = get_allowed_arch(
        arch=arch,
        arches=config.arches,
    )
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


def get_main_isos_table(config: MainConfig) -> dict[str, list[str]]:
    result = defaultdict(list)
    for arch in config.arches:
        result[arch] = [
            version for version in config.versions
            if version not in config.duplicated_versions and
            arch in config.versions_arches.get(version, config.arches)
        ]

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
