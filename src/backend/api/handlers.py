# coding=utf-8
import math
import os
from typing import AnyStr

import dateparser

from api.exceptions import BadRequestFormatExceptioin
from api.mirrors_update import (
    get_config,
    get_verified_mirrors,
    REQUIRED_MIRROR_PROTOCOLS,
)
from db.db_engine import GeoIPEngine
from db.models import Url, Mirror
from db.utils import session_scope
from sqlalchemy.sql.expression import false

from common.sentry import (
    get_logger,
)


MAX_LENGTH_OF_MIRRORS_LIST = 5

logger = get_logger(__name__)


def _get_distance_between(
        lat1: float,
        lon1: float,
        lat2: float,
        lon2: float,
) -> float:
    return math.sqrt(
        (lat1 - lat2)**2 + (lon1 - lon2)**2
    )


def _get_nearest_mirrors(ip_address):
    db = GeoIPEngine.get_instance()
    match = db.lookup(ip_address)
    with session_scope() as session:
        if match is None:
            all_mirrors = session.query(Mirror).filter(
                Mirror.is_expired == false(),
                ).all()
            all_mirrors = [mirror.to_dict() for mirror in all_mirrors]
            return all_mirrors
        match_dict = match.get_info_dict()
        match_country = match_dict['country']['names']['en']
        match_continent = match_dict['continent']['names']['en']
        suitable_mirrors = session.query(Mirror).filter(
            Mirror.continent == match_continent,
            Mirror.country == match_country,
            Mirror.is_expired == false(),
        ).limit(MAX_LENGTH_OF_MIRRORS_LIST).all()
        suitable_mirrors = [mirror.to_dict() for mirror in suitable_mirrors]
        if len(suitable_mirrors) == MAX_LENGTH_OF_MIRRORS_LIST:
            return suitable_mirrors
        rest_mirrors = session.query(Mirror).filter(
            Mirror.continent == match_continent,
            Mirror.is_expired == false(),
            Mirror.name.notin_(
                [mirror.name for mirror in suitable_mirrors]
            ),
        ).all()
        rest_mirrors = [mirror.to_dict() for mirror in rest_mirrors]
        if not rest_mirrors:
            rest_mirrors = session.query(Mirror).filter(
                Mirror.name.notin_(
                    [mirror.name for mirror in suitable_mirrors]
                ),
                Mirror.is_expired == false(),
            ).all()
            rest_mirrors = [mirror.to_dict() for mirror in rest_mirrors]
        distance_dict = {
            _get_distance_between(
                lat1=match.location[0],
                lon1=match.location[1],
                lat2=mirror['location']['lat'],
                lon2=mirror['location']['lon'],
            ): mirror for mirror in rest_mirrors
        }
        suitable_mirrors.extend(
            [distance_dict[distance] for distance in
             sorted(distance_dict.keys())
             [:MAX_LENGTH_OF_MIRRORS_LIST - len(suitable_mirrors)]]
        )
        return suitable_mirrors


def update_mirrors_handler():
    config = get_config()
    versions = config['version']
    repos = config['repos']
    verified_mirrors = get_verified_mirrors(
        mirrors_dir=os.path.join(
            os.path.dirname(
                os.path.abspath(__file__),
            ),
            '../../../mirrors',
            config['mirrors_dir'],
        ),
        versions=versions,
        repos=repos,
        allowed_outdate=config['allowed_outdate']
    )
    with session_scope() as session:
        session.query(Mirror).delete()
        session.query(Url).delete()
        for mirror in verified_mirrors:
            urls_to_create = [
                Url(
                    url=url,
                    type=url_type,
                ) for url_type, url in mirror['address'].items()
            ]
            for url_to_create in urls_to_create:
                session.add(url_to_create)
            mirror_to_create = Mirror(
                name=mirror['name'],
                continent=mirror['continent'],
                country=mirror['country'],
                ip=mirror['ip'],
                latitude=mirror['location']['lat'],
                longitude=mirror['location']['lon'],
                is_expired=mirror['status'] == 'expired',
                update_frequency=dateparser.parse(mirror['update_frequency']),
                sponsor_name=mirror['sponsor'],
                sponsor_url=mirror['sponsor_url'],
                email=mirror['email'],
                urls=urls_to_create,
            )
            session.add(mirror_to_create)
        session.flush()

    return 'Done'


def get_all_mirrors():
    mirrors_list = []
    with session_scope() as session:
        mirrors = session.query(Mirror).all()
        logger.info(mirrors)
        for mirror in mirrors:
            mirror_data = mirror.to_dict()
            mirror_data['urls'] = {
                url['type']: url['url'] for url in mirror_data['urls']
            }
            mirrors_list.append(mirror_data)
    logger.info(mirrors_list)
    return mirrors_list


def get_mirrors_list(
        ip_address: AnyStr,
        version: AnyStr,
        repository: AnyStr,
) -> AnyStr:
    mirrors_list = []
    config = get_config()
    versions = [str(version) for version in config['version']]
    if version not in versions:
        raise BadRequestFormatExceptioin(
            'Unknown version "%s". Allowed list of versions "%s"',
            version,
            ', '.join(versions),
        )
    repos = {repo['name']: repo['path'] for repo in config['repos']}
    if repository not in repos:
        raise BadRequestFormatExceptioin(
            'Unknown repository "%s". Allowed list of repositories "%s"',
            repository,
            ', '.join(repos.keys()),
        )
    repo_path = repos[repository]
    nearest_mirrors = _get_nearest_mirrors(ip_address=ip_address)
    for mirror in nearest_mirrors:
        mirror_url = next(iter(
            url['url'] for url in mirror['urls']
            if url['type'] in REQUIRED_MIRROR_PROTOCOLS
        ))
        full_mirror_path = os.path.join(
            mirror_url,
            version,
            repo_path
        )
        mirrors_list.append(full_mirror_path)

    return '\n'.join(mirrors_list)
