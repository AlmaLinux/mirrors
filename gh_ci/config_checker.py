#!/usr/bin/env python3.9
import argparse
import logging
import os.path

import yaml
import requests

from aiohttp import (
    TCPConnector,
    ClientSession,
)
from syncer import sync

from yaml_snippets.data_models import (
    MirrorData,
    MainConfig,
)
from yaml_snippets.utils import (
    config_validation,
    process_main_config,
    process_mirror_config,
    mirror_available,
    load_json_schema,
)

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(name)s | %(levelname)s | %(message)s',
)


class YamlFileType(argparse.FileType):

    def __call__(self, string):
        file_stream = super(YamlFileType, self).__call__(string)
        try:
            return {
                'config_path': file_stream.name,
                'config_data': yaml.safe_load(file_stream),
            }
        except yaml.YAMLError as err:
            raise argparse.ArgumentTypeError(
                f'The YAML file by path "{file_stream.name}" '
                f'is invalid because "{err}"'
            )


def create_parser():
    parser = argparse.ArgumentParser(
        description='The script checks validity of config (mirror or service) '
                    'schema. It checks a mirror config if you pass both '
                    'params, otherwise it checks only a service config.',
    )
    parser.add_argument(
        '-sc',
        '--service-config',
        default='config.yml',
        dest='service_config',
        help='Path to a service yaml config. Default path is ./config.yml.',
        required=True,
        type=YamlFileType('r'),
    )
    parser.add_argument(
        '-mc',
        '--mirror-configs',
        dest='mirror_configs',
        help='A list of paths to a checked mirror config',
        default=[],
        nargs='+',
        type=YamlFileType('r'),
    )
    return parser


async def are_mirrors_available(
        mirrors: list[MirrorData],
        main_config: MainConfig,
) -> int:
    ret_code = 0
    conn = TCPConnector(limit=10000, force_close=True)
    async with ClientSession(
            connector=conn,
            headers={"Connection": "close"}
    ) as http_session:
        for mirror in mirrors:
            is_available = await mirror_available(
                mirror_info=mirror,
                http_session=http_session,
                logger=logger,
                main_config=main_config,
            )
            # True is 1, False is 0, so
            # we get 1 if a mirror is not available
            ret_code += int(not is_available)
    return ret_code


def do_mirrors_have_valid_geo_data(
        mirrors: list[MirrorData],
) -> int:
    ret_code = 0
    headers = {
        'referer': 'https://github.com/AlmaLinux/mirrors:CI'
    }
    url = 'https://nominatim.openstreetmap.org/search'
    ui_url = 'https://nominatim.openstreetmap.org/ui/details.html'
    for mirror in mirrors:
        if any(getattr(mirror.geolocation, geo_attr) is None for geo_attr in (
            'city', 'state_province', 'country'
        )):
            continue
        params = {
            'city': mirror.geolocation.city,
            'state': mirror.geolocation.state_province,
            'country': mirror.geolocation.country,
            'format': 'json',
        }
        req = requests.get(
            url=url,
            params=params,
            headers=headers,
        )
        try:
            req.raise_for_status()
            if req.json():
                logger.info(
                    'Mirror "%s" has valid geodata',
                    mirror.name,
                )
            else:
                logger.error(
                    'Mirror "%s" has invalid geodata (params=%s). '
                    'Please check your data on "%s"',
                    mirror.name,
                    params,
                    ui_url,
                )
                ret_code = 1
        except requests.RequestException as err:
            logger.warning(
                'Cannot check validity of mirror "%s" geodata because "%s"',
                mirror.name,
                err,
            )
    return ret_code


def main(args):
    service_config_data = args.service_config['config_data']
    service_config_version = service_config_data.get('config_version', 1)
    json_schema_path = os.path.join(
        'gh_ci/yaml_snippets/json_schemas/service_config',
        f'v{service_config_version}.json',
    )
    json_schema = load_json_schema(path=json_schema_path)
    is_validity, err = config_validation(
        yaml_data=service_config_data,
        json_schema=json_schema,
    )
    if is_validity:
        logger.info(
            'Main config "%s" is valid',
            args.service_config['config_path'],
        )
    else:
        logger.error(
            'Main config "%s" is invalid because "%s"',
            args.service_config['config_path'],
            err,
        )
        exit(1)
    main_config, err_msg = process_main_config(yaml_data=service_config_data)
    if err_msg:
        logger.error(
            'Main config of the mirror service is invalid because "%s"',
            err_msg,
        )
        exit(1)
    exit_code = 0
    for mirror_config in args.mirror_configs:
        mirror_config_data = mirror_config['config_data']
        mirror_config_version = mirror_config_data.get('config_version', 1)
        json_schema_path = os.path.join(
            'gh_ci/yaml_snippets/json_schemas/mirror_config',
            f'v{mirror_config_version}.json',
        )
        json_schema = load_json_schema(path=json_schema_path)
        is_validity, err = config_validation(
            yaml_data=mirror_config_data,
            json_schema=json_schema,
        )
        if is_validity:
            logger.info(
                'The mirror config "%s" is valid',
                mirror_config['config_path'],
            )
        else:
            logger.error(
                'The mirror config "%s" is invalid because "%s"',
                mirror_config['config_path'],
                err,
            )
            exit_code = 1
    if not exit_code:
        logger.info('All configs are valid')
    mirrors = [
        process_mirror_config(
            yaml_data=mirror_config['config_data'],
            logger=logger,
            main_config=main_config,
        )
        for mirror_config in args.mirror_configs
    ]
    main_config, err_msg = process_main_config(
        yaml_data=args.service_config['config_data'],
    )
    exit_code += do_mirrors_have_valid_geo_data(
        mirrors=mirrors,
    )
    exit_code += sync(are_mirrors_available(
        mirrors=mirrors,
        main_config=main_config,
    ))
    exit(exit_code)


if __name__ == '__main__':
    arguments = create_parser().parse_args()
    main(arguments)
