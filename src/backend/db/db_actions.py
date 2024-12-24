# coding=utf-8

import argparse
import os

from alembic.config import Config

from db.db_engine import Engine
from db.utils import (
    create_database_if_not_exists,
    make_migrations,
    migrate_db,
)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    subparsers = parser.add_subparsers(
        dest='command',
    )
    create_db = subparsers.add_parser(
        'create_db',
    )

    make_migrations_parser = subparsers.add_parser(
        'make_migrations',
    )
    make_migrations_parser.add_argument(
        '-m',
        '--message',
        dest='message',
        required=True,
        help='Revision message',
    )
    make_migrations_parser.add_argument(
        '-p', '--revisions-path',
        dest='revisions_path',
        default='/versions',
        help='Where to store revisions in container',
    )

    migrate = subparsers.add_parser(
        'migrate',
    )
    migrate.add_argument(
        '-r', '--revision', default='head', dest='revision',
        help='Revision to migrate (upgrade or downgrade) DB',
    )

    args = parser.parse_args()

    dir_path = os.path.dirname(__file__)
    alembic_conf = os.path.join(dir_path, 'alembic.ini')
    config = Config(alembic_conf)

    # Postgres DB create
    engine = Engine.get_instance()

    if args.command == 'create_db':
        create_database_if_not_exists(config)
    elif args.command == 'make_migrations':
        make_migrations(config, args.message, dir_path, args.revisions_path)
    elif args.command == 'migrate':
        migrate_db(config, args.revision)
    else:
        raise NotImplementedError
