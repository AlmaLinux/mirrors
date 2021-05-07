# coding=utf-8
import logging
import os
import shutil
from contextlib import contextmanager
from typing import List

from alembic import command, script
from alembic.config import Config
from alembic.runtime.migration import MigrationContext
from sqlalchemy.exc import OperationalError

from db.db_engine import Engine
from db.models import Base
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.orm import Session

BASE_REVISION = 'base'


@contextmanager
def session_scope() -> Session:
    """
    Provide a transactional scope around a series of operations
    """
    session = Session(
        bind=Engine.get_instance(),
        autoflush=False,
    )
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


def is_db_exists():
    """
    Checks that DB already present, by getting tables list from it
    """
    tables = []
    try:
        database_inspection = Inspector.from_engine(Engine.get_instance())
        tables = [table for table in database_inspection.get_table_names()
                  if table != 'alembic_version']
    except OperationalError as err:
        if '(sqlite3.OperationalError) unable to' \
           ' open database file' in err.args:
            return False
    return len(tables) > 0


def get_database_version() -> str:
    """
    Gets current DB version
    """
    with Engine.get_instance().connect() as connection:
        context = MigrationContext.configure(connection)
        current_rev = context.get_current_revision()
    return current_rev or BASE_REVISION


def get_revisions_list(config: Config) -> List[str]:
    """
    Generates list of revisions
    """

    revisions = []
    alembic_script = script.ScriptDirectory.from_config(config)
    revision_pairs = {s.down_revision or BASE_REVISION: s.revision for s in
                      alembic_script.walk_revisions()}
    current_revision = BASE_REVISION
    while current_revision:
        revisions.append(current_revision)
        current_revision = revision_pairs.get(current_revision)
    return revisions


def create_database_if_not_exists(config: Config) -> None:
    """
    Create db tables if those don't exist
    """
    engine = Engine.get_instance()
    if not is_db_exists():
        Base.metadata.create_all(engine)
        # set created DB as latest version
        command.stamp(config, 'head')


def make_migrations(
        config: Config,
        message: str,
        alembic_env_path: str,
        revisions_path: str,
) -> None:
    """
    Generates revisions by alembic autogenerate feature
    1. Copies existing revisions to writable dir
    2. Generates new revision into this dir
    """
    # in order to not mount as rw and use tmp location for revisions
    if os.path.exists(revisions_path):
        shutil.rmtree(revisions_path)
    shutil.copytree(
        f'{alembic_env_path}/dbmigrations/versions/',
        revisions_path,
    )
    config.set_main_option(
        'version_locations',
        revisions_path,
    )
    command.revision(
        config,
        message,
        autogenerate=True,
        version_path=revisions_path,
    )


def migrate_db(config: Config, revision: str) -> None:
    """
    Migrate DB to passed revision
     - do upgrade, if passed revision is higher than current DB version
     - do downgrade, if passed revision is less than current DB version
    """

    def is_revision_higher() -> bool:
        """
        Upgrade only if target revision is head or
         higher than current revision version
        """
        return revision == 'head' or \
            revisions_list.index(database_version) <= \
            revisions_list.index(revision)

    database_version = get_database_version()
    logging.info(f'Current DB revision version: {database_version}')
    logging.info(f'Passed target revision to migrate DB: {revision}')
    revisions_list = get_revisions_list(config)

    if is_revision_higher():
        logging.info(
            f'Passed revision is higher than current, start upgrading')
        command.upgrade(config, revision=revision)
    else:
        logging.info(
            f'Passed revision is less than current, start downgrading')
        command.downgrade(config, revision=revision)