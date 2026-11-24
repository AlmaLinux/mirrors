# coding=utf-8
"""
Find mirrors whose repodata lags behind upstream although their TIME file
is fresh.

Every run records the repomd.xml revision that repo.almalinux.org serves for
each checked repo, and when that revision was first seen. A mirror's repo is
stale when its revision is older than the newest upstream revision that has
been available for longer than allowed_outdate. Upstream publishes several
revisions a day, so comparing with its current revision alone would flag
nearly every mirror.
"""
import asyncio
import time
from collections import defaultdict
from logging import Logger
from typing import Iterable, Optional

from aiohttp_retry.types import ClientType

from yaml_snippets.data_models import (
    MainConfig,
    MirrorData,
)
from yaml_snippets.utils import (
    RepodataError,
    fetch_repomd,
    get_module_repomd_urls,
    get_repomd_urls,
)

UPSTREAM_MIRROR_NAME = 'repo.almalinux.org'
HISTORY_CACHE_KEY = 'upstream_repomd_history'
HISTORY_CACHE_TIMEOUT = 30 * 24 * 60 * 60  # 30 days
UPSTREAM_CONCURRENCY = 10
# Seconds the upstream revisions may take to fetch before the update goes on
# with the ones it has. A healthy upstream answers all of them in seconds.
UPSTREAM_FETCH_TIMEOUT = 60
# If more than this share of the mirrors which passed every other check
# looks stale, the upstream reference is more likely wrong than the mirrors
MAX_STALE_SHARE = 0.5
MIN_MIRRORS_FOR_STALE_SHARE = 10


def _revision_number(revision: Optional[str]) -> Optional[int]:
    """
    AlmaLinux repomd.xml revisions are Unix timestamps.
    Other revisions can't be compared.
    """
    if (
        revision and
        len(revision) <= 20 and
        revision.isascii() and
        revision.isdigit()
    ):
        return int(revision)
    return None


def _history_entries(entries) -> list[list[float]]:
    """
    Return the [revision, first seen at] entries of one repo from the cache,
    or none if they aren't in that shape
    """
    try:
        return sorted(
            [int(revision), float(first_seen)]
            for revision, first_seen in entries
        )
    except (TypeError, ValueError):
        return []


def get_upstream_repomd_urls(
    upstream: MirrorData,
    main_config: MainConfig,
) -> dict[str, str]:
    """
    Get the upstream repomd.xml URL of every repo mirrors are checked for
    :return: URL -> repo key
    """
    urls = {
        url: info['key'] for url, info in get_repomd_urls(
            mirror_info=upstream,
            main_config=main_config,
        ).items()
    }
    for module in main_config.optional_module_versions:
        if not upstream.module_urls.get(module):
            continue
        urls.update({
            url: info['key'] for url, info in get_module_repomd_urls(
                mirror_info=upstream,
                main_config=main_config,
                module=module,
            ).items()
        })
    return urls


async def fetch_upstream_revisions(
    urls: dict[str, str],
    http_session: ClientType,
    logger: Logger,
) -> dict[str, str]:
    """
    Get the revision of every repo upstream serves now. Gives up on the
    repos it hasn't got after UPSTREAM_FETCH_TIMEOUT seconds.
    :param urls: repomd.xml URL -> repo key
    :return: repo key -> revision
    """
    if not urls:
        return {}
    sem = asyncio.Semaphore(UPSTREAM_CONCURRENCY)

    async def _fetch(url: str) -> Optional[str]:
        try:
            repomd = await fetch_repomd(
                repomd_url=url,
                http_session=http_session,
                sem=sem,
            )
        except RepodataError as err:
            logger.warning(
                'Cannot get the upstream revision of "%s" because "%s"',
                url,
                err,
            )
            return None
        return repomd.revision

    tasks = {
        asyncio.ensure_future(_fetch(url)): key for url, key in urls.items()
    }
    done, pending = await asyncio.wait(tasks, timeout=UPSTREAM_FETCH_TIMEOUT)
    if pending:
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        logger.warning(
            'Gave up getting %d out of %d upstream revisions after %s seconds',
            len(pending),
            len(tasks),
            UPSTREAM_FETCH_TIMEOUT,
        )
    revisions = {}
    for task in done:
        revision = task.result()
        if revision is not None:
            revisions[tasks[task]] = revision
    return revisions


def update_history(
    history: dict[str, list[list[float]]],
    current: dict[str, str],
    keys: Iterable[str],
    now: float,
    cutoff: float,
) -> dict[str, list[list[float]]]:
    """
    Add the revisions upstream serves now to their history, and drop what
    reference_revision() can't need any more
    :param history: repo key -> [[revision, first seen at], ...], oldest first
    :param current: repo key -> revision upstream serves now. A repo missing
                    here (its repomd.xml couldn't be fetched) keeps its history.
    :param keys: every repo key that is checked now. Others are dropped.
    :param now: current Unix time
    :param cutoff: revisions first seen at or before this time have been
                   available for long enough
    """
    updated = {}
    for key in keys:
        entries = _history_entries(history.get(key, []))
        if key in current:
            revision = _revision_number(current[key])
            if revision is None:
                continue
            # Upstream went back, or one CDN node is behind the others.
            # Forget the newer revisions rather than flag every mirror.
            entries = [entry for entry in entries if entry[0] <= revision]
            if not entries or entries[-1][0] < revision:
                entries.append([revision, now])
        older = [entry for entry in entries if entry[1] <= cutoff]
        newer = [entry for entry in entries if entry[1] > cutoff]
        entries = older[-1:] + newer
        if entries:
            updated[key] = entries
    return updated


def reference_revision(
    entries: list[list[float]],
    cutoff: float,
) -> Optional[int]:
    """
    Get the newest revision first seen at or before cutoff
    """
    revisions = [
        int(revision) for revision, first_seen in entries
        if first_seen <= cutoff
    ]
    return max(revisions) if revisions else None


async def get_reference_revisions(
    mirrors: list[MirrorData],
    main_config: MainConfig,
    http_session: ClientType,
    cache,
    logger: Logger,
    outdate_seconds: float,
) -> dict[str, int]:
    """
    Fetch the revisions upstream serves now, update their history in the
    cache, and return the revision every mirror should have by now
    :param mirrors: all configured mirrors, upstream among them
    :param cache: Redis cache which keeps the history between runs
    :param outdate_seconds: how long mirrors have to catch up with upstream
    :return: repo key -> revision
    """
    upstream = next(
        (mirror for mirror in mirrors if mirror.name == UPSTREAM_MIRROR_NAME),
        None,
    )
    if upstream is None:
        logger.warning(
            'Mirror "%s" is not configured, so repodata of mirrors '
            'is not compared with upstream',
            UPSTREAM_MIRROR_NAME,
        )
        return {}
    urls = get_upstream_repomd_urls(
        upstream=upstream,
        main_config=main_config,
    )
    current = await fetch_upstream_revisions(
        urls=urls,
        http_session=http_session,
        logger=logger,
    )
    now = time.time()
    cutoff = now - outdate_seconds
    history = cache.get(HISTORY_CACHE_KEY)
    history = update_history(
        history=history if isinstance(history, dict) else {},
        current=current,
        keys=urls.values(),
        now=now,
        cutoff=cutoff,
    )
    cache.set(HISTORY_CACHE_KEY, history, timeout=HISTORY_CACHE_TIMEOUT)
    reference = {}
    for key, entries in history.items():
        revision = reference_revision(entries=entries, cutoff=cutoff)
        if revision is not None:
            reference[key] = revision
    logger.info(
        'Got upstream revisions of %d out of %d repos. '
        '%d repos have a revision mirrors should have by now',
        len(current),
        len(urls),
        len(reference),
    )
    return reference


def find_stale_repos(
    revisions: dict[str, str],
    reference: dict[str, int],
) -> list[str]:
    """
    Get the keys of the repos whose revision is older than the reference
    :param revisions: repo key -> revision a mirror serves
    :param reference: repo key -> revision the mirror should have by now
    """
    stale = []
    for key, revision in revisions.items():
        expected = reference.get(key)
        number = _revision_number(revision)
        if expected is not None and number is not None and number < expected:
            stale.append(key)
    return sorted(stale)


def mark_stale_mirrors(
    checked_mirrors: list[tuple[MirrorData, dict[str, str]]],
    reference: dict[str, int],
    main_config: MainConfig,
    logger: Logger,
) -> None:
    """
    Mark the mirrors whose repodata is older than the reference as expired,
    and drop the optional modules whose repodata is older
    :param checked_mirrors: every mirror with the revisions of the repos it
                            passed the checks for
    :param reference: repo key -> revision mirrors should have by now
    """
    if not reference:
        return
    module_by_version = {
        f'{version}-{module}': module
        for module, versions in main_config.optional_module_versions.items()
        for version in versions
    }
    # module (None for the main repos) -> number of mirrors compared
    compared = defaultdict(int)
    # module (None for the main repos) -> [(mirror, its revisions, stale repos)]
    stale = defaultdict(list)
    for mirror, revisions in checked_mirrors:
        if mirror.status != 'ok':
            continue
        mirror_modules = (mirror.has_optional_modules or '').split(',')
        revisions_by_module = defaultdict(dict)
        for key, revision in revisions.items():
            module = module_by_version.get(key.split('/', 1)[0])
            revisions_by_module[module][key] = revision
        for module, module_revisions in revisions_by_module.items():
            if module is not None and module not in mirror_modules:
                continue
            compared[module] += 1
            stale_repos = find_stale_repos(
                revisions=module_revisions,
                reference=reference,
            )
            if stale_repos:
                stale[module].append((mirror, module_revisions, stale_repos))

    for module, stale_mirrors in stale.items():
        what = (
            'repodata' if module is None
            else f'repodata of optional module "{module}"'
        )
        if (
            compared[module] >= MIN_MIRRORS_FOR_STALE_SHARE and
            len(stale_mirrors) > compared[module] * MAX_STALE_SHARE
        ):
            logger.error(
                'The %s of %d out of %d mirrors is older than upstream. '
                'The upstream reference is probably wrong, '
                'so these mirrors are left as they are.',
                what,
                len(stale_mirrors),
                compared[module],
            )
            continue
        for mirror, revisions, stale_repos in stale_mirrors:
            key = stale_repos[0]
            logger.warning(
                'Mirror "%s" serves %s older than upstream for %d repo(s): '
                '"%s" has revision %s, but mirrors should have %s by now',
                mirror.name,
                what,
                len(stale_repos),
                key,
                revisions[key],
                reference[key],
            )
            if module is None:
                mirror.status = 'expired'
            else:
                mirror.has_optional_modules = ','.join(
                    name for name in
                    (mirror.has_optional_modules or '').split(',')
                    if name and name != module
                ) or None
