# coding=utf-8
"""
Refresh the IPinfo databases the service reads: geolocation (GEOIP_PATH) and
ASN (ASN_PATH).

Run daily by almalinux_ipinfo_update.timer on a single host. Ansible installs
the timer everywhere but never enables it.

The databases live on an EFS share that every web host mounts over NFS, and
gunicorn workers keep them mmapped until the worker is recycled. If a file
that another NFS client has open is overwritten or deleted, that client's next
read fails with ESTALE (SIGBUS for an mmap). So a database is never
overwritten or deleted while a reader might still hold it:

1. Ask IPinfo for the sha256 (checksum requests don't count toward the
   10 downloads/day limit) and stop if the current file already matches.
2. Download into versions/ under a temp name, verify the sha256, and check
   that it opens and has the fields the app reads.
3. Rename it to versions/<name>.<sha256>.mmdb.
4. Repoint GEOIP_PATH / ASN_PATH by renaming a new relative symlink over the
   old one. Both renames are atomic, so a reader sees either the new file on
   its next open or the old file it already holds; never a partial one.
5. Delete versions that stopped being current more than IPINFO_KEEP_HOURS
   ago (default 7 days), by which point readers should have reopened.
"""
import hashlib
import logging
import os
import re
import secrets
import signal
import sys
import time
import traceback
from itertools import islice
from pathlib import Path

import maxminddb
import requests

BASE_URL = os.environ.get('IPINFO_BASE_URL') or 'https://ipinfo.io/data'
TOKEN = os.environ.get('IPINFO_TOKEN', '')
KEEP_SECONDS = float(os.environ.get('IPINFO_KEEP_HOURS') or 7 * 24) * 3600
VERSIONS_DIR = 'versions'
CHUNK_SIZE = 1 << 20

# (env var with the path the app reads, env var overriding the IPinfo file
#  name, default IPinfo file name, fields the app needs in each record).
# The app expects the legacy schema (country as ISO code, lat/lng). The newer
# ipinfo_location.mmdb uses country_code/latitude/longitude and is rejected.
DATABASES = (
    ('GEOIP_PATH', 'IPINFO_LOCATION_DB', 'standard_location.mmdb',
     ('country', 'lat', 'lng')),
    ('ASN_PATH', 'IPINFO_ASN_DB', 'asn.mmdb', ('asn',)),
)

logger = logging.getLogger('update_ipinfo')


def redact(text: str) -> str:
    # The token travels as ?token=, and requests/urllib3 errors embed the URL
    return text.replace(TOKEN, '<token>') if TOKEN else text


def version_re(stem: str) -> re.Pattern:
    return re.compile(rf'{re.escape(stem)}\.(?P<sha256>[0-9a-f]{{64}})\.mmdb')


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, 'rb') as f:
        while chunk := f.read(CHUNK_SIZE):
            digest.update(chunk)
    return digest.hexdigest()


def current_sha256(public: Path, stem: str) -> str | None:
    if public.is_symlink():
        match = version_re(stem).fullmatch(Path(os.readlink(public)).name)
        if match:
            return match['sha256']
    if public.exists():
        # Plain file from before this script managed the directory
        return file_sha256(public)
    return None


def fetch_sha256(remote: str) -> str:
    resp = requests.get(
        f'{BASE_URL}/{remote}/checksums',
        params={'token': TOKEN},
        timeout=30,
    )
    if resp.status_code != 200:
        raise RuntimeError(
            f'checksum request for {remote} returned HTTP {resp.status_code}'
        )
    sha256 = str(resp.json()['checksums']['sha256']).lower()
    if not re.fullmatch(r'[0-9a-f]{64}', sha256):
        raise RuntimeError(f'malformed sha256 for {remote}: {sha256!r}')
    return sha256


def check_database(path: Path, fields: tuple[str, ...]) -> None:
    with maxminddb.open_database(str(path)) as reader:
        if reader.metadata().node_count == 0:
            raise RuntimeError(f'{path.name} has no records')
        for _, record in islice(reader, 1000):
            if isinstance(record, dict) and all(record.get(f) for f in fields):
                return
    raise RuntimeError(
        f'{path.name}: no record has all of {fields}; wrong database?'
    )


def download(
    remote: str,
    version: Path,
    sha256: str,
    fields: tuple[str, ...],
) -> None:
    tmp = version.parent / f'.{remote}.{secrets.token_hex(4)}.tmp'
    digest = hashlib.sha256()
    try:
        with requests.get(
            f'{BASE_URL}/{remote}',
            params={'token': TOKEN},
            stream=True,
            timeout=(30, 300),
        ) as resp:
            if resp.status_code != 200:
                raise RuntimeError(
                    f'download of {remote} returned HTTP {resp.status_code}'
                )
            with open(tmp, 'xb') as f:
                for chunk in resp.iter_content(CHUNK_SIZE):
                    f.write(chunk)
                    digest.update(chunk)
                f.flush()
                os.fsync(f.fileno())
        if digest.hexdigest() != sha256:
            raise RuntimeError(
                f'{remote}: downloaded sha256 {digest.hexdigest()} does not '
                f'match IPinfo checksum {sha256}'
            )
        check_database(tmp, fields)
        # Read by the service user on every web host, whatever its uid
        os.chmod(tmp, 0o644)
        os.rename(tmp, version)
    finally:
        tmp.unlink(missing_ok=True)


def keep_old_inode(public: Path, versions: Path, stem: str, sha256: str):
    """
    Hard-link a plain-file database into versions/ before the symlink
    replaces it, so readers that have it open keep a live file.
    """
    try:
        os.link(public, versions / f'{stem}.{sha256}.mmdb')
    except FileExistsError:
        pass
    except OSError as exc:
        logger.warning(
            'could not hard-link %s into %s (%s); readers holding it open '
            'may hit ESTALE once',
            public, versions, exc,
        )


def point_at(public: Path, version: Path) -> None:
    link = public.parent / f'.{public.name}.{secrets.token_hex(4)}.lnk'
    os.symlink(version.relative_to(public.parent), link)
    try:
        os.replace(link, public)
    finally:
        link.unlink(missing_ok=True)


def prune(public: Path, versions: Path, stem: str, remote: str) -> None:
    current = Path(os.readlink(public)).name if public.is_symlink() else None
    pattern = version_re(stem)
    now = time.time()
    found = sorted(
        (p.stat().st_mtime, p)
        for p in versions.iterdir()
        if pattern.fullmatch(p.name)
    )
    # A version stopped being current when the next newer one was installed
    for (_, old), (replaced_at, _) in zip(found, found[1:]):
        if old.name != current and now - replaced_at > KEEP_SECONDS:
            logger.info('removing %s', old.name)
            old.unlink(missing_ok=True)
    # Downloads killed before their finally block could run
    for tmp in versions.glob(f'.{remote}.*.tmp'):
        if now - tmp.stat().st_mtime > 24 * 3600:
            tmp.unlink(missing_ok=True)


def refresh(public: Path, remote: str, fields: tuple[str, ...]) -> None:
    stem = public.name.removesuffix('.mmdb')
    versions = public.parent / VERSIONS_DIR
    versions.mkdir(mode=0o755, exist_ok=True)

    sha256 = fetch_sha256(remote)
    current = current_sha256(public, stem)
    if current == sha256:
        logger.info('%s is up to date (%s)', public, sha256[:12])
    else:
        version = versions / f'{stem}.{sha256}.mmdb'
        if version.exists():
            # Verified on an earlier run that failed before repointing
            logger.info('reusing already downloaded %s', version.name)
        else:
            logger.info('downloading %s (%s)', remote, sha256[:12])
            download(remote, version, sha256, fields)
        if current and not public.is_symlink():
            keep_old_inode(public, versions, stem, current)
        # prune() reads a version's mtime as the time it became current
        os.utime(version)
        point_at(public, version)
        logger.info('%s now points at %s', public, version.name)

    try:
        prune(public, versions, stem, remote)
    except OSError as exc:
        logger.warning('pruning old versions of %s failed: %s', public, exc)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format='%(levelname)s %(message)s')
    # Turn systemd's SIGTERM into SystemExit so temp files get cleaned up
    signal.signal(signal.SIGTERM, lambda signum, _: sys.exit(128 + signum))
    if not TOKEN:
        logger.error('IPINFO_TOKEN is not set')
        return 1
    failed = False
    for path_var, remote_var, default_remote, fields in DATABASES:
        public = os.environ.get(path_var)
        remote = os.environ.get(remote_var) or default_remote
        if not public:
            logger.error('%s is not set', path_var)
            failed = True
            continue
        try:
            refresh(Path(public), remote, fields)
        except Exception:
            logger.error(
                'refreshing %s from %s failed:\n%s',
                public, remote, redact(traceback.format_exc()),
            )
            failed = True
    return 1 if failed else 0


if __name__ == '__main__':
    sys.exit(main())
