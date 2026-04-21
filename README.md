# AlmaLinux Mirrors Service

The backend that powers `mirrors.almalinux.org`. It serves:

- **`/mirrorlist/...`** — the URL DNF/yum clients hit to get a ranked list of mirrors
- **`/isolist/...`** — equivalent endpoint for ISO downloads
- **A web UI** listing available mirrors and ISOs, plus debug/JSON endpoints
- **Periodic mirror health checks** that keep the served list fresh

Mirror ranking is multi-layered: on-net mirrors (matched by ASN or subnet) come first, then nearest-by-geography, then world. See [How mirror selection works](#how-mirror-selection-works).

## Contents

- [Endpoints](#endpoints)
- [How mirror selection works](#how-mirror-selection-works)
- [Query parameters](#query-parameters)
- [Mirror config format](#mirror-config-format)
- [Mirror health checks](#mirror-health-checks)
- [Deploying](#deploying)
- [Configuration / environment variables](#configuration--environment-variables)
- [Statistics](#statistics)

---

## Endpoints

### Client endpoints

| Endpoint | Purpose |
| --- | --- |
| `GET /mirrorlist/<version>/<repository>` | Ranked list of mirror URLs (newline-separated) for a DNF repo. Accepts `protocol`, `country`, `arch` query params. |
| `GET /isolist/<version>/<arch>` | Ranked list of mirror ISO URLs. |
| `GET /isos` / `GET /isos/<arch>/<version>` | HTML page of ISO download mirrors grouped by country with the nearest highlighted. |
| `GET /kitten/isos` / `GET /kitten/isos/<arch>/<version>` | Same, for the AlmaLinux Kitten optional module. |
| `GET /` | Main HTML mirrors table (public, working mirrors only). |
| `GET /kitten` | Mirrors carrying the Kitten optional module. |

### Debug endpoints

Rate-limited to 20 req/min per Nginx config.

| Endpoint | Purpose |
| --- | --- |
| `GET /debug/json/ip_info` | Shows the IP the service sees for you, request headers, and decoded GeoIP. Useful for diagnosing why a mirror list looks wrong. |
| `GET /debug/json/nearest_mirrors` | JSON of the ranked mirrors the service would return for your IP (hardcoded to version 8 / x86_64 ISOs). |
| `GET /debug/json/all_mirrors` | Full mirror database as JSON, including private/offline entries. |
| `GET /debug/html/all` | HTML table of every mirror (including offline, private, cloud). |

---

## How mirror selection works

When a client requests `/mirrorlist/...`, the service runs three passes and returns up to 10 mirrors.

### 1. On-net matching (ASN + subnet)

This runs first. For each mirror:

- **Auto ASN match** — every mirror's resolved IP is looked up in IPInfo's ASN database at update time. If the **requesting client's ASN equals the mirror's auto-detected ASN**, it's considered on-net. This means a client on AS12345 hitting a mirror whose IP is also on AS12345 gets that mirror at the top automatically, with no mirror-config changes required.
- **Forced ASN match** — a mirror can declare one or more ASNs in its config (`asn: 12345` or `asn: [12345, 67890]`). A client on any of those ASNs is considered on-net for that mirror, regardless of where the mirror's own IP sits. This is how a mirror operator declares, "my mirror serves these networks" when the mirror's public IP and the networks it serves aren't on the same ASN.
- **Subnet match** — a mirror can declare CIDR ranges (`subnets: [192.0.2.0/24, ...]`). Client IPs inside any declared range are on-net. Cloud mirrors (see below) use this mechanism.
- **Monopoly** — if a matched mirror is marked `monopoly: true`, **only that mirror is returned** and the remaining passes are skipped. Used for private/corporate mirrors that should exclusively serve their own networks.

If 1–9 on-net mirrors are found, the list is padded up to 10 with the geographically nearest neighbors.

### 2. Geographic ranking (fallback)

If no on-net match, mirrors are sorted by Haversine distance from the client's GeoIP-resolved coordinates, with same-country mirrors preferred. Distance is bucketed into 500 km bands and shuffled within each band, so clients in the same city don't all hammer the same mirror. ASN-aware tiebreaking still applies inside the shuffled bands.

Order of the final list:

1. Same country, within 500 km — shuffled
2. Same country, beyond 500 km — shuffled
3. Other country, within 500 km — shuffled
4. Other country, beyond 500 km — shuffled

Trimmed to 10.

### 3. No geo data

If the client IP can't be geolocated (or is private/unknown), the full mirror list is returned shuffled.

### Cloud region matching

Mirrors hosted inside AWS / Azure / GCP / OCI can set `cloud_type` and `cloud_regions`. During each update cycle, the service fetches the provider's published IP ranges and populates that mirror's `subnets` automatically. Clients inside those cloud ranges then match the mirror via the subnet mechanism above — i.e. AWS clients get AWS mirrors, transparently.

Mirrors meant to serve these large clouds are managed by the infrastructure team within the AlmaLinux OS Foundation.  Community-run mirrors are not allowed to set `cloud_type` or `cloud_regions`.

Cloud mirrors are excluded from the public `/isos/` listings.

### Vault vs. active

Requests for vault (archived) versions bypass ranking entirely and return a single hardcoded `vault.almalinux.org` URL.

---

## Query parameters

All apply to `/mirrorlist/<version>/<repository>`.

| Param | Example | Effect |
| --- | --- | --- |
| `country` | `?country=DE` | **Override GeoIP** and treat the request as coming from this country. 2-letter ISO code required. Useful if you want to pin a client to a specific region. |
| `protocol` | `?protocol=https` | Return only `http` or `https` URLs. Falls back to whatever protocol is available if the requested one isn't published for a given mirror. |
| `arch` | `?arch=aarch64` | Constrain mirror list to ones serving this architecture for the version. |

Example: force a German HTTPS mirror list for AppStream 9:

```
https://mirrors.almalinux.org/mirrorlist/9/appstream?country=DE&protocol=https
```

### Optional modules (Kitten)

Request paths of the form `/mirrorlist/<version>-kitten/<repo>` are auto-detected and return the Kitten-variant URLs published by mirrors that opt in via `address_optional.kitten`.

---

## Mirror config format

Each mirror is a YAML file under the mirrors config directory (pulled from git at deploy time and refreshed on a timer). Schema lives at [src/backend/yaml_snippets/json_schemas/mirror_config/v1.json](src/backend/yaml_snippets/json_schemas/mirror_config/v1.json).

Minimal example:

```yaml
name: repo.example.com
address:
  http: http://repo.example.com/almalinux
  https: https://repo.example.com/almalinux
  rsync: rsync://repo.example.com/almalinux
update_frequency: 3h
sponsor: Example Corp
sponsor_url: https://example.com
email: mirror@example.com
geolocation:
  country: US
  state_province: California
  city: San Francisco
```

### Useful optional fields

```yaml
# Forced ASN matching — treat clients on these ASNs as on-net for this mirror
asn:
  - 12345
  - 67890

# Subnet matching — CIDRs, or a URL to a JSON list of CIDRs
subnets:
  - 192.0.2.0/24
  - 2001:db8::/32
  - https://example.com/our-subnets.json

# Private mirror (behind NAT, not in public list), combined with monopoly
# to serve only its own networks.
private: true
monopoly: true

# Cloud mirror — subnets auto-populated from the provider's published ranges
cloud_type: aws         # aws | azure | gcp | oci
cloud_regions:
  - us-east-1
  - us-west-2
# or: cloud_regions: all

# Optional modules (e.g. Kitten)
address_optional:
  kitten:
    http: http://repo.example.com/almalinux-kitten
    https: https://repo.example.com/almalinux-kitten
```

### Semantic flags

- `private: true` — excluded from the public mirror list. Requires `subnets` (or `asn`) to be discoverable at all. Must also set `monopoly: true`.
- `monopoly: true` — when this mirror matches a request (by ASN or subnet), it is returned **alone**, short-circuiting the rest of selection.

---

## Mirror health checks

`src/backend/update_mirrors.py` runs on a systemd timer. For every configured mirror it:

1. Resolves DNS (A + AAAA; AAAA presence sets the `ipv6` flag).
2. Resolves the mirror's ASN from its IP (IPInfo ASN DB) — this is the **auto ASN** used for on-net matching.
3. Fetches `<mirror>/<version>/<repo>/repodata/repomd.xml` for each configured version/repo/arch.
4. Reads `<mirror>/<version>/TIME` and compares to `allowed_outdate` (e.g. "25 hours"); an old timestamp marks the mirror as `expired`.
5. HEAD-checks all published ISOs + checksums; if everything is present, sets `has_full_iso_set`.
6. For cloud mirrors, refreshes published provider IP ranges and repopulates `subnets`.
7. Resolves geo coordinates (offline IPInfo DB, falling back to LocationIQ if configured).

Failing mirrors are cached as offline in Valkey for 1 hour to avoid constantly re-probing them.

After all mirrors are checked, the DB is rewritten and common mirrorlist variants are pre-warmed into Valkey.

---

## Deploying

Ansible playbook, target server **AlmaLinux 10**.

### Requirements

- Ansible 2.10+
- `community.docker` collection
  ```
  ansible-galaxy collection install community.docker
  ```
- `udondan.ssh-reconnect` role
  ```
  ansible-galaxy install udondan.ssh-reconnect
  ```

### Steps

1. Clone `https://github.com/AlmaLinux/mirrors.git` and check out `mirrors_service`.
2. `cd ci/ansible/inventory`, copy `template` to a new inventory file.
3. Fill in at minimum:
   - `mirrors_service` — IP of the deploy target
   - `container_env_prefix` — `prod` for production, anything else for dev
   - `deploy_environment` — `Production`, `Dev`, etc.
   - `auth_key` — random string used for internal auth
   - `gunicorn_port` — backend port
   - `sentry_dsn` — optional, for error reporting
   - `test_ip_address` — optional, forces a specific IP for dev testing
4. `cd ci/ansible && ansible-playbook -vv -i inventory/<your-inv> -u <sudo-user> --become main.yml`

### What gets installed

- **Gunicorn + Flask app** (`alma-mirrors.service`) — the backend.
- **Nginx** — reverse proxy on :80, Cloudflare-aware `X-Forwarded-For` parsing, rate limiting, gzip.
- **Valkey** — cache. In production typically an external instance.
- **Systemd timers** — `update-mirrors.timer` runs the health check job; `mirror-service-git-updater.timer` pulls fresh mirror configs from git.
- **SQLite** — mirror database at `$SQLITE_PATH`.
- **IPInfo geolocation + ASN databases** — mounted from a shared NFS location (see `ci/ansible/roles/deploy/tasks/common_setup.yml`); sample databases are pulled from IPInfo's public sample repo for dev.

---

## Configuration / environment variables

The backend reads most of its paths from env:

| Variable | Purpose |
| --- | --- |
| `CONFIG_ROOT` | Directory holding service + mirror YAML configs |
| `SOURCE_PATH` | Code checkout location |
| `GEOIP_PATH` | IPInfo geolocation `.mmdb` (typically `/var/ipinfo/standard_location.mmdb`) |
| `ASN_PATH` | IPInfo ASN `.mmdb` (typically `/var/ipinfo/asn.mmdb`) |
| `CONTINENT_PATH` | JSON mapping countries to continents |
| `SQLITE_PATH` | Mirror database file |
| `REDIS_URI` | Write cache |
| `REDIS_URI_RO` | Read cache (can point to a replica) |
| `LOCATIONIQ_KEY` | Optional online geocoding fallback |
| `SENTRY_DSN` | Optional error reporting |
| `DEPLOY_ENVIRONMENT` | Label used in logs/Sentry (`Production`, `Dev`, ...) |
| `TEST_IP_ADDRESS` | Force all requests to be treated as coming from this IP (dev only) |

---
