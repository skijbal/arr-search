from __future__ import annotations

import os
import sys
import time
import json
import random
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple

import requests


# -----------------------------
# Utilities
# -----------------------------
def env(name: str, default: Optional[str] = None) -> str:
    v = os.getenv(name, default)
    if v is None:
        raise SystemExit(f"Missing required env var: {name}")
    return v


def env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or v.strip() == "":
        return default
    try:
        return int(v)
    except ValueError:
        raise SystemExit(f"Env var {name} must be an integer, got: {v!r}")


def env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None or v.strip() == "":
        return default
    try:
        return float(v)
    except ValueError:
        raise SystemExit(f"Env var {name} must be a number, got: {v!r}")


def env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


def normalize_base_url(url: str) -> str:
    url = url.strip()
    if url.endswith("/"):
        url = url[:-1]
    return url


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def atomic_write_json(path: str, data: Any) -> None:
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, sort_keys=True)
    os.replace(tmp, path)


def now_epoch() -> int:
    return int(time.time())


def pick_random(items: List[int], limit: int) -> List[int]:
    if limit <= 0 or not items:
        return []
    if len(items) <= limit:
        random.shuffle(items)
        return items
    return random.sample(items, limit)


# -----------------------------
# State store: cooldown + shuffle-bag
# -----------------------------
class StateStore:
    """
    Persists:
      cooldowns[bucket][item_id] = last_search_epoch
      shuffle[bucket] = {"bag": [ids not yet searched this cycle], "seen": [ids searched this cycle]}

    Buckets:
      sonarr_missing, sonarr_upgrades, radarr_missing, radarr_upgrades, lidarr_missing, lidarr_upgrades
    """

    def __init__(self, state_path: str, log: logging.Logger):
        self.state_path = state_path
        self.log = log
        self.cooldowns: Dict[str, Dict[str, int]] = {}
        self.shuffle: Dict[str, Dict[str, List[int]]] = {}
        self._load()

    def _load(self) -> None:
        try:
            if os.path.exists(self.state_path):
                with open(self.state_path, "r", encoding="utf-8") as f:
                    data = json.load(f) or {}

                # Backward compatibility: older files had flat cooldown buckets at top-level
                if "cooldowns" in data or "shuffle" in data:
                    self.cooldowns = data.get("cooldowns", {}) or {}
                    self.shuffle = data.get("shuffle", {}) or {}
                else:
                    # Assume old format: {bucket: {id: ts}, ...}
                    self.cooldowns = {
                        k: {str(ik): int(iv) for ik, iv in v.items()}
                        for k, v in (data.items() if isinstance(data, dict) else [])
                        if isinstance(v, dict)
                    }
                    self.shuffle = {}

                # Normalize cooldown types
                self.cooldowns = {
                    str(bucket): {str(i): int(ts) for i, ts in m.items()}
                    for bucket, m in (self.cooldowns.items() if isinstance(self.cooldowns, dict) else [])
                    if isinstance(m, dict)
                }

                def norm_bucket(b: Any) -> Dict[str, List[int]]:
                    if not isinstance(b, dict):
                        return {"bag": [], "seen": []}
                    bag = b.get("bag", [])
                    seen = b.get("seen", [])
                    bag = [int(x) for x in bag if str(x).lstrip("-").isdigit()]
                    seen = [int(x) for x in seen if str(x).lstrip("-").isdigit()]

                    # remove dups but preserve order
                    def uniq(seq: List[int]) -> List[int]:
                        s: Set[int] = set()
                        out: List[int] = []
                        for x in seq:
                            if x not in s:
                                s.add(x)
                                out.append(x)
                        return out

                    return {"bag": uniq(bag), "seen": uniq(seen)}

                self.shuffle = {
                    str(bucket): norm_bucket(v)
                    for bucket, v in (self.shuffle.items() if isinstance(self.shuffle, dict) else [])
                }

        except Exception as e:
            self.log.warning("Failed to load state (%s). Starting fresh.", e)
            self.cooldowns = {}
            self.shuffle = {}

    def save(self) -> None:
        try:
            ensure_dir(os.path.dirname(self.state_path) or ".")
            atomic_write_json(self.state_path, {"cooldowns": self.cooldowns, "shuffle": self.shuffle})
        except Exception as e:
            self.log.warning("Failed to save state: %s", e)

    # ---- Cooldown ----
    def _cd_bucket(self, bucket: str) -> Dict[str, int]:
        if bucket not in self.cooldowns:
            self.cooldowns[bucket] = {}
        return self.cooldowns[bucket]

    def is_cooled_down(self, bucket: str, item_id: int, cooldown_seconds: int) -> bool:
        if cooldown_seconds <= 0:
            return True
        b = self._cd_bucket(bucket)
        last = b.get(str(item_id))
        if last is None:
            return True
        return (now_epoch() - int(last)) >= cooldown_seconds

    def mark_searched(self, bucket: str, item_id: int) -> None:
        self._cd_bucket(bucket)[str(item_id)] = now_epoch()

    # ---- Shuffle bag ----
    def _sh_bucket(self, bucket: str) -> Dict[str, List[int]]:
        if bucket not in self.shuffle:
            self.shuffle[bucket] = {"bag": [], "seen": []}
        b = self.shuffle[bucket]
        if "bag" not in b or not isinstance(b["bag"], list):
            b["bag"] = []
        if "seen" not in b or not isinstance(b["seen"], list):
            b["seen"] = []
        return b

    def _refresh_bucket(self, bucket: str, eligible_ids: List[int]) -> None:
        """
        Keeps current cycle:
          - bag + seen are intersected with eligible
          - new eligible IDs are added to bag (but not if already seen)
          - if bag empties -> start new cycle (bag=shuffled eligible, seen=[])
        """
        eligible_set = set(int(x) for x in eligible_ids)

        b = self._sh_bucket(bucket)
        bag = [int(x) for x in b["bag"] if int(x) in eligible_set]
        seen = [int(x) for x in b["seen"] if int(x) in eligible_set]

        bag_set = set(bag)
        seen_set = set(seen)

        # Add newly-eligible ids that haven't been searched in this cycle
        new_ids = list(eligible_set - bag_set - seen_set)
        random.shuffle(new_ids)
        bag.extend(new_ids)

        # If the bag is empty, start a new cycle
        if not bag:
            bag = list(eligible_set)
            random.shuffle(bag)
            seen = []

        b["bag"] = bag
        b["seen"] = seen

    def draw_no_repeat(
        self,
        bucket: str,
        eligible_ids: List[int],
        count: int,
        cooldown_seconds: int,
        mark: bool,
    ) -> List[int]:
        """
        Draw up to `count` items without repeating until cycle completes.

        Cooldown is applied while drawing:
          - If the front item is not cooled down, rotate it to end and try next.
          - Stops if none are cooled down in a full pass.

        If mark=True, we record the cooldown timestamp when an item is picked.
        """
        if count <= 0:
            return []
        eligible_ids = [int(x) for x in eligible_ids]
        if not eligible_ids:
            return []

        self._refresh_bucket(bucket, eligible_ids)
        b = self._sh_bucket(bucket)

        picked: List[int] = []

        for _ in range(count):
            if not b["bag"]:
                self._refresh_bucket(bucket, eligible_ids)
                if not b["bag"]:
                    break

            attempts = 0
            max_attempts = len(b["bag"])
            chosen: Optional[int] = None

            while attempts < max_attempts and b["bag"]:
                candidate = int(b["bag"][0])

                if self.is_cooled_down(bucket, candidate, cooldown_seconds):
                    chosen = candidate
                    break

                # rotate to end
                b["bag"] = b["bag"][1:] + [candidate]
                attempts += 1

            if chosen is None:
                break

            # consume chosen from front
            b["bag"] = b["bag"][1:]
            b["seen"].append(chosen)

            if mark:
                self.mark_searched(bucket, chosen)

            picked.append(chosen)

        return picked


def cooldown_seconds_for(prefix: str, mode: str, default_seconds: int) -> int:
    """
    Env precedence:
      <PREFIX>_<MODE>_COOLDOWN_HOURS
      <PREFIX>_COOLDOWN_HOURS
      COOLDOWN_HOURS
      DEFAULT_COOLDOWN_HOURS
    """
    specific = os.getenv(f"{prefix}_{mode}_COOLDOWN_HOURS")
    if specific and specific.strip():
        return max(0, int(env_float(f"{prefix}_{mode}_COOLDOWN_HOURS", 0.0) * 3600))

    appwide = os.getenv(f"{prefix}_COOLDOWN_HOURS")
    if appwide and appwide.strip():
        return max(0, int(env_float(f"{prefix}_COOLDOWN_HOURS", 0.0) * 3600))

    global_hours = os.getenv("COOLDOWN_HOURS")
    if global_hours and global_hours.strip():
        return max(0, int(env_float("COOLDOWN_HOURS", 0.0) * 3600))

    return default_seconds


# -----------------------------
# HTTP Client
# -----------------------------
class ArrClient:
    def __init__(self, base_url: str, api_key: str, api_prefix: str, timeout_s: int = 30):
        self.base_url = normalize_base_url(base_url)
        self.api_key = api_key
        self.api_prefix = api_prefix
        self.timeout_s = timeout_s
        self.session = requests.Session()
        self.session.headers.update({
            "X-Api-Key": self.api_key,
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "arr-tag-searcher/2.0",
        })

    def _url(self, path: str) -> str:
        if not path.startswith("/"):
            path = "/" + path
        return f"{self.base_url}{self.api_prefix}{path}"

    def get_json(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        r = self.session.get(self._url(path), params=params, timeout=self.timeout_s)
        if r.status_code >= 400:
            raise RuntimeError(f"GET {path} failed: {r.status_code} {r.text[:300]}")
        return r.json()

    def put_json(self, path: str, payload: Dict[str, Any]) -> Any:
        r = self.session.put(self._url(path), data=json.dumps(payload), timeout=self.timeout_s)
        if r.status_code >= 400:
            raise RuntimeError(f"PUT {path} failed: {r.status_code} {r.text[:300]}")
        if r.text.strip():
            return r.json()
        return {}

    def post_json(self, path: str, payload: Dict[str, Any]) -> Any:
        r = self.session.post(self._url(path), data=json.dumps(payload), timeout=self.timeout_s)
        if r.status_code >= 400:
            raise RuntimeError(f"POST {path} failed: {r.status_code} {r.text[:300]}")
        if r.text.strip():
            return r.json()
        return {}

    def paged_records(self, path: str, page_size: int, max_records: int) -> List[Dict[str, Any]]:
        """
        Arr wanted endpoints typically respond with:
          { page, pageSize, totalRecords, records: [...] }
        """
        out: List[Dict[str, Any]] = []
        page = 1
        while True:
            data = self.get_json(path, params={"page": page, "pageSize": page_size})
            records = data.get("records") or []
            total = data.get("totalRecords", 0)

            out.extend(records)
            if max_records > 0 and len(out) >= max_records:
                return out[:max_records]

            if not records:
                return out
            if len(out) >= total:
                return out

            page += 1


# -----------------------------
# Config
# -----------------------------
@dataclass
class AppConfig:
    enabled: bool
    url: str
    api_key: str


@dataclass
class Limits:
    sonarr_missing: int
    sonarr_upgrades: int
    radarr_missing: int
    radarr_upgrades: int
    lidarr_missing: int
    lidarr_upgrades: int

    sonarr_promote: int
    radarr_promote: int
    lidarr_promote: int


# -----------------------------
# Helpers
# -----------------------------
def get_tag_ids(client: ArrClient, tag_search_label: str, tag_done_label: str) -> Tuple[Optional[int], Optional[int]]:
    tags = client.get_json("/tag")
    search_id = None
    done_id = None
    for t in tags:
        if str(t.get("label", "")).lower() == tag_search_label.lower():
            search_id = t.get("id")
        if str(t.get("label", "")).lower() == tag_done_label.lower():
            done_id = t.get("id")
    return search_id, done_id


def build_id_to_tags(items: List[Dict[str, Any]], id_field: str = "id") -> Dict[int, Set[int]]:
    out: Dict[int, Set[int]] = {}
    for it in items:
        _id = it.get(id_field)
        if _id is None:
            continue
        tags = it.get("tags") or []
        out[int(_id)] = set(int(x) for x in tags if isinstance(x, (int, float, str)) and str(x).isdigit())
    return out


def extract_id(record: Dict[str, Any], *keys: str) -> Optional[int]:
    for k in keys:
        v = record.get(k)
        if isinstance(v, int):
            return v
        if isinstance(v, str) and v.isdigit():
            return int(v)
    for k in keys:
        v = record.get(k)
        if isinstance(v, dict):
            inner = v.get("id")
            if isinstance(inner, int):
                return inner
    return None


def retag_item_get_put(
    client: ArrClient,
    item_type: str,   # "series" | "movie" | "artist"
    item_id: int,
    search_tag_id: int,
    done_tag_id: int,
    dry_run: bool,
    log: logging.Logger,
) -> None:
    obj = client.get_json(f"/{item_type}/{item_id}")
    tags = obj.get("tags") or []
    tags_set = {int(t) for t in tags if str(t).isdigit()}

    changed = False
    if search_tag_id in tags_set:
        tags_set.remove(search_tag_id)
        changed = True
    if done_tag_id not in tags_set:
        tags_set.add(done_tag_id)
        changed = True

    if not changed:
        return

    obj["tags"] = sorted(tags_set)

    if dry_run:
        log.info("%s DRY_RUN: would retag id=%d remove=%d add=%d", item_type.capitalize(), item_id, search_tag_id, done_tag_id)
        return

    client.put_json(f"/{item_type}/{item_id}", obj)
    log.info("%s: retagged id=%d (search->done)", item_type.capitalize(), item_id)


def promote_search_to_done_if_no_missing(
    *,
    app_name: str,
    client: ArrClient,
    item_type: str,                 # "series" | "movie" | "artist"
    all_items_path: str,            # "/series" | "/movie" | "/artist"
    missing_id_keys: Tuple[str, ...],
    tag_search_label: str,
    tag_done_label: str,
    wanted_page_size: int,
    promote_limit: int,
    dry_run: bool,
    log: logging.Logger,
) -> None:
    if promote_limit <= 0:
        return

    search_tag_id, done_tag_id = get_tag_ids(client, tag_search_label, tag_done_label)
    if search_tag_id is None:
        log.warning("%s: tag %r not found; cannot promote search->done.", app_name, tag_search_label)
        return
    if done_tag_id is None:
        log.warning("%s: tag %r not found; cannot promote search->done.", app_name, tag_done_label)
        return

    missing_records = client.paged_records("/wanted/missing", page_size=wanted_page_size, max_records=0)
    missing_ids: Set[int] = set()
    for r in missing_records:
        mid = extract_id(r, *missing_id_keys)
        if mid is not None:
            missing_ids.add(int(mid))

    all_items = client.get_json(all_items_path)
    id_to_tags = build_id_to_tags(all_items, "id")
    search_tagged_ids = [iid for iid, tags in id_to_tags.items() if int(search_tag_id) in tags]

    eligible = [iid for iid in search_tagged_ids if iid not in missing_ids]
    picked = pick_random(eligible, promote_limit)

    log.info(
        "%s: promote search->done candidates(tag=search)=%d eligible(no missing)=%d picked=%d",
        app_name, len(search_tagged_ids), len(eligible), len(picked)
    )

    for iid in picked:
        retag_item_get_put(
            client=client,
            item_type=item_type,
            item_id=iid,
            search_tag_id=int(search_tag_id),
            done_tag_id=int(done_tag_id),
            dry_run=dry_run,
            log=log,
        )


# -----------------------------
# Search runners
# -----------------------------
def sonarr_run_once(
    client: ArrClient,
    tag_search: str,
    tag_done: str,
    limits: Limits,
    wanted_page_size: int,
    dry_run: bool,
    log: logging.Logger,
    state: StateStore,
    cooldown_missing_s: int,
    cooldown_upgrades_s: int,
) -> None:
    search_tag_id, done_tag_id = get_tag_ids(client, tag_search, tag_done)
    if search_tag_id is None:
        log.warning("Sonarr: tag %r not found; missing-search pass will do nothing.", tag_search)
    if done_tag_id is None:
        log.warning("Sonarr: tag %r not found; upgrades pass will do nothing.", tag_done)

    series = client.get_json("/series")
    series_tags = build_id_to_tags(series, "id")

    # Missing by tag=search
    if search_tag_id is not None and limits.sonarr_missing > 0:
        missing = client.paged_records("/wanted/missing", page_size=wanted_page_size, max_records=0)
        missing_series_ids = sorted({extract_id(r, "seriesId") for r in missing if extract_id(r, "seriesId") is not None})
        eligible = [int(sid) for sid in missing_series_ids if int(search_tag_id) in series_tags.get(int(sid), set())]

        picked = state.draw_no_repeat(
            bucket="sonarr_missing",
            eligible_ids=eligible,
            count=limits.sonarr_missing,
            cooldown_seconds=cooldown_missing_s,
            mark=not dry_run,
        )

        log.info("Sonarr: missing eligible=%d picked=%d", len(eligible), len(picked))

        for sid in picked:
            payload = {"name": "SeriesSearch", "seriesId": sid}
            if dry_run:
                log.info("Sonarr DRY_RUN: POST /command %s", payload)
            else:
                client.post_json("/command", payload)
                log.info("Sonarr: triggered SeriesSearch for seriesId=%d", sid)

    # Upgrades by tag=done
    if done_tag_id is not None and limits.sonarr_upgrades > 0:
        cutoff = client.paged_records("/wanted/cutoff", page_size=wanted_page_size, max_records=0)
        cutoff_series_ids = sorted({extract_id(r, "seriesId") for r in cutoff if extract_id(r, "seriesId") is not None})
        eligible = [int(sid) for sid in cutoff_series_ids if int(done_tag_id) in series_tags.get(int(sid), set())]

        picked = state.draw_no_repeat(
            bucket="sonarr_upgrades",
            eligible_ids=eligible,
            count=limits.sonarr_upgrades,
            cooldown_seconds=cooldown_upgrades_s,
            mark=not dry_run,
        )

        log.info("Sonarr: upgrades eligible=%d picked=%d", len(eligible), len(picked))

        for sid in picked:
            payload = {"name": "SeriesSearch", "seriesId": sid}
            if dry_run:
                log.info("Sonarr DRY_RUN: POST /command %s", payload)
            else:
                client.post_json("/command", payload)
                log.info("Sonarr: triggered SeriesSearch for seriesId=%d", sid)


def radarr_run_once(
    client: ArrClient,
    tag_search: str,
    tag_done: str,
    limits: Limits,
    wanted_page_size: int,
    dry_run: bool,
    log: logging.Logger,
    state: StateStore,
    cooldown_missing_s: int,
    cooldown_upgrades_s: int,
) -> None:
    search_tag_id, done_tag_id = get_tag_ids(client, tag_search, tag_done)
    if search_tag_id is None:
        log.warning("Radarr: tag %r not found; missing-search pass will do nothing.", tag_search)
    if done_tag_id is None:
        log.warning("Radarr: tag %r not found; upgrades pass will do nothing.", tag_done)

    movies = client.get_json("/movie")
    movie_tags = build_id_to_tags(movies, "id")

    # Missing by tag=search
    if search_tag_id is not None and limits.radarr_missing > 0:
        missing = client.paged_records("/wanted/missing", page_size=wanted_page_size, max_records=0)
        missing_movie_ids = sorted({extract_id(r, "movieId", "movie") for r in missing if extract_id(r, "movieId", "movie") is not None})
        eligible = [int(mid) for mid in missing_movie_ids if int(search_tag_id) in movie_tags.get(int(mid), set())]

        picked = state.draw_no_repeat(
            bucket="radarr_missing",
            eligible_ids=eligible,
            count=limits.radarr_missing,
            cooldown_seconds=cooldown_missing_s,
            mark=not dry_run,
        )

        log.info("Radarr: missing eligible=%d picked=%d", len(eligible), len(picked))

        if picked:
            payload = {"name": "MoviesSearch", "movieIds": picked}
            if dry_run:
                log.info("Radarr DRY_RUN: POST /command %s", payload)
            else:
                client.post_json("/command", payload)
                log.info("Radarr: triggered MoviesSearch for movieIds=%s", picked)

    # Upgrades by tag=done
    if done_tag_id is not None and limits.radarr_upgrades > 0:
        cutoff = client.paged_records("/wanted/cutoff", page_size=wanted_page_size, max_records=0)
        cutoff_movie_ids = sorted({extract_id(r, "movieId", "movie") for r in cutoff if extract_id(r, "movieId", "movie") is not None})
        eligible = [int(mid) for mid in cutoff_movie_ids if int(done_tag_id) in movie_tags.get(int(mid), set())]

        picked = state.draw_no_repeat(
            bucket="radarr_upgrades",
            eligible_ids=eligible,
            count=limits.radarr_upgrades,
            cooldown_seconds=cooldown_upgrades_s,
            mark=not dry_run,
        )

        log.info("Radarr: upgrades eligible=%d picked=%d", len(eligible), len(picked))

        if picked:
            payload = {"name": "MoviesSearch", "movieIds": picked}
            if dry_run:
                log.info("Radarr DRY_RUN: POST /command %s", payload)
            else:
                client.post_json("/command", payload)
                log.info("Radarr: triggered MoviesSearch for movieIds=%s", picked)


def lidarr_run_once(
    client: ArrClient,
    tag_search: str,
    tag_done: str,
    limits: Limits,
    wanted_page_size: int,
    dry_run: bool,
    log: logging.Logger,
    state: StateStore,
    cooldown_missing_s: int,
    cooldown_upgrades_s: int,
) -> None:
    search_tag_id, done_tag_id = get_tag_ids(client, tag_search, tag_done)
    if search_tag_id is None:
        log.warning("Lidarr: tag %r not found; missing-search pass will do nothing.", tag_search)
    if done_tag_id is None:
        log.warning("Lidarr: tag %r not found; upgrades pass will do nothing.", tag_done)

    artists = client.get_json("/artist")
    artist_tags = build_id_to_tags(artists, "id")

    # Missing by tag=search
    if search_tag_id is not None and limits.lidarr_missing > 0:
        missing = client.paged_records("/wanted/missing", page_size=wanted_page_size, max_records=0)
        missing_artist_ids = sorted({extract_id(r, "artistId", "artist") for r in missing if extract_id(r, "artistId", "artist") is not None})
        eligible = [int(aid) for aid in missing_artist_ids if int(search_tag_id) in artist_tags.get(int(aid), set())]

        picked = state.draw_no_repeat(
            bucket="lidarr_missing",
            eligible_ids=eligible,
            count=limits.lidarr_missing,
            cooldown_seconds=cooldown_missing_s,
            mark=not dry_run,
        )

        log.info("Lidarr: missing eligible=%d picked=%d", len(eligible), len(picked))

        for aid in picked:
            payload = {"name": "ArtistSearch", "artistId": aid}
            if dry_run:
                log.info("Lidarr DRY_RUN: POST /command %s", payload)
            else:
                client.post_json("/command", payload)
                log.info("Lidarr: triggered ArtistSearch for artistId=%d", aid)

    # Upgrades by tag=done
    if done_tag_id is not None and limits.lidarr_upgrades > 0:
        cutoff = client.paged_records("/wanted/cutoff", page_size=wanted_page_size, max_records=0)
        cutoff_artist_ids = sorted({extract_id(r, "artistId", "artist") for r in cutoff if extract_id(r, "artistId", "artist") is not None})
        eligible = [int(aid) for aid in cutoff_artist_ids if int(done_tag_id) in artist_tags.get(int(aid), set())]

        picked = state.draw_no_repeat(
            bucket="lidarr_upgrades",
            eligible_ids=eligible,
            count=limits.lidarr_upgrades,
            cooldown_seconds=cooldown_upgrades_s,
            mark=not dry_run,
        )

        log.info("Lidarr: upgrades eligible=%d picked=%d", len(eligible), len(picked))

        for aid in picked:
            payload = {"name": "ArtistSearch", "artistId": aid}
            if dry_run:
                log.info("Lidarr DRY_RUN: POST /command %s", payload)
            else:
                client.post_json("/command", payload)
                log.info("Lidarr: triggered ArtistSearch for artistId=%d", aid)


# -----------------------------
# Main
# -----------------------------
def main() -> None:
    log_level = env("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
        stream=sys.stdout,
    )
    log = logging.getLogger("arr-tag-searcher")

    seed = os.getenv("RANDOM_SEED")
    if seed:
        random.seed(seed)

    tag_search = env("TAG_SEARCH", "search")
    tag_done = env("TAG_DONE", "done")

    interval_minutes = env_int("RUN_INTERVAL_MINUTES", 60)
    wanted_page_size = env_int("WANTED_PAGE_SIZE", 200)
    timeout_s = env_int("HTTP_TIMEOUT_SECONDS", 30)
    dry_run = env_bool("DRY_RUN", False)

    # State
    state_dir = env("STATE_DIR", "/data/state")
    ensure_dir(state_dir)
    state_path = os.path.join(state_dir, "state.json")
    state = StateStore(state_path, log)

    default_cooldown_s = max(0, env_int("DEFAULT_COOLDOWN_HOURS", 0)) * 3600

    auto_promote = env_bool("AUTO_PROMOTE_SEARCH_TO_DONE", True)

    # Apps
    sonarr = AppConfig(enabled=env_bool("SONARR_ENABLED", True), url=env("SONARR_URL", ""), api_key=env("SONARR_API_KEY", ""))
    radarr = AppConfig(enabled=env_bool("RADARR_ENABLED", True), url=env("RADARR_URL", ""), api_key=env("RADARR_API_KEY", ""))
    lidarr = AppConfig(enabled=env_bool("LIDARR_ENABLED", True), url=env("LIDARR_URL", ""), api_key=env("LIDARR_API_KEY", ""))

    # Limits
    limits = Limits(
        sonarr_missing=env_int("SONARR_MISSING_LIMIT", 10),
        sonarr_upgrades=env_int("SONARR_UPGRADES_LIMIT", 10),
        radarr_missing=env_int("RADARR_MISSING_LIMIT", 10),
        radarr_upgrades=env_int("RADARR_UPGRADES_LIMIT", 10),
        lidarr_missing=env_int("LIDARR_MISSING_LIMIT", 10),
        lidarr_upgrades=env_int("LIDARR_UPGRADES_LIMIT", 10),
        sonarr_promote=env_int("SONARR_PROMOTE_LIMIT", 50),
        radarr_promote=env_int("RADARR_PROMOTE_LIMIT", 50),
        lidarr_promote=env_int("LIDARR_PROMOTE_LIMIT", 50),
    )

    def sleep_loop():
        time.sleep(max(5, interval_minutes * 60))

    while True:
        try:
            if sonarr.enabled and sonarr.url and sonarr.api_key:
                sc = ArrClient(sonarr.url, sonarr.api_key, api_prefix="/api/v3", timeout_s=timeout_s)
                sonarr_run_once(
                    sc, tag_search, tag_done, limits, wanted_page_size, dry_run, log, state,
                    cooldown_missing_s=cooldown_seconds_for("SONARR", "MISSING", default_cooldown_s),
                    cooldown_upgrades_s=cooldown_seconds_for("SONARR", "UPGRADES", default_cooldown_s),
                )
                if auto_promote:
                    promote_search_to_done_if_no_missing(
                        app_name="Sonarr",
                        client=sc,
                        item_type="series",
                        all_items_path="/series",
                        missing_id_keys=("seriesId",),
                        tag_search_label=tag_search,
                        tag_done_label=tag_done,
                        wanted_page_size=wanted_page_size,
                        promote_limit=limits.sonarr_promote,
                        dry_run=dry_run,
                        log=log,
                    )
            elif sonarr.enabled:
                log.warning("Sonarr enabled but SONARR_URL/SONARR_API_KEY not set; skipping.")

            if radarr.enabled and radarr.url and radarr.api_key:
                rc = ArrClient(radarr.url, radarr.api_key, api_prefix="/api/v3", timeout_s=timeout_s)
                radarr_run_once(
                    rc, tag_search, tag_done, limits, wanted_page_size, dry_run, log, state,
                    cooldown_missing_s=cooldown_seconds_for("RADARR", "MISSING", default_cooldown_s),
                    cooldown_upgrades_s=cooldown_seconds_for("RADARR", "UPGRADES", default_cooldown_s),
                )
                if auto_promote:
                    promote_search_to_done_if_no_missing(
                        app_name="Radarr",
                        client=rc,
                        item_type="movie",
                        all_items_path="/movie",
                        missing_id_keys=("movieId", "movie"),
                        tag_search_label=tag_search,
                        tag_done_label=tag_done,
                        wanted_page_size=wanted_page_size,
                        promote_limit=limits.radarr_promote,
                        dry_run=dry_run,
                        log=log,
                    )
            elif radarr.enabled:
                log.warning("Radarr enabled but RADARR_URL/RADARR_API_KEY not set; skipping.")

            if lidarr.enabled and lidarr.url and lidarr.api_key:
                lc = ArrClient(lidarr.url, lidarr.api_key, api_prefix="/api/v1", timeout_s=timeout_s)
                lidarr_run_once(
                    lc, tag_search, tag_done, limits, wanted_page_size, dry_run, log, state,
                    cooldown_missing_s=cooldown_seconds_for("LIDARR", "MISSING", default_cooldown_s),
                    cooldown_upgrades_s=cooldown_seconds_for("LIDARR", "UPGRADES", default_cooldown_s),
                )
                if auto_promote:
                    promote_search_to_done_if_no_missing(
                        app_name="Lidarr",
                        client=lc,
                        item_type="artist",
                        all_items_path="/artist",
                        missing_id_keys=("artistId", "artist"),
                        tag_search_label=tag_search,
                        tag_done_label=tag_done,
                        wanted_page_size=wanted_page_size,
                        promote_limit=limits.lidarr_promote,
                        dry_run=dry_run,
                        log=log,
                    )
            elif lidarr.enabled:
                log.warning("Lidarr enabled but LIDARR_URL/LIDARR_API_KEY not set; skipping.")

        except Exception as e:
            log.exception("Run failed: %s", e)
        finally:
            state.save()

        sleep_loop()


if __name__ == "__main__":
    main()
