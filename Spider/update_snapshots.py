# -*- coding: utf-8 -*-
"""
update_snapshots.py — Week2: 快照补抓（1h/3h/6h）

强制要求（来自 README）：
- API: https://api.bilibili.com/x/web-interface/archive/stat
- 快照规则：
  - 1h：now >= pubdate + 1h
  - 3h：now >= pubdate + 3h
  - 6h：now >= pubdate + 6h
- 实现要求：
  - 不允许提前抓
  - 不允许覆盖已有快照
  - 写入 snapshots["1h"/"3h"/"6h"] 与 features[...]（同 key）
"""

from __future__ import annotations

import argparse
import json
import os
import time
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


STAT_API = "https://api.bilibili.com/x/web-interface/archive/stat"

DATA_DIR = "data"
DAILY_DIR = os.path.join(DATA_DIR, "daily")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.bilibili.com/",
}

DELTAS = [("1h", 3600), ("3h", 3 * 3600), ("6h", 6 * 3600)]


def utc_ts() -> int:
    return int(time.time())


def today_str() -> str:
    return date.today().isoformat()


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def read_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def atomic_write_json(path: str, obj: Any) -> None:
    ensure_dir(os.path.dirname(path))
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def request_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    retry = Retry(
        total=3,
        backoff_factor=0.8,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


def recompute_daily_stats(daily: Dict[str, Any]) -> None:
    videos = daily.get("videos", [])
    daily["count"] = len(videos)

    pos = sum(1 for v in videos if int(v.get("label", 0)) == 1)
    neg = sum(1 for v in videos if int(v.get("label", 0)) == 0)
    daily["meta"] = {"pos_count": pos, "neg_count": neg, "total_count": len(videos)}

    cat: Dict[str, Any] = {}
    for v in videos:
        tid = v.get("tid")
        if tid is None:
            continue
        tid_s = str(tid)
        tname = v.get("tname") or ""
        if tid_s not in cat:
            cat[tid_s] = {"tname": tname, "video_count": 0}
        if not cat[tid_s]["tname"] and tname:
            cat[tid_s]["tname"] = tname
        cat[tid_s]["video_count"] += 1
    daily["category_stats"] = cat


def like_rate(like: int, view: int) -> Optional[float]:
    if view <= 0:
        return None
    return round(float(like) / float(view), 6)


def eligible(pubdate: int, now_ts: int, delta_s: int) -> bool:
    if pubdate <= 0:
        return False
    return now_ts >= pubdate + delta_s


def get_stat(s: requests.Session, aid: Optional[int], bvid: Optional[str]) -> Optional[Dict[str, Any]]:
    """
    README 指定 archive/stat。
    官方常用参数是 aid；有些环境可能支持 bvid。
    这里做容错：优先 aid，没有再尝试 bvid。
    """
    params = {}
    if aid and int(aid) > 0:
        params["aid"] = int(aid)
    elif bvid:
        params["bvid"] = str(bvid)
    else:
        return None

    try:
        r = s.get(STAT_API, params=params, timeout=20)
    except requests.RequestException:
        return None

    if r.status_code == 404:
        return None

    try:
        payload = r.json()
    except Exception:
        return None

    if payload.get("code") != 0:
        return None

    data = payload.get("data") or {}
    if not isinstance(data, dict):
        return None

    # 常见字段：view/like/coin...
    return data


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("day", nargs="?", default=today_str(), help="YYYY-MM-DD (default: today)")
    ap.add_argument("--sleep", type=float, default=0.15, help="request sleep seconds (default: 0.15)")
    args = ap.parse_args()

    day = args.day
    now_ts = utc_ts()

    daily_path = os.path.join(DAILY_DIR, f"{day}.json")
    if not os.path.exists(daily_path):
        raise FileNotFoundError(f"daily file not found: {daily_path}")

    daily = read_json(daily_path)
    videos: List[Dict[str, Any]] = daily.get("videos", [])

    s = request_session()

    updated = 0
    skipped_early = 0
    skipped_exists = 0
    failed = 0

    for v in videos:
        pubdate = int(v.get("pubdate") or 0)
        aid = v.get("aid")
        bvid = v.get("bvid")

        snapshots = v.get("snapshots") or {}
        features = v.get("features") or {}
        if not isinstance(snapshots, dict):
            snapshots = {}
        if not isinstance(features, dict):
            features = {}

        # 对每个 delta 按规则补抓
        need_any = False
        for key, ds in DELTAS:
            if key in snapshots:
                continue
            if not eligible(pubdate, now_ts, ds):
                skipped_early += 1
                continue
            need_any = True

        if not need_any:
            v["snapshots"] = snapshots
            v["features"] = features
            continue

        stat = get_stat(s, aid=int(aid) if aid is not None else None, bvid=str(bvid) if bvid else None)
        if stat is None:
            failed += 1
            continue

        view = int(stat.get("view") or 0)
        like = int(stat.get("like") or 0)
        coin = int(stat.get("coin") or 0)

        for key, ds in DELTAS:
            if key in snapshots:
                skipped_exists += 1
                continue
            if not eligible(pubdate, now_ts, ds):
                continue

            snapshots[key] = {"ts": int(now_ts), "view": view, "like": like, "coin": coin}
            features[key] = {"like_rate": like_rate(like, view)}
            updated += 1

        v["snapshots"] = snapshots
        v["features"] = features

        time.sleep(float(args.sleep))

    daily["capture_ts"] = now_ts
    daily["videos"] = videos
    recompute_daily_stats(daily)

    atomic_write_json(daily_path, daily)

    print(
        f"[update_snapshots] day={day} updated_snapshots={updated} "
        f"skipped_early={skipped_early} skipped_exists={skipped_exists} failed={failed} "
        f"total_videos={daily['count']}"
    )
    print(f"[update_snapshots] wrote: {daily_path}")


if __name__ == "__main__":
    main()