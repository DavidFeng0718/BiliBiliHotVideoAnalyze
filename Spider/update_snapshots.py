# -*- coding: utf-8 -*-
"""
update_snapshots.py — Week2: 快照补抓（宽松版，按捕捉次数）

✅ 仅使用 bvid 调 archive/stat
✅ daily 文件名按北京时间（Asia/Shanghai）
✅ 不再用 pubdate 时间判断
✅ 每个视频每次运行最多写 1 个槽位：
   第1次 -> 1h
   第2次 -> 3h
   第3次 -> 6h
   第4次 -> 12h
   超过4次 -> 跳过
✅ 不覆盖已有快照
✅ 更稳健：connect/read 超时、异常吞掉继续跑
"""

from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from zoneinfo import ZoneInfo  # Python 3.9+

STAT_API = "https://api.bilibili.com/x/web-interface/wbi/view"

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

SLOTS = ["1h", "3h", "6h", "12h"]


def now_beijing() -> datetime:
    return datetime.now(ZoneInfo("Asia/Shanghai"))


def today_str_beijing() -> str:
    return now_beijing().date().isoformat()


def utc_ts() -> int:
    return int(time.time())


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
    adapter = HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


def like_rate(like: int, view: int) -> Optional[float]:
    if view <= 0:
        return None
    return round(float(like) / float(view), 6)


def next_slot(snapshots: Dict[str, Any]) -> Optional[str]:
    """根据已有快照数量，找到下一个未写入的槽位"""
    if not isinstance(snapshots, dict):
        return SLOTS[0]
    for k in SLOTS:
        if k not in snapshots:
            return k
    return None


def get_stat_by_bvid(s: requests.Session, bvid: str) -> Optional[Dict[str, Any]]:
    """
    仅 bvid 调用 archive/stat
    更稳健：timeout 用 (connect, read)，避免长时间卡住
    """
    try:
        r = s.get(STAT_API, params={"bvid": bvid}, timeout=(5, 12))
    except (requests.Timeout, requests.RequestException):
        return None

    if r.status_code == 404:
        return None

    try:
        payload = r.json()
    except Exception:
        return None

    if payload.get("code") != 0:
        return None

    data = payload.get("data")
    if not isinstance(data, dict):
        return None

    return data


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


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("day", nargs="?", default=today_str_beijing(), help="YYYY-MM-DD (default: Beijing today)")
    ap.add_argument("--sleep", type=float, default=0.20, help="sleep seconds per request (default: 0.20)")
    ap.add_argument("--log_every", type=int, default=50, help="print progress every N videos (default: 50)")
    args = ap.parse_args()

    day = args.day
    daily_path = os.path.join(DAILY_DIR, f"{day}.json")
    if not os.path.exists(daily_path):
        raise FileNotFoundError(f"daily file not found: {daily_path}")

    daily = read_json(daily_path)
    videos: List[Dict[str, Any]] = daily.get("videos", [])

    s = request_session()

    updated = 0
    skipped_full = 0
    skipped_no_bvid = 0
    failed = 0

    total = len(videos)

    for i, v in enumerate(videos, 1):
        bvid = str(v.get("bvid") or "").strip()
        if not bvid:
            skipped_no_bvid += 1
            continue

        snapshots = v.get("snapshots") or {}
        features = v.get("features") or {}
        if not isinstance(snapshots, dict):
            snapshots = {}
        if not isinstance(features, dict):
            features = {}

        slot = next_slot(snapshots)
        if slot is None:
            skipped_full += 1
            v["snapshots"] = snapshots
            v["features"] = features
            continue

        stat = get_stat_by_bvid(s, bvid)
        if stat is None:
            failed += 1
            continue

        now_ts = utc_ts()

        # wbi/view 的统计字段在 data.stat 里
        stat_obj = stat.get("stat") or {}
        if not isinstance(stat_obj, dict):
            stat_obj = {}

        view = int(stat_obj.get("view") or 0)
        like = int(stat_obj.get("like") or 0)
        coin = int(stat_obj.get("coin") or 0)

        # 每次运行只写一个槽位（第1次/第2次/第3次/第4次）
        snapshots[slot] = {
            "ts": now_ts,
            "view": view,
            "like": like,
            "coin": coin,
        }
        features[slot] = {
            "like_rate": like_rate(like, view),
        }
        updated += 1

        v["snapshots"] = snapshots
        v["features"] = features

        if args.log_every > 0 and i % int(args.log_every) == 0:
            print(f"[update_snapshots] progress {i}/{total} updated={updated} failed={failed} skipped_full={skipped_full}")

        time.sleep(float(args.sleep))

    daily["capture_ts"] = utc_ts()
    daily["videos"] = videos
    recompute_daily_stats(daily)

    atomic_write_json(daily_path, daily)

    print(
        f"[update_snapshots] day={day} updated={updated} failed={failed} "
        f"skipped_full={skipped_full} skipped_no_bvid={skipped_no_bvid} total_videos={daily['count']}"
    )
    print(f"[update_snapshots] wrote: {daily_path}")


if __name__ == "__main__":
    main()