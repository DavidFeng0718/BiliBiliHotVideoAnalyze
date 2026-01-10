# -*- coding: utf-8 -*-
"""
collect_popular.py — Week2: 热门正样本采集（label=1）

强制要求（来自 README）：
- API: https://api.bilibili.com/x/web-interface/popular
- 支持多页 pn=1..PN_MAX
- 若 data.list 为空，立即停止分页
- 每页原始响应保存到：
  data/raw/popular/YYYY-MM-DDTHHMMSS_pnX.json
- 所有视频：
  - label = 1
  - 写入 snapshots["0h"]
  - 写入 features["0h"]
- 合并规则（按 bvid）：
  - snapshots/features 取并集（不覆盖已有）
  - tid/tname/stat 等：用非空新值补旧值
  - label：只要出现过 1 → 永远 1
- 输出合并进：data/daily/YYYY-MM-DD.json
- 自动更新：count/meta/category_stats
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, date
from typing import Any, Dict, Optional, List

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


POPULAR_API = "https://api.bilibili.com/x/web-interface/popular"

DATA_DIR = "data"
RAW_DIR = os.path.join(DATA_DIR, "raw", "popular")
DAILY_DIR = os.path.join(DATA_DIR, "daily")

PN_MAX = 100
PS = 50

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.bilibili.com/",
}


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def utc_ts() -> int:
    return int(time.time())


def today_str() -> str:
    return date.today().isoformat()


def now_compact() -> str:
    return datetime.now().strftime("%Y-%m-%dT%H%M%S")


def atomic_write_json(path: str, obj: Any) -> None:
    ensure_dir(os.path.dirname(path))
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def read_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


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


def like_rate(like: Optional[int], view: Optional[int]) -> Optional[float]:
    try:
        if like is None or view is None or view <= 0:
            return None
        return round(float(like) / float(view), 6)
    except Exception:
        return None


def build_daily_skeleton(day: str, capture_ts: int) -> Dict[str, Any]:
    return {
        "date": day,
        "capture_ts": capture_ts,
        "source": "bilibili_popular",
        "count": 0,
        "category_stats": {},
        "meta": {"pos_count": 0, "neg_count": 0, "total_count": 0},
        "videos": [],
    }


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


def merge_video(old: Dict[str, Any], new: Dict[str, Any]) -> Dict[str, Any]:
    """
    README 合并规则：
    - snapshots/features：取并集（不覆盖已有）
    - tid/tname/stat/url/title/pubdate/up：用非空新值补旧值
    - label：只要有一次是 1 → 永远 1
    """
    merged = dict(old)

    # label: max
    merged["label"] = 1 if (int(old.get("label", 0)) == 1 or int(new.get("label", 0)) == 1) else 0

    # prefer earliest first_seen_ts
    fst_old = old.get("first_seen_ts")
    fst_new = new.get("first_seen_ts")
    if fst_old is None:
        merged["first_seen_ts"] = fst_new
    elif fst_new is None:
        merged["first_seen_ts"] = fst_old
    else:
        merged["first_seen_ts"] = min(int(fst_old), int(fst_new))

    # scalar fields: fill if old empty
    for k in ["aid", "title", "url", "tid", "tname", "pubdate"]:
        ov = old.get(k)
        nv = new.get(k)
        if (ov is None or ov == "" or ov == 0) and (nv is not None and nv != "" and nv != 0):
            merged[k] = nv

    # up object: fill missing subfields
    merged_up = dict(old.get("up") or {})
    new_up = new.get("up") or {}
    for k in ["mid", "name", "follower"]:
        if (merged_up.get(k) in [None, "", 0]) and (new_up.get(k) not in [None, "", 0]):
            merged_up[k] = new_up.get(k)
    merged["up"] = merged_up

    # snapshots/features: union, do not overwrite existing keys
    merged_snap = dict(old.get("snapshots") or {})
    new_snap = new.get("snapshots") or {}
    for dk, dv in new_snap.items():
        if dk not in merged_snap:
            merged_snap[dk] = dv
    merged["snapshots"] = merged_snap

    merged_feat = dict(old.get("features") or {})
    new_feat = new.get("features") or {}
    for dk, dv in new_feat.items():
        if dk not in merged_feat:
            merged_feat[dk] = dv
    merged["features"] = merged_feat

    return merged


def index_by_bvid(videos: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for v in videos:
        bvid = v.get("bvid")
        if not bvid:
            continue
        out[str(bvid)] = v
    return out


def load_or_init_daily(day: str, capture_ts: int) -> Dict[str, Any]:
    ensure_dir(DAILY_DIR)
    daily_path = os.path.join(DAILY_DIR, f"{day}.json")
    if os.path.exists(daily_path):
        daily = read_json(daily_path)
        # keep existing structure; update capture_ts to latest run
        daily["date"] = day
        daily["capture_ts"] = capture_ts
        if "videos" not in daily:
            daily["videos"] = []
        return daily
    return build_daily_skeleton(day, capture_ts)


def parse_popular_item(item: Dict[str, Any], capture_ts: int) -> Optional[Dict[str, Any]]:
    bvid = item.get("bvid")
    if not bvid:
        # 有些返回可能只有 aid；但你的主键是 bvid，缺 bvid 直接跳过
        return None

    aid = item.get("aid") or item.get("id") or item.get("aid", 0)

    tid = item.get("tid")
    tname = item.get("tname") or ""

    pubdate = item.get("pubdate") or item.get("ctime") or 0

    owner = item.get("owner") or {}
    up = {
        "mid": owner.get("mid") or item.get("mid") or 0,
        "name": owner.get("name") or item.get("author") or "",
        # popular 接口通常不给 follower，这里保持空，后续你如需要可再补
        "follower": None,
    }

    stat = item.get("stat") or {}
    view = stat.get("view")
    like = stat.get("like")
    coin = stat.get("coin")

    rec = {
        "bvid": str(bvid),
        "aid": int(aid) if aid is not None else 0,
        "label": 1,
        "title": item.get("title") or "",
        "url": f"https://www.bilibili.com/video/{bvid}",
        "tid": int(tid) if tid is not None else None,
        "tname": tname,
        "pubdate": int(pubdate) if pubdate else 0,
        "first_seen_ts": int(capture_ts),
        "up": up,
        "snapshots": {
            "0h": {
                "ts": int(capture_ts),
                "view": int(view) if view is not None else 0,
                "like": int(like) if like is not None else 0,
                "coin": int(coin) if coin is not None else 0,
            }
        },
        "features": {
            "0h": {
                "like_rate": like_rate(
                    int(like) if like is not None else 0,
                    int(view) if view is not None else 0,
                )
            }
        },
    }
    return rec


def main() -> None:
    day = today_str()
    capture_ts = utc_ts()

    ensure_dir(RAW_DIR)
    daily = load_or_init_daily(day, capture_ts)
    existing_map = index_by_bvid(daily.get("videos", []))

    s = request_session()
    new_cnt = 0
    merged_cnt = 0

    for pn in range(1, PN_MAX + 1):
        params = {"pn": pn, "ps": PS}
        try:
            r = s.get(POPULAR_API, params=params, timeout=20)
        except requests.RequestException:
            continue

        # 跳过 404（不重试也不崩）
        if r.status_code == 404:
            continue

        try:
            payload = r.json()
        except Exception:
            continue

        # raw 落盘（每页）
        raw_name = f"{now_compact()}_pn{pn}.json"
        raw_path = os.path.join(RAW_DIR, raw_name)
        atomic_write_json(raw_path, payload)

        if payload.get("code") != 0:
            # API 异常：跳到下一页
            continue

        data = payload.get("data") or {}
        lst = data.get("list") or []
        if not lst:
            # README: data.list 为空立即停止分页
            break

        for item in lst:
            rec = parse_popular_item(item, capture_ts)
            if not rec:
                continue
            bvid = rec["bvid"]
            if bvid in existing_map:
                existing_map[bvid] = merge_video(existing_map[bvid], rec)
                merged_cnt += 1
            else:
                existing_map[bvid] = rec
                new_cnt += 1

    # 写回 daily
    daily["videos"] = list(existing_map.values())
    recompute_daily_stats(daily)

    daily_path = os.path.join(DAILY_DIR, f"{day}.json")
    atomic_write_json(daily_path, daily)

    print(f"[collect_popular] day={day} new={new_cnt} merged={merged_cnt} total={daily['count']}")
    print(f"[collect_popular] wrote: {daily_path}")


if __name__ == "__main__":
    main()