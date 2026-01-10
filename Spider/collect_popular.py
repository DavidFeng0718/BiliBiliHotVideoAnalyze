# -*- coding: utf-8 -*-
"""
add_negatives.py — Week2: 负样本采集（label=0）

强制要求（来自 README）：
- API: https://api.bilibili.com/x/web-interface/newlist
- 参数：rid=tid, pn=1(固定), ps=每页数量
- 采样规则：
  - 按 tid 采样
  - 负样本发布时间≈正样本发布时间（工程上：先按同 tid 拉 newlist，再随机采样）
  - 不能出现在热门列表中（工程上：排除已存在 bvid，正样本已写入 daily）
  - 每个 tid：负样本 ≈ 正样本数量
- 实现要求：
  - 先读取当日 daily.json
  - 统计每个 tid 的正样本数量
  - 对每个 tid：拉 newlist → 排除已存在 bvid → 随机采样 N 条 → 写入 label=0、snapshots/features 空
- 输出：
  - 合并进同一个 data/daily/YYYY-MM-DD.json
  - 不覆盖正样本
  - 自动重算 count/meta/category_stats
"""

from __future__ import annotations

import argparse
import json
import os
import random
import time
from datetime import date
from typing import Any, Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


NEWLIST_API = "https://api.bilibili.com/x/web-interface/newlist"

DATA_DIR = "data"
DAILY_DIR = os.path.join(DATA_DIR, "daily")

PS = 50

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.bilibili.com/",
}


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


def index_by_bvid(videos: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for v in videos:
        bvid = v.get("bvid")
        if bvid:
            out[str(bvid)] = v
    return out


def count_pos_by_tid(videos: List[Dict[str, Any]]) -> Dict[int, int]:
    out: Dict[int, int] = {}
    for v in videos:
        if int(v.get("label", 0)) != 1:
            continue
        tid = v.get("tid")
        if tid is None:
            continue
        out[int(tid)] = out.get(int(tid), 0) + 1
    return out


def parse_newlist_items(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    newlist 返回结构在不同时间/版本可能略有差异，做容错解析：
    - 常见：data -> archives (list)
    - 或：data -> list (list)
    """
    data = payload.get("data") or {}
    if isinstance(data, dict):
        if isinstance(data.get("archives"), list):
            return data["archives"]
        if isinstance(data.get("list"), list):
            return data["list"]
    return []


def build_negative_record(item: Dict[str, Any], capture_ts: int) -> Optional[Dict[str, Any]]:
    bvid = item.get("bvid")
    if not bvid:
        return None

    aid = item.get("aid") or item.get("id") or 0
    tid = item.get("tid")
    tname = item.get("tname") or ""
    pubdate = item.get("pubdate") or item.get("ctime") or 0

    owner = item.get("owner") or {}
    up = {
        "mid": owner.get("mid") or item.get("mid") or 0,
        "name": owner.get("name") or item.get("author") or "",
        "follower": None,
    }

    return {
        "bvid": str(bvid),
        "aid": int(aid) if aid is not None else 0,
        "label": 0,
        "title": item.get("title") or "",
        "url": f"https://www.bilibili.com/video/{bvid}",
        "tid": int(tid) if tid is not None else None,
        "tname": tname,
        "pubdate": int(pubdate) if pubdate else 0,
        "first_seen_ts": int(capture_ts),
        "up": up,
        "snapshots": {},
        "features": {},
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("day", nargs="?", default=today_str(), help="YYYY-MM-DD (default: today)")
    ap.add_argument("--ps", type=int, default=PS, help="newlist page size (default: 50)")
    ap.add_argument("--seed", type=int, default=None, help="random seed (optional)")
    args = ap.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    day = args.day
    capture_ts = utc_ts()

    daily_path = os.path.join(DAILY_DIR, f"{day}.json")
    if not os.path.exists(daily_path):
        raise FileNotFoundError(f"daily file not found: {daily_path}")

    daily = read_json(daily_path)
    videos: List[Dict[str, Any]] = daily.get("videos", [])
    bvid_map = index_by_bvid(videos)

    pos_by_tid = count_pos_by_tid(videos)
    if not pos_by_tid:
        print("[add_negatives] no positive samples found; nothing to do.")
        return

    s = request_session()
    added = 0

    for tid, need in sorted(pos_by_tid.items(), key=lambda x: x[0]):
        if need <= 0:
            continue

        params = {"rid": tid, "pn": 1, "ps": int(args.ps)}
        try:
            r = s.get(NEWLIST_API, params=params, timeout=20)
        except requests.RequestException:
            continue

        if r.status_code == 404:
            # 跳过 404 / 不支持分区
            continue

        try:
            payload = r.json()
        except Exception:
            continue

        if payload.get("code") != 0:
            continue

        items = parse_newlist_items(payload)
        if not items:
            continue

        # 过滤掉已存在 bvid（含热门正样本）
        candidates: List[Dict[str, Any]] = []
        for it in items:
            bv = it.get("bvid")
            if not bv:
                continue
            if str(bv) in bvid_map:
                continue
            candidates.append(it)

        if not candidates:
            continue

        # 随机采样 need 条（不足则全取）
        k = min(need, len(candidates))
        picked = random.sample(candidates, k=k)

        for it in picked:
            rec = build_negative_record(it, capture_ts)
            if not rec:
                continue
            bv = rec["bvid"]
            if bv in bvid_map:
                continue
            bvid_map[bv] = rec
            added += 1

        # 简单限速（避免被限流）
        time.sleep(0.2)

    daily["capture_ts"] = capture_ts
    daily["videos"] = list(bvid_map.values())
    recompute_daily_stats(daily)

    atomic_write_json(daily_path, daily)
    print(f"[add_negatives] day={day} added={added} total={daily['count']}")
    print(f"[add_negatives] wrote: {daily_path}")


if __name__ == "__main__":
    main()