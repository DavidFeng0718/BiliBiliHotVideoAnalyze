# -*- coding: utf-8 -*-
"""
JSON-only Week2 - Step1: 抓热门(popular) 多页 + 去重 + 合并写入当日 daily.json
- 原始响应每页备份到 data/raw/popular/
- 规范化正样本合并到 data/daily/YYYY-MM-DD.json（断点续跑）

重要约定（修复若干逻辑坑）：
- daily.capture_ts 仅记录“当天首次抓取时间”，后续重复运行不会覆盖
- daily.last_capture_ts 记录“最近一次运行的抓取时间”，daily.runs 记录每次运行
- snapshots/features 的键名使用 as_of_capture（为兼容仍保留 0h 同步写入）
- 合并时以 bvid 为唯一主键；若同一 bvid 同时出现 label=0/1，强制以 label=1 为准

用法：
  python v1.py
可改参数：
  PN_MAX = 100
  PS = 50
"""

import json
import copy
import os
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ================== 配置 ==================
POPULAR_API = "https://api.bilibili.com/x/web-interface/popular"
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36"

DATA_DIR = "data"
RAW_DIR = os.path.join(DATA_DIR, "raw", "popular")
DAILY_DIR = os.path.join(DATA_DIR, "daily")

TIMEOUT = 30
PN_MAX = 100
PS = 50
SLEEP_BETWEEN_PAGES = 0.2

# ================== 工具函数 ==================
def ensure_dirs() -> None:
    os.makedirs(RAW_DIR, exist_ok=True)
    os.makedirs(DAILY_DIR, exist_ok=True)

def now_ts() -> int:
    return int(time.time())

def today_str(ts: int) -> str:
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d")

def datetime_compact(ts: int) -> str:
    # 带毫秒，降低同秒重复运行导致 raw 覆盖的风险
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%dT%H-%M-%S") + f".{int((time.time() % 1) * 1000):03d}"

def safe_div(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is None or b in (None, 0):
        return None
    return a / b

# ================== 网络 ==================
def build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": UA})
    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s

def fetch_popular_page(session: requests.Session, pn: int = 1, ps: int = 20) -> Dict[str, Any]:
    resp = session.get(POPULAR_API, params={"pn": pn, "ps": ps}, timeout=TIMEOUT)
    data = resp.json()
    if not isinstance(data, dict):
        raise RuntimeError(f"popular response not dict (pn={pn}, ps={ps})")
    if data.get("code") != 0:
        raise RuntimeError(f"popular error pn={pn} ps={ps}: code={data.get('code')} msg={data.get('message')}")
    return data

# ================== 规范化/合并 ==================
def normalize_popular_items(items: List[Dict[str, Any]], capture_ts: int) -> List[Dict[str, Any]]:
    videos: List[Dict[str, Any]] = []
    for item in items:
        stat = item.get("stat") or {}
        owner = item.get("owner") or {}

        pubdate = item.get("pubdate")
        age_hours = (capture_ts - pubdate) / 3600 if pubdate else None

        view = stat.get("view", 0)

        features_as_of_capture = {
            "like_rate": safe_div(stat.get("like"), view),
            "coin_rate": safe_div(stat.get("coin"), view),
            "favorite_rate": safe_div(stat.get("favorite"), view),
            "view_per_hour": safe_div(view, age_hours) if age_hours and age_hours > 0 else None,
            "age_hours": age_hours,
        }

        bvid = item.get("bvid")
        videos.append(
            {
                "bvid": bvid,
                "aid": item.get("aid"),
                "label": 1,

                "title": item.get("title"),
                "url": item.get("short_link_v2") or (f"https://b23.tv/{bvid}" if bvid else None),

                "tid": item.get("tid"),
                "tname": item.get("tname"),
                "pubdate": pubdate,
                "first_seen_ts": capture_ts,

                "up": {
                    "mid": owner.get("mid"),
                    "name": owner.get("name"),
                    "follower": owner.get("follower"),
                },

                # 快照（抓取这一刻）
                "snapshots": {
                    "as_of_capture": {
                        "ts": capture_ts,
                        "view": stat.get("view"),
                        "like": stat.get("like"),
                        "coin": stat.get("coin"),
                        "favorite": stat.get("favorite"),
                        "reply": stat.get("reply"),
                        "danmaku": stat.get("danmaku"),
                        "share": stat.get("share"),
                    },
                    # 兼容旧字段名（不要再把它理解成“发布后0小时”）
                    "0h": {
                        "ts": capture_ts,
                        "view": stat.get("view"),
                        "like": stat.get("like"),
                        "coin": stat.get("coin"),
                        "favorite": stat.get("favorite"),
                        "reply": stat.get("reply"),
                        "danmaku": stat.get("danmaku"),
                        "share": stat.get("share"),
                    },
                },
                "features": {
                    "as_of_capture": features_as_of_capture,
                    # 兼容旧字段名
                    "0h": features_as_of_capture,
                },
            }
        )
    return videos

def dedup_by_bvid(videos: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    out = []
    for v in videos:
        bvid = v.get("bvid")
        if not bvid or bvid in seen:
            continue
        seen.add(bvid)
        out.append(v)
    return out

def load_daily_if_exists(path: str) -> Dict[str, Any]:
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def _deep_merge_dict(dst: Dict[str, Any], src: Dict[str, Any]) -> Dict[str, Any]:
    """浅层优先合并：dict 递归合并；非 dict 以 src 覆盖 dst。"""
    for k, v in (src or {}).items():
        if isinstance(v, dict) and isinstance(dst.get(k), dict):
            _deep_merge_dict(dst[k], v)
        else:
            dst[k] = v
    return dst


def merge_videos_by_bvid(existing: List[Dict[str, Any]], incoming: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """以 bvid 为唯一主键合并。

    规则：
    - 同 bvid 冲突时：label=1 覆盖 label=0
    - first_seen_ts 取更早的
    - snapshots/features 做字典合并（尽量不丢历史）
    """
    out: Dict[str, Dict[str, Any]] = {}

    def put(v: Dict[str, Any]) -> None:
        bvid = v.get("bvid")
        if not bvid:
            return
        if bvid not in out:
            out[bvid] = copy.deepcopy(v)
            return

        cur = out[bvid]
        cur_label = cur.get("label")
        new_label = v.get("label")

        # label 优先级：1 > 0 > 其他/None
        def label_rank(x: Any) -> int:
            return 2 if x == 1 else (1 if x == 0 else 0)

        if label_rank(new_label) >= label_rank(cur_label):
            # 用新记录覆盖基础字段（保留快照/特征合并）
            base = copy.deepcopy(v)
            # 先拿当前快照/特征
            base_snap = cur.get("snapshots") or {}
            base_feat = cur.get("features") or {}
            base.setdefault("snapshots", {})
            base.setdefault("features", {})
            _deep_merge_dict(base["snapshots"], base_snap)
            _deep_merge_dict(base["features"], base_feat)
            cur = base

        # first_seen_ts：取更早
        a = cur.get("first_seen_ts")
        b = v.get("first_seen_ts")
        if isinstance(a, int) and isinstance(b, int):
            cur["first_seen_ts"] = min(a, b)
        elif a is None and isinstance(b, int):
            cur["first_seen_ts"] = b

        # snapshots/features：合并（新覆盖旧同键）
        cur.setdefault("snapshots", {})
        cur.setdefault("features", {})
        _deep_merge_dict(cur["snapshots"], v.get("snapshots") or {})
        _deep_merge_dict(cur["features"], v.get("features") or {})

        # label：确保正样本优先
        if cur.get("label") != 1 and v.get("label") == 1:
            cur["label"] = 1

        out[bvid] = cur

    for v in existing or []:
        put(v)
    for v in incoming or []:
        put(v)

    return list(out.values())


def merge_daily_pos(existing_daily: Dict[str, Any], new_pos: List[Dict[str, Any]]) -> Dict[str, Any]:
    old_videos = existing_daily.get("videos", []) or []
    merged_all = merge_videos_by_bvid(old_videos, new_pos)

    existing_daily.setdefault("meta", {})
    existing_daily["videos"] = merged_all
    existing_daily["meta"]["pos_count"] = len([v for v in merged_all if v.get("label") == 1])
    existing_daily["meta"]["neg_count"] = len([v for v in merged_all if v.get("label") == 0])
    existing_daily["meta"]["total_count"] = len(merged_all)
    return existing_daily

def recompute_daily_fields(daily: Dict[str, Any]) -> Dict[str, Any]:
    videos = daily.get("videos", []) or []

    # 1) count
    daily["count"] = len(videos)

    # 2) meta（可一起兜底）
    daily.setdefault("meta", {})
    daily["meta"]["pos_count"] = len([v for v in videos if v.get("label") == 1])
    daily["meta"]["neg_count"] = len([v for v in videos if v.get("label") == 0])
    daily["meta"]["total_count"] = len(videos)

    # 3) category_stats：按 tid 聚合（只统计正样本/或全部都行，这里用全部）
    cat = {}
    for v in videos:
        tid = v.get("tid")
        if tid is None:
            continue
        tid = str(tid)
        tname = v.get("tname")

        snaps = (v.get("snapshots") or {})
        snap = snaps.get("as_of_capture") or snaps.get("0h") or {}
        view_as_of = snap.get("view")

        feats = (v.get("features") or {})
        feat = feats.get("as_of_capture") or feats.get("0h") or {}
        like_rate_as_of = feat.get("like_rate")

        if tid not in cat:
            cat[tid] = {
                "tname": tname,
                "video_count": 0,
                "view_sum": 0,
                "view_cnt": 0,
                "like_rate_sum": 0.0,
                "like_rate_cnt": 0,
            }

        cat[tid]["video_count"] += 1

        if isinstance(view_as_of, (int, float)):
            cat[tid]["view_sum"] += view_as_of
            cat[tid]["view_cnt"] += 1

        if isinstance(like_rate_as_of, (int, float)):
            cat[tid]["like_rate_sum"] += like_rate_as_of
            cat[tid]["like_rate_cnt"] += 1

    # 输出时把中间变量变成最终统计
    out = {}
    for tid, s in cat.items():
        out[tid] = {
            "tname": s["tname"],
            "video_count": s["video_count"],
            "avg_view_0h": (s["view_sum"] / s["view_cnt"]) if s["view_cnt"] > 0 else None,
            "avg_like_rate_0h": (s["like_rate_sum"] / s["like_rate_cnt"]) if s["like_rate_cnt"] > 0 else None,
        }

    daily["category_stats"] = out
    return daily
# ================== 主入口 ==================
def main(pn_max: int = PN_MAX, ps: int = PS) -> None:
    ensure_dirs()
    ts = now_ts()
    date = today_str(ts)
    daily_path = os.path.join(DAILY_DIR, f"{date}.json")

    daily = load_daily_if_exists(daily_path) or {
        "date": date,
        "capture_ts": ts,
        "source": "bilibili_popular",
        "videos": [],
        "meta": {"pos_count": 0, "neg_count": 0},
    }
    daily["date"] = date
    daily.setdefault("capture_ts", ts)  # 当天首次抓取时间（不覆盖）
    daily["last_capture_ts"] = ts       # 最近一次运行时间（覆盖）
    daily.setdefault("runs", [])
    daily["runs"].append({"ts": ts, "pn_max": pn_max, "ps": ps})

    all_items: List[Dict[str, Any]] = []

    s = build_session()
    try:
        for pn in range(1, pn_max + 1):
            raw = fetch_popular_page(s, pn=pn, ps=ps)

            items = raw.get("data", {}).get("list", [])

            # ⭐ 关键防御：如果本页已经没有数据，直接停止分页
            if not items:
                print(f"[INFO] pn={pn} returned empty list, stop pagination.")
                break

            # 只有在有数据时才备份 raw
            raw_path = os.path.join(RAW_DIR, f"{datetime_compact(ts)}_pn{pn}_ps{ps}.json")
            with open(raw_path, "w", encoding="utf-8") as f:
                json.dump(raw, f, ensure_ascii=False, indent=2)

            all_items.extend(items)
            time.sleep(SLEEP_BETWEEN_PAGES)
    finally:
        s.close()

    new_pos = normalize_popular_items(all_items, ts)
    new_pos = dedup_by_bvid(new_pos)

    daily = merge_daily_pos(daily, new_pos)
    daily = recompute_daily_fields(daily)
    with open(daily_path, "w", encoding="utf-8") as f:
        json.dump(daily, f, ensure_ascii=False, indent=2)

    print(f"[OK] date={date} pn_max={pn_max} ps={ps} items={len(all_items)} pos_added={len(new_pos)} merged_total={daily.get('meta', {}).get('total_count')}")
    print(f"[OK] daily -> {daily_path}")

if __name__ == "__main__":
    main()