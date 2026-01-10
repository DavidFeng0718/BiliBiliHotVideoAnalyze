# -*- coding: utf-8 -*-
"""
JSON-only Week2 (No-merge per-run) + Aggregate history by bvid

保留：
- data/raw/popular/           每页 raw 原始响应（不覆盖）
- data/runs/popular/          每次运行 processed 文件（不合并、不覆盖）

新增：
- data/agg/popular_history.json
  把历史所有 run 的视频数据聚合到同一个文件里，但按 bvid 归档：
  videos[bvid].captures[<capture_ts>] = 本次抓到的快照/特征/关键信息

用法：
  python popular_run_crawler.py

可改参数：
  PN_MAX = 100
  PS = 50
"""

import json
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
RUN_DIR = os.path.join(DATA_DIR, "daily", "Pos")     # 每次运行的 processed 输出目录
AGG_DIR = os.path.join(DATA_DIR, "agg")                 # 聚合输出目录（新）
AGG_PATH = os.path.join(AGG_DIR, "popular_history.json")

TIMEOUT = 30
PN_MAX = 100
PS = 50
SLEEP_BETWEEN_PAGES = 0.2


# ================== 工具函数 ==================
def ensure_dirs() -> None:
    os.makedirs(RAW_DIR, exist_ok=True)
    os.makedirs(RUN_DIR, exist_ok=True)
    os.makedirs(AGG_DIR, exist_ok=True)

def now_ts() -> int:
    return int(time.time())

def today_str(ts: int) -> str:
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d")

def datetime_compact(ts: int) -> str:
    # 带毫秒，避免同秒覆盖
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%dT%H-%M-%S") + f".{int((time.time() % 1) * 1000):03d}"

def safe_div(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is None or b in (None, 0):
        return None
    return a / b

def load_json_if_exists(path: str) -> Optional[Dict[str, Any]]:
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return None

def save_json(path: str, obj: Dict[str, Any]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


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


# ================== 规范化 ==================
def normalize_popular_items(items: List[Dict[str, Any]], capture_ts: int) -> List[Dict[str, Any]]:
    """
    输出结构尽量沿用你旧脚本字段：
    - 增加：每个视频显式带 capture_ts（方便你后续理解“时间戳维度”）
    - snapshots/features 仍保留 as_of_capture + 0h 兼容键
    """
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

                "capture_ts": capture_ts,  # ✅ 本条视频的抓取时间戳（秒）
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
                    "0h": {  # 兼容旧字段名（不要当“发布后0小时”理解）
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
                    "0h": features_as_of_capture,
                },
            }
        )
    return videos


# ================== 统计（与你旧脚本一致） ==================
def recompute_run_fields(doc: Dict[str, Any]) -> Dict[str, Any]:
    videos = doc.get("videos", []) or []

    doc["count"] = len(videos)

    doc.setdefault("meta", {})
    doc["meta"]["pos_count"] = len([v for v in videos if v.get("label") == 1])
    doc["meta"]["neg_count"] = len([v for v in videos if v.get("label") == 0])
    doc["meta"]["total_count"] = len(videos)

    cat: Dict[str, Dict[str, Any]] = {}
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

    out: Dict[str, Dict[str, Any]] = {}
    for tid, s in cat.items():
        out[tid] = {
            "tname": s["tname"],
            "video_count": s["video_count"],
            "avg_view_0h": (s["view_sum"] / s["view_cnt"]) if s["view_cnt"] > 0 else None,
            "avg_like_rate_0h": (s["like_rate_sum"] / s["like_rate_cnt"]) if s["like_rate_cnt"] > 0 else None,
        }

    doc["category_stats"] = out
    return doc


# ================== 聚合：把历史不同文件的数据挂到每个视频下面 ==================
def _unique_capture_key(capture_ts: int, run_id: str, existing_captures: Dict[str, Any]) -> str:
    """
    默认用 str(capture_ts) 作为 key。
    若同一秒重复运行导致冲突，则自动加后缀："<ts>__<run_id>" 或再追加序号。
    """
    base = str(capture_ts)
    if base not in existing_captures:
        return base
    k = f"{base}__{run_id}"
    if k not in existing_captures:
        return k
    i = 2
    while True:
        kk = f"{k}__{i}"
        if kk not in existing_captures:
            return kk
        i += 1


def update_aggregate_history(run_doc: Dict[str, Any], agg_path: str = AGG_PATH) -> Dict[str, Any]:
    """
    把本次 run_doc 的 videos 合并进聚合总文件：
    - 按 bvid 聚合
    - 每条抓取记录追加到 videos[bvid].captures[<timestamp_key>] 下
    """
    agg = load_json_if_exists(agg_path) or {
        "source": "bilibili_popular",
        "created_ts": run_doc.get("capture_ts"),
        "last_update_ts": run_doc.get("capture_ts"),
        # 用 dict 方便查找：videos_by_bvid[bvid] = {...}
        "videos_by_bvid": {},
        "meta": {
            "video_unique_count": 0,
            "capture_record_count": 0,
        },
    }

    agg["last_update_ts"] = run_doc.get("capture_ts")
    videos_by_bvid: Dict[str, Any] = agg.get("videos_by_bvid") or {}
    run_id = run_doc.get("run_id") or "unknown_run"
    capture_ts = run_doc.get("capture_ts")

    added_records = 0
    for v in run_doc.get("videos", []) or []:
        bvid = v.get("bvid")
        if not bvid:
            continue

        node = videos_by_bvid.get(bvid)
        if not node:
            node = {
                "bvid": bvid,
                "aid": v.get("aid"),
                "label": v.get("label"),
                # 固定信息（可以随最新覆盖）
                "title": v.get("title"),
                "url": v.get("url"),
                "tid": v.get("tid"),
                "tname": v.get("tname"),
                "pubdate": v.get("pubdate"),
                "up": v.get("up"),
                "first_seen_ts": v.get("first_seen_ts"),
                # ✅ 核心：不同时间戳下的数据都放在这里
                "captures": {},  # key: capture_ts 或带 run_id 的唯一 key
            }

        # 固定字段：以“最新一次”覆盖（你也可以改成只在 None 时写）
        node["title"] = v.get("title") or node.get("title")
        node["url"] = v.get("url") or node.get("url")
        node["tid"] = v.get("tid") if v.get("tid") is not None else node.get("tid")
        node["tname"] = v.get("tname") or node.get("tname")
        node["pubdate"] = v.get("pubdate") if v.get("pubdate") is not None else node.get("pubdate")
        node["up"] = v.get("up") or node.get("up")
        if isinstance(node.get("first_seen_ts"), int) and isinstance(v.get("first_seen_ts"), int):
            node["first_seen_ts"] = min(node["first_seen_ts"], v["first_seen_ts"])
        elif node.get("first_seen_ts") is None and isinstance(v.get("first_seen_ts"), int):
            node["first_seen_ts"] = v["first_seen_ts"]

        captures: Dict[str, Any] = node.get("captures") or {}
        key = _unique_capture_key(int(v.get("capture_ts") or capture_ts), str(run_id), captures)

        # 只把“会变化的东西”放到 captures 里（你要求的“放在每一个视频下面”）
        captures[key] = {
            "ts": int(v.get("capture_ts") or capture_ts),
            "run_id": run_id,
            "snapshots": v.get("snapshots"),
            "features": v.get("features"),
        }

        node["captures"] = captures
        videos_by_bvid[bvid] = node
        added_records += 1

    agg["videos_by_bvid"] = videos_by_bvid
    agg["meta"]["video_unique_count"] = len(videos_by_bvid)
    agg["meta"]["capture_record_count"] = agg["meta"].get("capture_record_count", 0) + added_records

    save_json(agg_path, agg)
    return agg


# ================== 主入口（每次 run 独立 + 更新聚合文件） ==================
def main(pn_max: int = PN_MAX, ps: int = PS) -> None:
    ensure_dirs()

    ts = now_ts()
    date = today_str(ts)
    run_id = datetime_compact(ts)

    run_path = os.path.join(RUN_DIR, f"{date}__run_{run_id}__pn{pn_max}__ps{ps}.json")

    doc: Dict[str, Any] = {
        "date": date,
        "capture_ts": ts,
        "last_capture_ts": ts,
        "source": "bilibili_popular",
        "run_id": run_id,
        "runs": [{"ts": ts, "pn_max": pn_max, "ps": ps}],
        "videos": [],
        "meta": {"pos_count": 0, "neg_count": 0, "total_count": 0},
        "count": 0,
        "category_stats": {},
    }

    all_items: List[Dict[str, Any]] = []
    pages_fetched = 0

    s = build_session()
    try:
        for pn in range(1, pn_max + 1):
            raw = fetch_popular_page(s, pn=pn, ps=ps)
            items = raw.get("data", {}).get("list", [])

            if not items:
                print(f"[INFO] pn={pn} returned empty list, stop pagination.")
                break

            raw_path = os.path.join(RAW_DIR, f"{date}__run_{run_id}__pn{pn}__ps{ps}.json")
            save_json(raw_path, raw)

            all_items.extend(items)
            pages_fetched += 1
            time.sleep(SLEEP_BETWEEN_PAGES)
    finally:
        s.close()

    videos = normalize_popular_items(all_items, ts)

    doc["videos"] = videos
    doc = recompute_run_fields(doc)

    save_json(run_path, doc)

    # ✅ 增量更新聚合总文件：把本次 run 的数据挂到每个 bvid 的 captures 下
    agg = update_aggregate_history(doc, AGG_PATH)

    print(
        f"[OK] date={date} run_id={run_id} pages={pages_fetched} "
        f"items={len(all_items)} videos={len(videos)} -> {run_path}"
    )
    print(f"[OK] agg -> {AGG_PATH} (unique_videos={agg['meta']['video_unique_count']})")


if __name__ == "__main__":
    main()