# -*- coding: utf-8 -*-
"""
Week2 Step2 (Negative candidates) - dynamic/region
目标：完全“模仿 popular 的结构”，同时满足你新增的 daily 单文件按时间戳归档。

保留：
- data/raw/Neg/                 每页 raw 原始响应（不覆盖）
- data/runs/Neg/                每次运行 processed 文件（不合并、不覆盖）

新增：
- data/agg/region_history.json
  按 bvid 聚合历史：videos_by_bvid[bvid].captures[<capture_ts_key>] = 本次抓到的快照/特征

- data/daily/Neg/<YYYY-MM-DD>.json
  ✅ daily 文件夹只存 1 个文件（每天一个），并按时间戳分类：
  captures[<capture_ts_key>] = 本次 run 的概要 + videos（列表）

API:
  https://api.bilibili.com/x/web-interface/dynamic/region
  params: rid, pn, ps

用法：
  python v2_region_negative_run_crawler.py

可改参数：
  RID_LIST, PN_MAX, PS, OBS_HOURS
"""

from __future__ import annotations

import json
import os
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ================== 配置 ==================
API_DYNAMIC_REGION = "https://api.bilibili.com/x/web-interface/dynamic/region"
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36"

DATA_DIR = "data"
RAW_DIR = os.path.join(DATA_DIR, "raw", "Neg")          # 每页 raw（不覆盖）
RUN_DIR = os.path.join(DATA_DIR, "runs", "Neg")         # 每次运行 processed（不合并、不覆盖）
DAILY_DIR = os.path.join(DATA_DIR, "daily", "Neg")      # 每天一个文件，按时间戳分类（新增）
AGG_DIR = os.path.join(DATA_DIR, "agg")
AGG_PATH = os.path.join(AGG_DIR, "region_history.json")

TIMEOUT = 15
SLEEP_BETWEEN_PAGES = 0.2

# 默认抓一些主分区（可自行加 rid）
RID_LIST: List[int] = [
    1,    # 动画
    13,   # 番剧
    167,  # 国创
    3,    # 音乐
    129,  # 舞蹈
    4,    # 游戏
    36,   # 知识
    188,  # 科技
    160,  # 生活
    211,  # 美食
    217,  # 动物圈
    119,  # 鬼畜
    155,  # 时尚
    202,  # 资讯
    5,    # 娱乐
    181,  # 影视
    177,  # 纪录片
    23,   # 电影
    11,   # 电视剧
]

PN_MAX = 10
PS = 50

# 观察窗口（负样本候选 covered_until = pubdate + OBS_HOURS）
OBS_HOURS = 48


# ================== 工具函数 ==================
def ensure_dirs() -> None:
    os.makedirs(RAW_DIR, exist_ok=True)
    os.makedirs(RUN_DIR, exist_ok=True)
    os.makedirs(DAILY_DIR, exist_ok=True)
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

def _unique_capture_key(capture_ts: int, run_id: str, existing: Dict[str, Any]) -> str:
    """
    默认用 str(capture_ts) 作为 key。
    若同一秒重复运行导致冲突，则自动加后缀："<ts>__<run_id>" 或再追加序号。
    """
    base = str(capture_ts)
    if base not in existing:
        return base
    k = f"{base}__{run_id}"
    if k not in existing:
        return k
    i = 2
    while True:
        kk = f"{k}__{i}"
        if kk not in existing:
            return kk
        i += 1


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

def fetch_dynamic_region_page(s: requests.Session, rid: int, pn: int, ps: int) -> Dict[str, Any]:
    params = {"rid": rid, "pn": pn, "ps": ps}
    r = s.get(API_DYNAMIC_REGION, params=params, timeout=TIMEOUT)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, dict):
        raise RuntimeError(f"dynamic/region response not dict rid={rid} pn={pn} ps={ps}")
    # bilibili 有时返回 200 但 code != 0
    if data.get("code") != 0:
        raise RuntimeError(
            f"dynamic/region error rid={rid} pn={pn} ps={ps}: code={data.get('code')} msg={data.get('message')}"
        )
    return data


# ================== 规范化 ==================
def normalize_dynamic_archives(
    archives: List[Dict[str, Any]],
    capture_ts: int,
    request_rid: int
) -> List[Dict[str, Any]]:
    """
    转成与你 popular 脚本同风格的 record（label=0），并加 capture_ts。
    额外：写入 request_rid（你这次调用 dynamic/region 的 rid），避免和 tid 混淆。
    """
    out: List[Dict[str, Any]] = []

    for a in archives or []:
        stat = a.get("stat") or {}
        owner = a.get("owner") or {}

        bvid = a.get("bvid")
        if not bvid:
            continue

        pub_ts = a.get("pubdate")
        age_hours: Optional[float] = None
        if isinstance(pub_ts, int) and pub_ts > 0:
            age_hours = (capture_ts - pub_ts) / 3600.0

        view = stat.get("view", 0)

        features_as_of_capture = {
            "like_rate": safe_div(stat.get("like"), view),
            "coin_rate": safe_div(stat.get("coin"), view),
            "favorite_rate": safe_div(stat.get("favorite"), view),
            "view_per_hour": safe_div(view, age_hours) if age_hours and age_hours > 0 else None,
            "age_hours": age_hours,
        }

        covered_until_ts: Optional[int] = None
        if isinstance(pub_ts, int) and pub_ts > 0:
            covered_until_ts = pub_ts + int(OBS_HOURS * 3600)

        out.append(
            {
                "bvid": bvid,
                "aid": a.get("aid"),
                "label": 0,
                "label_source": "dynamic_region",

                # ✅ 本条视频抓取时间
                "capture_ts": capture_ts,

                "title": a.get("title"),
                "url": f"https://www.bilibili.com/video/{bvid}",

                # ✅ 两套：request_rid（接口参数） + tid/tname（视频自身分区）
                "request_rid": request_rid,
                "tid": a.get("tid"),
                "tname": a.get("tname"),

                "pubdate": pub_ts,
                "duration": a.get("duration"),

                # 负样本候选窗口
                "covered_until": covered_until_ts,

                # 追溯
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

    return out


# ================== 统计（与你 popular 一致） ==================
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


# ================== 聚合：按 bvid，把不同时间抓到的数据挂到 captures 下（历史总文件） ==================
def update_aggregate_history(run_doc: Dict[str, Any], agg_path: str = AGG_PATH) -> Dict[str, Any]:
    agg = load_json_if_exists(agg_path) or {
        "source": "bilibili_dynamic_region",
        "created_ts": run_doc.get("capture_ts"),
        "last_update_ts": run_doc.get("capture_ts"),
        "videos_by_bvid": {},
        "meta": {
            "video_unique_count": 0,
            "capture_record_count": 0,
        },
    }

    agg["last_update_ts"] = run_doc.get("capture_ts")
    videos_by_bvid: Dict[str, Any] = agg.get("videos_by_bvid") or {}
    run_id = run_doc.get("run_id") or "unknown_run"
    capture_ts = int(run_doc.get("capture_ts") or now_ts())

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
                "label": v.get("label"),  # 基本是 0
                "label_source": v.get("label_source"),
                "covered_until_latest": v.get("covered_until"),
                "title": v.get("title"),
                "url": v.get("url"),
                "tid": v.get("tid"),
                "tname": v.get("tname"),
                "pubdate": v.get("pubdate"),
                "up": v.get("up"),
                "first_seen_ts": v.get("first_seen_ts"),
                "captures": {},
            }

        # 固定字段：以最新覆盖
        node["title"] = v.get("title") or node.get("title")
        node["url"] = v.get("url") or node.get("url")
        node["tid"] = v.get("tid") if v.get("tid") is not None else node.get("tid")
        node["tname"] = v.get("tname") or node.get("tname")
        node["pubdate"] = v.get("pubdate") if v.get("pubdate") is not None else node.get("pubdate")
        node["up"] = v.get("up") or node.get("up")
        node["label_source"] = v.get("label_source") or node.get("label_source")

        # covered_until：取更晚的（更保守）
        cu_old = node.get("covered_until_latest")
        cu_new = v.get("covered_until")
        if isinstance(cu_old, int) and isinstance(cu_new, int):
            node["covered_until_latest"] = max(cu_old, cu_new)
        elif cu_old is None and isinstance(cu_new, int):
            node["covered_until_latest"] = cu_new

        # first_seen：取更早的
        if isinstance(node.get("first_seen_ts"), int) and isinstance(v.get("first_seen_ts"), int):
            node["first_seen_ts"] = min(node["first_seen_ts"], v["first_seen_ts"])
        elif node.get("first_seen_ts") is None and isinstance(v.get("first_seen_ts"), int):
            node["first_seen_ts"] = v["first_seen_ts"]

        captures: Dict[str, Any] = node.get("captures") or {}
        key = _unique_capture_key(int(v.get("capture_ts") or capture_ts), str(run_id), captures)

        captures[key] = {
            "ts": int(v.get("capture_ts") or capture_ts),
            "run_id": run_id,
            "request_rid": v.get("request_rid"),
            "covered_until": v.get("covered_until"),
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


# ================== daily：每天一个文件，按时间戳分类（模仿你 popular 的 captures 归档思路） ==================
def update_daily_file(run_doc: Dict[str, Any], daily_dir: str = DAILY_DIR) -> Dict[str, Any]:
    """
    daily/Neg/YYYY-MM-DD.json
      captures[<capture_ts_key>] = { run info + meta + category_stats + videos(list) }
    """
    date = run_doc.get("date") or today_str(int(run_doc.get("capture_ts") or now_ts()))
    daily_path = os.path.join(daily_dir, f"{date}.json")

    daily = load_json_if_exists(daily_path) or {
        "date": date,
        "source": "bilibili_dynamic_region",
        "captures": {},  # key: capture_ts or unique key
        "meta": {
            "capture_count": 0,
            "total_video_records": 0,
        },
        "last_update_ts": run_doc.get("capture_ts"),
    }

    captures: Dict[str, Any] = daily.get("captures") or {}
    run_id = run_doc.get("run_id") or "unknown_run"
    capture_ts = int(run_doc.get("capture_ts") or now_ts())
    key = _unique_capture_key(capture_ts, str(run_id), captures)

    # 把本次 run 的 processed 内容，完整放入 daily 的一个 capture 节点
    captures[key] = {
        "ts": capture_ts,
        "run_id": run_id,
        "runs": run_doc.get("runs", []),
        "meta": run_doc.get("meta", {}),
        "count": run_doc.get("count", 0),
        "category_stats": run_doc.get("category_stats", {}),
        "videos": run_doc.get("videos", []),
    }

    daily["captures"] = captures
    daily["last_update_ts"] = capture_ts
    daily["meta"]["capture_count"] = len(captures)

    # total_video_records = sum(每次 capture 的 count)
    total_records = 0
    for _, c in captures.items():
        if isinstance(c, dict) and isinstance(c.get("count"), int):
            total_records += int(c.get("count"))
    daily["meta"]["total_video_records"] = total_records

    save_json(daily_path, daily)
    return daily


# ================== 主流程：每次 run 独立 + 更新聚合总文件 + 更新 daily 单文件 ==================
def main() -> None:
    ensure_dirs()

    ts = now_ts()
    date = today_str(ts)
    run_id = datetime_compact(ts)

    run_path = os.path.join(
        RUN_DIR,
        f"{date}__run_{run_id}__rids{len(RID_LIST)}__pn{PN_MAX}__ps{PS}__obs{OBS_HOURS}h.json",
    )

    # ✅ processed run 结构：完全模仿 popular run 文件
    doc: Dict[str, Any] = {
        "date": date,
        "capture_ts": ts,
        "last_capture_ts": ts,
        "source": "bilibili_dynamic_region",
        "run_id": run_id,
        "runs": [
            {
                "ts": ts,
                "script": "v2_region_negative_run_crawler",
                "rid_list": RID_LIST,
                "pn_max": PN_MAX,
                "ps": PS,
                "obs_hours": OBS_HOURS,
            }
        ],
        "videos": [],
        "meta": {"pos_count": 0, "neg_count": 0, "total_count": 0},
        "count": 0,
        "category_stats": {},
    }

    pages_fetched = 0
    all_videos: List[Dict[str, Any]] = []

    s = build_session()
    try:
        for rid in RID_LIST:
            rid_collected = 0
            for pn in range(1, PN_MAX + 1):
                try:
                    raw = fetch_dynamic_region_page(s, rid=rid, pn=pn, ps=PS)
                except requests.HTTPError as e:
                    # 1️⃣ HTTP 层面的 404 / 5xx
                    status = getattr(e.response, "status_code", None)
                    if status == 404:
                        #print(f"[WARN] rid={rid} pn={pn} HTTP 404, skip this rid.")
                        break
                    #print(f"[WARN] rid={rid} pn={pn} HTTP error={status}, skip this page.")
                    continue
                except Exception as e:
                    # 2️⃣ code != 0 或其他异常
                    #print(f"[WARN] rid={rid} pn={pn} error: {e}, skip this rid.")
                    break

                # ✅ 每页 raw 单独保存（不覆盖）
                raw_path = os.path.join(RAW_DIR, f"{date}__run_{run_id}__rid{rid}__pn{pn}__ps{PS}.json")
                save_json(raw_path, raw)

                data = raw.get("data") or {}
                archives = data.get("archives") or []

                # ✅ 空则停止该 rid 的分页
                if not archives:
                    print(f"[INFO] rid={rid} pn={pn} empty, stop this rid.")
                    break

                # 规范化（把 request_rid 写进去）
                videos = normalize_dynamic_archives(archives, capture_ts=ts, request_rid=rid)
                all_videos.extend(videos)

                rid_collected += len(archives)
                pages_fetched += 1
                time.sleep(SLEEP_BETWEEN_PAGES)

            print(f"[INFO] rid={rid} pages_fetched~ collected_items={rid_collected}")
    finally:
        s.close()

    doc["videos"] = all_videos
    doc = recompute_run_fields(doc)

    # 1) 保存本次 run processed（不覆盖）
    save_json(run_path, doc)

    # 2) 更新历史聚合（按 bvid / captures）
    agg = update_aggregate_history(doc, AGG_PATH)

    # 3) 更新 daily 单文件（按 capture_ts 分类）
    daily = update_daily_file(doc, DAILY_DIR)

    print(
        f"[OK] date={date} run_id={run_id} rids={len(RID_LIST)} pages={pages_fetched} "
        f"videos={len(all_videos)} -> {run_path}"
    )
    print(f"[OK] agg -> {AGG_PATH} (unique_videos={agg['meta']['video_unique_count']})")
    print(f"[OK] daily -> {os.path.join(DAILY_DIR, date + '.json')} (capture_count={daily['meta']['capture_count']})")


if __name__ == "__main__":
    main()