# -*- coding: utf-8 -*-
"""
Week2 Step2 (Negative candidates) - dynamic/region

改造目标（与 Step1 同步）：
- 每页 raw 原始响应单独保存（不覆盖）
- 每次运行生成一个 processed run JSON（不与历史合并、不覆盖）
- 额外维护一个聚合总文件：按 bvid 聚合，在每个视频下面用 captures[<timestamp>] 存每次抓取的快照/特征
- 每条视频 record 增加 capture_ts

API:
  https://api.bilibili.com/x/web-interface/dynamic/region

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
from typing import Any, Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ================== 配置 ==================
API_DYNAMIC_REGION = "https://api.bilibili.com/x/web-interface/dynamic/region"
UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/138.0.0.0 Safari/537.36"
)

DATA_DIR = "data"

# ✅ 目录语义修正：
# - raw：每页原始响应
# - daily/run：每次运行的 processed run 输出
RAW_DIR = os.path.join(DATA_DIR, "raw", "Neg")
RUN_DIR = os.path.join(DATA_DIR, "daily", "Neg")

AGG_DIR = os.path.join(DATA_DIR, "agg")
AGG_PATH = os.path.join(AGG_DIR, "region_history.json")

TIMEOUT = 15
SLEEP_BETWEEN_PAGES = 0.2

# 默认抓一些主分区（可自行加 tid/rid）
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

# 观察窗口（负样本候选 covered_until）
OBS_HOURS = 48


# ================== 工具函数 ==================
def ensure_dirs() -> None:
    os.makedirs(RAW_DIR, exist_ok=True)
    os.makedirs(RUN_DIR, exist_ok=True)
    os.makedirs(AGG_DIR, exist_ok=True)

def now_ts_float() -> float:
    return time.time()

def today_str(ts_sec: int) -> str:
    return datetime.fromtimestamp(ts_sec).strftime("%Y-%m-%d")

def datetime_compact_from_float(ts: float) -> str:
    """
    生成更稳定的 run_id：精确到毫秒
    例：2026-01-09T18-12-33.123
    """
    base = datetime.fromtimestamp(ts).strftime("%Y-%m-%dT%H-%M-%S")
    ms = int((ts - int(ts)) * 1000)
    return f"{base}.{ms:03d}"

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

def fetch_dynamic_region_page(s: requests.Session, rid: int, pn: int, ps: int) -> Dict[str, Any]:
    params = {"rid": rid, "pn": pn, "ps": ps}
    r = s.get(API_DYNAMIC_REGION, params=params, timeout=TIMEOUT)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, dict):
        raise RuntimeError(f"dynamic/region response not dict rid={rid} pn={pn} ps={ps}")
    if data.get("code") != 0:
        raise RuntimeError(
            f"dynamic/region error rid={rid} pn={pn} ps={ps}: "
            f"code={data.get('code')} msg={data.get('message')}"
        )
    return data


# ================== 规范化 ==================
def normalize_dynamic_archives(
    archives: List[Dict[str, Any]],
    capture_ts: int,
) -> List[Dict[str, Any]]:
    """
    转成与你 v1/v2 同结构的 record（label=0），并加 capture_ts。
    同时保留 query_rid（本次请求使用的 rid），避免把 tid 当 rid 的语义误用。
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

        # ✅ 本页请求使用的 rid（由主流程注入到 archive["_query_rid"]）
        query_rid = a.get("_query_rid")

        out.append(
            {
                "bvid": bvid,
                "aid": a.get("aid"),
                "title": a.get("title"),
                "url": f"https://www.bilibili.com/video/{bvid}",
                "tid": a.get("tid"),
                "tname": a.get("tname"),
                "pubdate": pub_ts,
                "duration": a.get("duration"),

                # ✅ 本条视频抓取时间
                "capture_ts": capture_ts,

                # ✅ 本次分页请求参数 rid（避免把 tid 当 rid）
                "query_rid": query_rid,

                # 负样本候选
                "label": 0,
                "label_source": "dynamic_region",
                "covered_until": covered_until_ts,

                # 追溯
                "first_seen_ts": capture_ts,

                "up": {
                    "mid": owner.get("mid"),
                    "name": owner.get("name"),
                    # 接口可能没有 follower 字段，保留为可选
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
                    # 兼容旧字段名：0h == 本次抓取时刻
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
                    "0h": features_as_of_capture,  # 兼容旧字段名
                },
            }
        )

    return out


# ================== 统计（与 v1 一致 + 额外新增更准确字段名） ==================
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
        tid_str = str(tid)
        tname = v.get("tname")

        snaps = (v.get("snapshots") or {})
        snap = snaps.get("as_of_capture") or snaps.get("0h") or {}
        view_as_of = snap.get("view")

        feats = (v.get("features") or {})
        feat = feats.get("as_of_capture") or feats.get("0h") or {}
        like_rate_as_of = feat.get("like_rate")

        if tid_str not in cat:
            cat[tid_str] = {
                "tname": tname,
                "video_count": 0,
                "view_sum": 0.0,
                "view_cnt": 0,
                "like_rate_sum": 0.0,
                "like_rate_cnt": 0,
            }

        cat[tid_str]["video_count"] += 1

        if isinstance(view_as_of, (int, float)):
            cat[tid_str]["view_sum"] += float(view_as_of)
            cat[tid_str]["view_cnt"] += 1

        if isinstance(like_rate_as_of, (int, float)):
            cat[tid_str]["like_rate_sum"] += float(like_rate_as_of)
            cat[tid_str]["like_rate_cnt"] += 1

    out: Dict[str, Dict[str, Any]] = {}
    for tid, s in cat.items():
        avg_view = (s["view_sum"] / s["view_cnt"]) if s["view_cnt"] > 0 else None
        avg_like_rate = (s["like_rate_sum"] / s["like_rate_cnt"]) if s["like_rate_cnt"] > 0 else None

        # ✅ 保持兼容旧字段名（0h == capture snapshot）
        out[tid] = {
            "tname": s["tname"],
            "video_count": s["video_count"],
            "avg_view_0h": avg_view,
            "avg_like_rate_0h": avg_like_rate,
            # ✅ 新增更不误导的字段名
            "avg_view_as_of_capture": avg_view,
            "avg_like_rate_as_of_capture": avg_like_rate,
        }

    doc["category_stats"] = out
    return doc


# ================== 聚合：按 bvid，把不同时间抓到的数据挂到 captures 下 ==================
def _unique_capture_key(capture_ts: int, run_id: str, existing_captures: Dict[str, Any]) -> str:
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

        # 固定字段：以最新覆盖（方便查）
        node["title"] = v.get("title") or node.get("title")
        node["url"] = v.get("url") or node.get("url")
        node["tid"] = v.get("tid") if v.get("tid") is not None else node.get("tid")
        node["tname"] = v.get("tname") or node.get("tname")
        node["pubdate"] = v.get("pubdate") if v.get("pubdate") is not None else node.get("pubdate")
        node["up"] = v.get("up") or node.get("up")
        node["label_source"] = v.get("label_source") or node.get("label_source")

        # covered_until：取更晚的一个（更保守）
        cu_old = node.get("covered_until_latest")
        cu_new = v.get("covered_until")
        if isinstance(cu_old, int) and isinstance(cu_new, int):
            node["covered_until_latest"] = max(cu_old, cu_new)
        elif cu_old is None and isinstance(cu_new, int):
            node["covered_until_latest"] = cu_new

        if isinstance(node.get("first_seen_ts"), int) and isinstance(v.get("first_seen_ts"), int):
            node["first_seen_ts"] = min(node["first_seen_ts"], v["first_seen_ts"])
        elif node.get("first_seen_ts") is None and isinstance(v.get("first_seen_ts"), int):
            node["first_seen_ts"] = v["first_seen_ts"]

        captures: Dict[str, Any] = node.get("captures") or {}
        key = _unique_capture_key(int(v.get("capture_ts") or capture_ts), str(run_id), captures)

        captures[key] = {
            "ts": int(v.get("capture_ts") or capture_ts),
            "run_id": run_id,
            # ✅ 两个都存：query_rid 表示本次列表来自哪个请求参数 rid
            "query_rid": v.get("query_rid"),
            # ✅ tid/tname 表示视频本身分区
            "tid": v.get("tid"),
            "tname": v.get("tname"),
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


# ================== 主流程：每次 run 独立 + 更新聚合总文件 ==================
def main() -> None:
    ensure_dirs()

    run_ts_float = now_ts_float()
    ts = int(run_ts_float)  # capture_ts：秒级
    date = today_str(ts)
    run_id = datetime_compact_from_float(run_ts_float)

    run_path = os.path.join(
        RUN_DIR,
        f"{date}__run_{run_id}__rids{len(RID_LIST)}__pn{PN_MAX}__ps{PS}__obs{OBS_HOURS}h.json"
    )

    doc: Dict[str, Any] = {
        "date": date,
        "capture_ts": ts,
        "last_capture_ts": ts,
        "source": "bilibili_dynamic_region",
        "run_id": run_id,
        "runs": [{
            "ts": ts,
            "script": "v2_region_negative_run_crawler",
            "rid_list": RID_LIST,
            "pn_max": PN_MAX,
            "ps": PS,
            "obs_hours": OBS_HOURS,
        }],
        "videos": [],
        "meta": {"pos_count": 0, "neg_count": 0, "total_count": 0},
        "count": 0,
        "category_stats": {},
    }

    all_archives: List[Dict[str, Any]] = []
    pages_fetched = 0

    s = build_session()
    try:
        for rid in RID_LIST:
            rid_collected = 0

            for pn in range(1, PN_MAX + 1):
                # ✅ 更鲁棒：单页失败不让全局崩掉
                try:
                    raw = fetch_dynamic_region_page(s, rid=rid, pn=pn, ps=PS)
                except Exception as e:
                    # 尽可能留日志，继续下一个 rid
                    print(f"[WARN] rid={rid} pn={pn} fetch failed: {e}; stop this rid.")
                    break

                # ✅ 每页 raw 只保存一次：包含 run_id/rid/pn/ps
                raw_path = os.path.join(
                    RAW_DIR, f"{date}__run_{run_id}__rid{rid}__pn{pn}__ps{PS}.json"
                )
                save_json(raw_path, raw)

                data = raw.get("data") or {}
                archives = data.get("archives") or []

                # ✅ 如果这一页没有内容：停止当前 rid（符合常见分页逻辑）
                if not archives:
                    print(f"[INFO] rid={rid} pn={pn} empty, stop this rid.")
                    break

                # ✅ 注入 query_rid，后续 normalize/agg 不会把 tid 当 rid
                for a in archives:
                    if isinstance(a, dict):
                        a["_query_rid"] = rid

                all_archives.extend(archives)
                rid_collected += len(archives)
                pages_fetched += 1
                time.sleep(SLEEP_BETWEEN_PAGES)

            print(f"[INFO] rid={rid} collected={rid_collected}")
    finally:
        s.close()

    videos = normalize_dynamic_archives(all_archives, ts)
    doc["videos"] = videos
    doc = recompute_run_fields(doc)

    save_json(run_path, doc)

    agg = update_aggregate_history(doc, AGG_PATH)

    print(
        f"[OK] date={date} run_id={run_id} rids={len(RID_LIST)} pages={pages_fetched} "
        f"archives={len(all_archives)} videos={len(videos)} -> {run_path}"
    )
    print(f"[OK] agg -> {AGG_PATH} (unique_videos={agg['meta']['video_unique_count']})")


if __name__ == "__main__":
    main()