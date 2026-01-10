# -*- coding: utf-8 -*-
"""JSON-only Week2 - Step2: 采集负样本（候选）

从分区最新投稿接口抓取候选负样本：
  - https://api.bilibili.com/x/web-interface/dynamic/region

本脚本会：
  1) 按 rid(分区 tid) + pn/ps 分页拉取最新投稿
  2) 每页原始 response 备份到 data/raw/region/
  3) 将视频规范化为与 v1.py 相同的 record 结构（label=0）
  4) 按 bvid 主键合并写入 data/daily/YYYY-MM-DD.json
     - 若同一 bvid 同时出现 label=0/1，将保留 label=1（由 v1 的合并规则保证）

重要说明（避免逻辑错误）：
  - 本脚本采集的是“候选负样本”。严格的 y=0 需要等观察窗口结束后才能确认。
  - 因此每条负样本会写入 covered_until = pubdate + OBS_HOURS。

用法：
  python v2_collect_negative_dynamic_region.py

可改参数：
  RID_LIST: 默认覆盖若干主分区（可自行按 tid 扩展）
  PN_MAX, PS: 分页参数
  OBS_HOURS: 观察窗口（小时），用于 covered_until
"""

from __future__ import annotations

import json
import os
import time
from typing import Any, Dict, List, Optional

import requests

# 复用 v1.py 中的工具与合并逻辑
# v1.py 需要与本文件位于同一目录，或在 PYTHONPATH 中。
from Pos_sample import (
    DAILY_DIR,
    RAW_DIR,
    SLEEP_BETWEEN_PAGES,
    build_session,
    datetime_compact,
    dedup_by_bvid,
    load_daily_if_exists,
    merge_daily_pos,          # 虽然名字叫 pos，但内部是按 bvid 合并，label=1 优先
    recompute_daily_fields,
    safe_div,
    today_str,
)

# --------------------------
# 配置区
# --------------------------

API_DYNAMIC_REGION = "https://api.bilibili.com/x/web-interface/dynamic/region"

# 负样本 raw 目录（在 v1 的 RAW_DIR 下新增子目录 region）
RAW_REGION_DIR = os.path.join(RAW_DIR, "region")

# 默认抓一些“主分区”以保证覆盖面；你可以按 video_zone.md 的 tid 自由加。
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

# 观察窗口（用于 covered_until）；严格 y=0 需要等到 now > covered_until 且期间未进入热门
OBS_HOURS = 48


# --------------------------
# API 调用
# --------------------------

def fetch_dynamic_region_page(s: requests.Session, rid: int, pn: int, ps: int) -> Dict[str, Any]:
    """分区最新投稿（兼容 code=-404 '啥都木有' 作为空页）"""
    params = {"rid": rid, "pn": pn, "ps": ps}
    r = s.get(API_DYNAMIC_REGION, params=params, timeout=15)
    r.raise_for_status()
    data = r.json()

    if not isinstance(data, dict):
        raise RuntimeError(f"dynamic/region response not dict rid={rid} pn={pn} ps={ps}")

    code = data.get("code")
    if code == 0:
        return data

    # ✅ -404：这一页没数据了，转为空 archives，让上层自然 break
    if code == -404:
        return {"code": 0, "data": {"archives": []}, "message": data.get("message")}

    # ❗其他非0：真错误，抛出
    raise RuntimeError(
        f"dynamic/region error rid={rid} pn={pn} ps={ps}: code={code} msg={data.get('message')}"
    )

def normalize_dynamic_archives(archives: List[Dict[str, Any]], capture_ts: int) -> List[Dict[str, Any]]:
    """把 dynamic/region 的 archives 规范化成与 v1 一致的 video record。"""
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
                "title": a.get("title"),
                "url": f"https://www.bilibili.com/video/{bvid}",
                "tid": a.get("tid"),
                "tname": a.get("tname"),
                "pubdate": pub_ts,
                "duration": a.get("duration"),

                # 负样本候选
                "label": 0,
                "label_source": "dynamic_region",
                "covered_until": covered_until_ts,

                # 追溯
                "first_seen_ts": capture_ts,

                "up": {
                    "mid": owner.get("mid"),
                    "name": owner.get("name"),
                    # dynamic/region 通常也拿不到 follower；保留空值
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
                    # 兼容旧字段名
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
                    "0h": features_as_of_capture,
                },
            }
        )

    return out


# --------------------------
# 主流程
# --------------------------

def main() -> None:
    ts = int(time.time())
    date = today_str(ts)

    os.makedirs(DAILY_DIR, exist_ok=True)
    os.makedirs(RAW_REGION_DIR, exist_ok=True)

    daily_path = os.path.join(DAILY_DIR, f"{date}.json")
    daily = load_daily_if_exists(daily_path) or {
        "date": date,
        "videos": [],
        "meta": {"pos_count": 0, "neg_count": 0, "total_count": 0},
    }

    # 跟 v1 一致的 run 记录方式
    daily["date"] = date
    daily.setdefault("capture_ts", ts)
    daily["last_capture_ts"] = ts
    daily.setdefault("runs", [])
    daily["runs"].append(
        {
            "ts": ts,
            "script": "v2_collect_negative_dynamic_region",
            "rid_list": RID_LIST,
            "pn_max": PN_MAX,
            "ps": PS,
            "obs_hours": OBS_HOURS,
        }
    )

    all_archives: List[Dict[str, Any]] = []

    s = build_session()
    try:
        for rid in RID_LIST:
            rid_collected = 0
            for pn in range(1, PN_MAX + 1):
                raw = fetch_dynamic_region_page(s, rid=rid, pn=pn, ps=PS)

                # ✅ 先备份 raw（即使 archives 为空也保存，方便你复盘）
                raw_path = os.path.join(
                    RAW_REGION_DIR, f"{datetime_compact(ts)}_rid{rid}_pn{pn}_ps{PS}.json"
                )
                with open(raw_path, "w", encoding="utf-8") as f:
                    json.dump(raw, f, ensure_ascii=False, indent=2)

                data = raw.get("data") or {}
                archives = data.get("archives") or []

                if not archives:
                    print(f"[INFO] rid={rid} pn={pn} empty (or -404), stop this rid.")
                    break

                # 备份 raw
                raw_path = os.path.join(
                    RAW_REGION_DIR, f"{datetime_compact(ts)}_rid{rid}_pn{pn}_ps{PS}.json"
                )
                with open(raw_path, "w", encoding="utf-8") as f:
                    json.dump(raw, f, ensure_ascii=False, indent=2)

                all_archives.extend(archives)
                rid_collected += len(archives)
                time.sleep(SLEEP_BETWEEN_PAGES)

            print(f"[INFO] rid={rid} collected={rid_collected}")

    finally:
        s.close()

    new_neg = normalize_dynamic_archives(all_archives, ts)
    new_neg = dedup_by_bvid(new_neg)

    # 合并进 daily（merge_daily_pos 内部是 bvid 主键合并，label=1 优先）
    daily = merge_daily_pos(daily, new_neg)
    daily = recompute_daily_fields(daily)

    with open(daily_path, "w", encoding="utf-8") as f:
        json.dump(daily, f, ensure_ascii=False, indent=2)

    print(
        f"[OK] date={date} rids={len(RID_LIST)} archives={len(all_archives)} "
        f"neg_added={len(new_neg)} merged_total={daily.get('meta', {}).get('total_count')}"
    )
    print(f"[OK] daily -> {daily_path}")


if __name__ == "__main__":
    main()