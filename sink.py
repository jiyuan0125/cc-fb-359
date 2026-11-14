#!/usr/bin/env python3
"""续写管道：按文件名顺序消费 incoming/ 下的 jsonl 批次，落库 store.db。

规则：
- 已在库中的批次（events.batch_id 出现过）整批跳过，不处理也不进报告；
  其存量落库记录仍参与去重窗与序号接续的判定。
- 去重窗为滚动 24 小时，锚点是该 idem_key 最后一条入账记录的 event_time；
  事件时间与锚点之差 < 24h 视为窗内，后来事件丢弃（去重）；
  差 >= 24h（恰满 24h 按跨窗算）视为新事件入账并刷新锚点。
- 缺任一必填字段（idem_key/seq/event_time/payload，含空值）或 event_time
  无法解析、seq 非整数的记录无法写入。
- 任一记录写入失败即整批失败：回滚本批已写行，随后整批重写一次；重写时
  无法写入的记录跳过，不写也不计入去重。
- 输出 batch_report.txt 与 check_report.txt。
"""

import json
import os
import sqlite3
import sys
from datetime import datetime

REQUIRED = ("idem_key", "seq", "event_time", "payload")
WINDOW_SECONDS = 24 * 60 * 60
TIME_FMT = "%Y-%m-%dT%H:%M:%SZ"


def parse_time(value):
    if not isinstance(value, str):
        raise ValueError("event_time 必须是字符串")
    return datetime.strptime(value, TIME_FMT)


def load_records(path):
    """读取批次文件，每行一条 json；无法解析的行按不可写记录占位。"""
    records = []
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                records.append(None)
    return records


def writable(record):
    """不可写返回 None；可写返回 (key, seq, time_str, dt, payload)。"""
    if not isinstance(record, dict):
        return None
    for field in REQUIRED:
        if field not in record or record[field] is None:
            return None
    if not isinstance(record["seq"], int) or isinstance(record["seq"], bool):
        return None
    try:
        dt = parse_time(record["event_time"])
    except (ValueError, TypeError):
        return None
    return (
        record["idem_key"],
        record["seq"],
        record["event_time"],
        dt,
        record["payload"],
    )


def decide(anchors, key, dt):
    """返回 ('post'|'dedupe', 与锚点秒差或 None)。恰 24h 按跨窗入账。"""
    if key not in anchors:
        return "post", None
    delta = (dt - anchors[key][0]).total_seconds()
    if delta < WINDOW_SECONDS:
        return "dedupe", delta
    return "post", delta


def process_batch(con, batch_id, records, anchors):
    """处理一个批次，返回 (posted, deduped, revoked, excluded, audit)。"""
    audit = {
        "exact24_post": 0,
        "exact24_dedupe": 0,
        "cross_post": 0,
        "within_dedupe": 0,
    }

    # ---- 第一次：整批试写，遇不可写记录即整批失败并回滚 ----
    trial_anchors = dict(anchors)
    cur = con.cursor()
    inserted = 0
    failed = False
    try:
        for record in records:
            item = writable(record)
            if item is None:
                failed = True
                break
            key, seq, time_str, dt, payload = item
            action, _delta = decide(trial_anchors, key, dt)
            if action == "dedupe":
                continue
            cur.execute(
                "INSERT INTO events (idem_key, seq, event_time, payload, batch_id)"
                " VALUES (?, ?, ?, ?, ?)",
                (key, seq, time_str, payload, batch_id),
            )
            trial_anchors[key] = (dt, seq)
            inserted += 1
    except sqlite3.Error:
        failed = True

    revoked = 0
    if failed:
        con.rollback()
        revoked = inserted  # 本批已写行全部撤销
    else:
        con.commit()

    # ---- 重写：失败批跳过不可写记录（不写、不去重、不刷新锚点）----
    if failed:
        work_anchors = dict(anchors)
    else:
        work_anchors = trial_anchors  # 首次成功，锚点即试写结果
    posted = deduped = excluded = 0
    if failed:
        cur = con.cursor()
        for record in records:
            item = writable(record)
            if item is None:
                excluded += 1
                continue
            key, seq, time_str, dt, payload = item
            action, delta = decide(work_anchors, key, dt)
            if action == "dedupe":
                deduped += 1
                audit["within_dedupe"] += 1
                if delta == WINDOW_SECONDS:
                    audit["exact24_dedupe"] += 1
                continue
            cur.execute(
                "INSERT INTO events (idem_key, seq, event_time, payload, batch_id)"
                " VALUES (?, ?, ?, ?, ?)",
                (key, seq, time_str, payload, batch_id),
            )
            work_anchors[key] = (dt, seq)
            posted += 1
            if delta is not None:
                audit["cross_post"] += 1
                if delta == WINDOW_SECONDS:
                    audit["exact24_post"] += 1
        con.commit()
    else:
        # 首次成功：重放判定以统计去重/跨窗明细（行已在试写事务中落库）
        stat_anchors = dict(anchors)
        for record in records:
            item = writable(record)  # 首次成功意味着全部可写
            key, _seq, _t, dt, _p = item
            action, delta = decide(stat_anchors, key, dt)
            if action == "dedupe":
                deduped += 1
                audit["within_dedupe"] += 1
                if delta == WINDOW_SECONDS:
                    audit["exact24_dedupe"] += 1
                continue
            stat_anchors[key] = (dt, _seq)
            posted += 1
            if delta is not None:
                audit["cross_post"] += 1
                if delta == WINDOW_SECONDS:
                    audit["exact24_post"] += 1

    anchors.clear()
    anchors.update(work_anchors)
    return posted, deduped, revoked, excluded, audit


def run_checks(con, batch_stats, totals, stock_count):
    """六项检查，返回 (行列表, 是否全部通过)。"""
    # 1) 重复：同键且事件时间差 < 24h 却同时落库的记录对
    dup_pairs = 0
    last_time = {}
    for key, event_time in con.execute(
        "SELECT idem_key, event_time FROM events ORDER BY rowid"
    ):
        dt = parse_time(event_time)
        if key in last_time and (dt - last_time[key]).total_seconds() < WINDOW_SECONDS:
            dup_pairs += 1
        last_time[key] = dt

    # 2) 丢失：未对账记录；排除记录另列
    unreconciled = sum(
        rows - posted - deduped - excluded
        for rows, posted, deduped, _rev, excluded in batch_stats
    )

    # 3) 序号：同键落库 seq 从 1 起逐条接续
    seq_anomalies = 0
    key_seqs = {}
    for key, seq in con.execute("SELECT idem_key, seq FROM events ORDER BY rowid"):
        key_seqs.setdefault(key, []).append(seq)
    for seqs in key_seqs.values():
        expected = 1
        for seq in seqs:
            if seq != expected:
                seq_anomalies += 1
            expected = seq + 1

    # 4) 窗归属
    exact24_post = totals["exact24_post"]
    exact24_dedupe = totals["exact24_dedupe"]
    cross_post = totals["cross_post"]
    within_dedupe = totals["within_dedupe"]

    # 5) 计数：rows == posted + deduped + excluded 的对平批数
    reconciled_batches = sum(
        1 for rows, posted, deduped, _rev, excluded in batch_stats
        if rows == posted + deduped + excluded
    )

    # 6) 对账：落库合计 == 存量 + 本次入账
    final_count = con.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    new_posted = totals["posted"]
    reconciled = (final_count == stock_count + new_posted)

    lines = [
        f"重复: {'通过' if dup_pairs == 0 else '不通过'} 同键窗内落库对 {dup_pairs}",
        f"丢失: {'通过' if unreconciled == 0 else '不通过'} "
        f"未对账记录 {unreconciled} 排除记录 {totals['excluded']}",
        f"序号: {'通过' if seq_anomalies == 0 else '不通过'} 序号异常 {seq_anomalies}",
        f"窗归属: {'通过' if exact24_dedupe == 0 else '不通过'} "
        f"恰二十四小时入账 {exact24_post} 恰二十四小时去重 {exact24_dedupe} "
        f"跨窗入账合计 {cross_post} 窗内去重合计 {within_dedupe}",
        f"计数: {'通过' if reconciled_batches == len(batch_stats) else '不通过'} "
        f"对平批数 {reconciled_batches}",
        f"对账: {'通过' if reconciled else '不通过'} "
        f"落库合计 {final_count} 存量 {stock_count} 本次入账 {new_posted}",
    ]
    all_pass = (
        dup_pairs == 0
        and unreconciled == 0
        and seq_anomalies == 0
        and exact24_dedupe == 0
        and reconciled_batches == len(batch_stats)
        and reconciled
    )
    lines.append(f"总检: {'合格' if all_pass else '不合格'}")
    return lines, all_pass


def main(incoming_dir="incoming", db_path="store.db",
         batch_report="batch_report.txt", check_report="check_report.txt"):
    base = os.getcwd()
    incoming_dir = os.path.join(base, incoming_dir)
    db_path = os.path.join(base, db_path)

    con = sqlite3.connect(db_path)
    already = {
        row[0] for row in con.execute("SELECT DISTINCT batch_id FROM events")
    }
    stock_count = con.execute("SELECT COUNT(*) FROM events").fetchone()[0]

    # 存量记录参与去重窗与序号接续：按落库顺序重建锚点
    anchors = {}
    for key, event_time, seq in con.execute(
        "SELECT idem_key, event_time, seq FROM events ORDER BY rowid"
    ):
        anchors[key] = (parse_time(event_time), seq)

    batch_stats = []  # (batch_id, rows, posted, deduped, revoked, excluded)
    totals = {
        "posted": 0, "deduped": 0, "revoked": 0, "excluded": 0,
        "exact24_post": 0, "exact24_dedupe": 0,
        "cross_post": 0, "within_dedupe": 0,
    }

    for name in sorted(os.listdir(incoming_dir)):
        if not name.endswith(".jsonl"):
            continue
        batch_id = os.path.splitext(name)[0]
        if batch_id in already:
            continue  # 已在库批次：不处理、不计入报告
        records = load_records(os.path.join(incoming_dir, name))
        posted, deduped, revoked, excluded, audit = process_batch(
            con, batch_id, records, anchors
        )
        batch_stats.append(
            (batch_id, len(records), posted, deduped, revoked, excluded)
        )
        totals["posted"] += posted
        totals["deduped"] += deduped
        totals["revoked"] += revoked
        totals["excluded"] += excluded
        for key in ("exact24_post", "exact24_dedupe",
                    "cross_post", "within_dedupe"):
            totals[key] += audit[key]

    with open(os.path.join(base, batch_report), "w", encoding="utf-8") as fh:
        fh.write("batch,rows,posted,deduped,revoked\n")
        for batch_id, rows, posted, deduped, revoked, _ex in batch_stats:
            fh.write(f"{batch_id},{rows},{posted},{deduped},{revoked}\n")

    check_stats = [
        (rows, posted, deduped, revoked, excluded)
        for _bid, rows, posted, deduped, revoked, excluded in batch_stats
    ]
    check_lines, _all_pass = run_checks(con, check_stats, totals, stock_count)
    with open(os.path.join(base, check_report), "w", encoding="utf-8") as fh:
        fh.write("\n".join(check_lines) + "\n")

    con.close()


if __name__ == "__main__":
    main(*sys.argv[1:])
