#!/usr/bin/env python3
"""results/*.jsonl -> 마크다운 표 (규모별 MQ 비교 + MQ별 종합).
사용: python3 report.py reports/scale-spring-raw.jsonl
"""
import collections
import json
import statistics as st
import sys

MQS = ['kafka', 'redis', 'rabbit']
KIND = {'A': 'A 대량 발행 (요청 1번에 서버가 N건 발행)',
        'B': 'B 요청 1건 = 이벤트 1건 (VU(동시 사용자) 100명이 합계 N번 요청)'}
rows = [json.loads(l) for l in open(sys.argv[1])]
g = collections.defaultdict(list)
count_of = {}
for r in rows:
    g[(r['scenario'], r['mq'])].append(r)
    count_of[r['scenario']] = r['count']
# 측정이 없는 MQ는 0으로 표시한다
med = lambda rs, k: int(st.median(x[k] for x in rs)) if rs else 0
# 방식(A/B) → 건수 순
scenarios = sorted(count_of, key=lambda s: (s[0], count_of[s]))

print(f"앱: {rows[0]['app']} · 시나리오별 MQ마다 반복 측정의 중앙값 (반복 횟수는 표의 n)\n")
ranks = collections.defaultdict(list)
for kind in ['A', 'B']:
    sc = [s for s in scenarios if s[0] == kind]
    if not sc:
        continue
    print(f"## {KIND[kind]}\n")
    print('| 건수 | MQ | n | TPS(중앙값) | TPS 최소~최대 | p50(ms) | p95(ms) | p99(ms) | 순위(TPS) |')
    print('|---:|---|---:|---:|---|---:|---:|---:|---:|')
    for s in sc:
        order = sorted(MQS, key=lambda m: -med(g.get((s, m), []), 'tps'))
        for m in MQS:
            rs = g.get((s, m), [])
            tps = [x['tps'] for x in rs] or [0]
            rk = order.index(m) + 1
            ranks[(kind, m)].append((s, rk))
            ok = '' if rs and all(x['done'] for x in rs) else ' ⚠미완료'
            print(f"| {count_of[s]:,} | {m} | {len(rs)} | {med(rs,'tps'):,} | {min(tps):,}~{max(tps):,} | "
                  f"{med(rs,'p50'):,} | {med(rs,'p95'):,} | {med(rs,'p99'):,} | {rk}{ok} |")
    print()
    print(f"### {kind} 규모별 TPS 요약\n")
    print('| 건수 | ' + ' | '.join(MQS) + ' | 1위 |')
    print('|---:|' + '---:|' * len(MQS) + '---|')
    for s in sc:
        vals = {m: med(g.get((s, m), []), 'tps') for m in MQS}
        print(f"| {count_of[s]:,} | " + ' | '.join(f"{vals[m]:,}" for m in MQS) + f" | {max(vals, key=vals.get)} |")
    print()
    print(f"### {kind} MQ별 종합\n")
    print('| MQ | 1위 횟수 | 평균 순위 | 건수별 순위 |')
    print('|---|---:|---:|---|')
    for m in MQS:
        rk = ranks[(kind, m)]
        print(f"| {m} | {sum(1 for _, r in rk if r == 1)} | {sum(r for _, r in rk)/len(rk):.2f} | "
              f"{', '.join(f'{count_of[s]:,}:{r}' for s, r in rk)} |")
    print()
