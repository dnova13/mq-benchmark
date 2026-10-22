#!/usr/bin/env python3
"""results/*.jsonl -> 마크다운 표 (시나리오별 MQ 비교 + MQ별 종합).
사용: python3 report.py results/spring-XXXX.jsonl > reports/spring-tables.md
"""
import collections
import json
import statistics as st
import sys

DESC = {
    'A1': 'A 대량발행 · 1,000건 · DB 없음', 'A2': 'A 대량발행 · 10,000건 · DB 없음',
    'A3': 'A 대량발행 · 1,000건 · DB 저장', 'A4': 'A 대량발행 · 10,000건 · DB 저장',
    'A5': 'A 대량발행 · 10,000건 · 1KB · DB 저장',
    'B1': 'B 요청1건=이벤트1건 · 1,000건 · VU 50 · DB 저장', 'B2': 'B · 10,000건 · VU 100 · DB 저장',
    'B3': 'B · 10,000건 · VU 100 · DB 없음', 'B4': 'B · 10,000건 · VU 10 · DB 저장',
    'B5': 'B · 10,000건 · VU 200 · DB 저장',
    'A6': 'A 대량발행 · 100,000건 · DB 저장', 'A7': 'A 대량발행 · 1,000,000건 · DB 저장',
    'A8': 'A 대량발행 · 1,000,000건 · DB 없음',
    'B6': 'B · 100,000건 · VU 100 · DB 저장', 'B7': 'B · 1,000,000건 · VU 100 · DB 저장',
}
MQS = ['kafka', 'redis', 'rabbit']
rows = [json.loads(l) for l in open(sys.argv[1])]
g = collections.defaultdict(list)
for r in rows:
    g[(r['scenario'], r['mq'])].append(r)
# 측정이 없는 MQ는 0으로 표시한다(대용량 일부만 실행한 경우)
med = lambda rs, k: int(st.median(x[k] for x in rs)) if rs else 0

print(f"앱: {rows[0]['app']} · 시나리오당 MQ별 {max(len(v) for v in g.values())}회 측정의 중앙값\n")
ranks = collections.defaultdict(list)
for s in sorted({k[0] for k in g}):
    print(f"### {s}: {DESC.get(s, '')}\n")
    print('| MQ | TPS(중앙값) | TPS 최소~최대 | p50(ms) | p95(ms) | p99(ms) | 순위(TPS) |')
    print('|---|---:|---|---:|---:|---:|---:|')
    order = sorted(MQS, key=lambda m: -med(g[(s, m)], 'tps'))
    for m in MQS:
        rs = g.get((s, m), [])
        tps = [x['tps'] for x in rs] or [0]
        rk = order.index(m) + 1
        ranks[m].append(rk)
        ok = '' if rs and all(x['done'] for x in rs) else ' ⚠미완료'
        print(f"| {m} | {med(rs,'tps'):,} | {min(tps):,}~{max(tps):,} | {med(rs,'p50')} | {med(rs,'p95')} | {med(rs,'p99')} | {rk}{ok} |")
    print()
print('### MQ별 종합 (시나리오 전체)\n')
print('| MQ | 1위 횟수 | 평균 순위 | 시나리오별 순위 |')
print('|---|---:|---:|---|')
sc = sorted({k[0] for k in g})
for m in MQS:
    print(f"| {m} | {ranks[m].count(1)} | {sum(ranks[m])/len(ranks[m]):.2f} | {', '.join(f'{s}:{r}' for s, r in zip(sc, ranks[m]))} |")
