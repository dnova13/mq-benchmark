#!/usr/bin/env python3
"""Spring vs Nest 비교표. 사용: python3 compare.py reports/scale-spring-raw.jsonl reports/scale-nest-raw.jsonl"""
import collections
import json
import math
import statistics as st
import sys

def load(path):
    g = collections.defaultdict(list)
    for l in open(path):
        r = json.loads(l)
        g[(r['scenario'], r['mq'])].append(r)
    return g

sp, ne = load(sys.argv[1]), load(sys.argv[2])
med = lambda rs, k: st.median(x[k] for x in rs)
MQS = ['kafka', 'redis', 'rabbit']
count_of = {s: rs[0]['count'] for (s, _), rs in sp.items()}
SC = sorted({k[0] for k in sp} & {k[0] for k in ne}, key=lambda s: (s[0], count_of[s]))

print('| 방식 | 건수 | MQ | Spring TPS | Nest TPS | Nest/Spring | Spring p95(ms) | Nest p95(ms) | 범위 겹침 | 우세 |')
print('|---|---:|---|---:|---:|---:|---:|---:|:--:|:--:|')
ratios = collections.defaultdict(list)
wins = collections.Counter()
for s in SC:
    for m in MQS:
        a, b = sp.get((s, m)), ne.get((s, m))
        if not a or not b:
            continue
        ta, tb = med(a, 'tps'), med(b, 'tps')
        if not ta or not tb:  # 미완료 등으로 TPS가 0이면 비율에서 뺀다
            continue
        ra = [x['tps'] for x in a]; rb = [x['tps'] for x in b]
        overlap = max(min(ra), min(rb)) <= min(max(ra), max(rb))
        ratio = tb / ta
        for k in ('all', s[0], m, (s[0], count_of[s])):
            ratios[k].append(ratio)
        win = '-' if overlap else ('Nest' if ratio > 1 else 'Spring')
        wins[win] += 1
        print(f"| {s[0]} | {count_of[s]:,} | {m} | {int(ta):,} | {int(tb):,} | {ratio:.2f} | "
              f"{int(med(a,'p95')):,} | {int(med(b,'p95')):,} | {'예' if overlap else '아니오'} | {win} |")

gm = lambda v: math.exp(sum(math.log(x) for x in v) / len(v))
print('\n### 요약 (Nest TPS ÷ Spring TPS의 기하평균, 1보다 크면 Nest가 빠름)\n')
print('| 구분 | 비율 | 케이스 수 |')
print('|---|---:|---:|')
names = [('all', '전체'), ('A', 'A 대량 발행'), ('B', 'B 요청 1건=이벤트 1건'),
         ('kafka', 'Kafka'), ('redis', 'Redis Streams'), ('rabbit', 'RabbitMQ')]
for k, n in names:
    if ratios[k]:
        print(f"| {n} | {gm(ratios[k]):.2f} | {len(ratios[k])} |")
print('\n### 건수별 요약 (세 MQ의 기하평균)\n')
print('| 방식 | 건수 | Nest/Spring |')
print('|---|---:|---:|')
for k in sorted((k for k in ratios if isinstance(k, tuple))):
    print(f"| {k[0]} | {k[1]:,} | {gm(ratios[k]):.2f} |")
print(f"\n범위가 겹치지 않아 우열이 분명한 케이스: Spring {wins['Spring']}개, Nest {wins['Nest']}개, 구분 어려움 {wins['-']}개")
