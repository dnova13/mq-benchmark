#!/usr/bin/env python3
"""Spring vs Nest 비교표. 사용: python3 compare.py reports/spring-raw.jsonl reports/nest-raw.jsonl"""
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
SC = sorted({k[0] for k in sp} & {k[0] for k in ne})

print('| 시나리오 | MQ | Spring TPS | Nest TPS | Nest/Spring | Spring p95 | Nest p95 | 범위 겹침 | 우세 |')
print('|---|---|---:|---:|---:|---:|---:|:--:|:--:|')
ratios = collections.defaultdict(list)
wins = collections.Counter()
for s in SC:
    for m in MQS:
        a, b = sp[(s, m)], ne[(s, m)]
        ta, tb = med(a, 'tps'), med(b, 'tps')
        ra = [x['tps'] for x in a]; rb = [x['tps'] for x in b]
        overlap = max(min(ra), min(rb)) <= min(max(ra), max(rb))
        ratio = tb / ta
        ratios['all'].append(ratio)
        ratios['A' if s.startswith('A') else 'B'].append(ratio)
        ratios['db' if a[0]['db'] else 'nodb'].append(ratio)
        ratios[m].append(ratio)
        win = '-' if overlap else ('Nest' if ratio > 1 else 'Spring')
        wins[win] += 1
        print(f"| {s} | {m} | {int(ta):,} | {int(tb):,} | {ratio:.2f} | {int(med(a,'p95'))} | {int(med(b,'p95'))} | {'예' if overlap else '아니오'} | {win} |")

gm = lambda v: math.exp(sum(math.log(x) for x in v) / len(v))
print('\n### 요약 (Nest TPS ÷ Spring TPS의 기하평균, 1보다 크면 Nest가 빠름)\n')
print('| 구분 | 비율 | 케이스 수 |')
print('|---|---:|---:|')
names = [('all', '전체'), ('A', '시나리오 A(대량 발행)'), ('B', '시나리오 B(요청 1건=이벤트 1건)'),
         ('nodb', 'DB 저장 없음'), ('db', 'DB 저장 있음'), ('kafka', 'Kafka'), ('redis', 'Redis Streams'), ('rabbit', 'RabbitMQ')]
for k, n in names:
    print(f"| {n} | {gm(ratios[k]):.2f} | {len(ratios[k])} |")
print(f"\n범위가 겹치지 않아 우열이 분명한 케이스: Spring {wins['Spring']}개, Nest {wins['Nest']}개, 구분 어려움 {wins['-']}개")
