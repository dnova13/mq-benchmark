#!/usr/bin/env python3
"""규모 시리즈 결과 -> PNG 그래프 (reports/img/).
사용: python3 charts.py reports/scale-spring-raw.jsonl reports/scale-nest-raw.jsonl \
         [reports/locust-spring-raw.jsonl reports/locust-nest-raw.jsonl]
필요: pip install matplotlib, 한글 폰트(NanumGothic 등)
"""
import collections
import json
import os
import statistics as st
import sys

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib import font_manager
from matplotlib.ticker import FuncFormatter

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'reports', 'img')
MQS = ['kafka', 'redis', 'rabbit']
LABEL = {'kafka': 'Kafka', 'redis': 'Redis Streams', 'rabbit': 'RabbitMQ'}
# 범주형 팔레트 1~3번(검증 통과). 색만으로 구분하지 않도록 마커 모양과 선 끝 라벨을 같이 쓴다.
COLOR = {'kafka': '#2a78d6', 'redis': '#eb6834', 'rabbit': '#1baf7a'}
MARKER = {'kafka': 'o', 'redis': 's', 'rabbit': '^'}
SURFACE, INK, INK2, GRID = '#fcfcfb', '#0b0b0b', '#52514e', '#e4e3df'
KIND = {'A': 'A 대량 발행', 'B': 'B 요청 1건 = 이벤트 1건 (VU(동시 사용자) 100)'}

for name in ('NanumGothic', 'Noto Sans CJK KR', 'Noto Sans KR'):
    if any(name in f.name for f in font_manager.fontManager.ttflist):
        plt.rcParams['font.family'] = name
        break
plt.rcParams.update({
    'axes.facecolor': SURFACE, 'figure.facecolor': SURFACE, 'axes.edgecolor': GRID,
    'axes.labelcolor': INK2, 'xtick.color': INK2, 'ytick.color': INK2, 'text.color': INK,
    'axes.grid': True, 'grid.color': GRID, 'grid.linewidth': 0.8, 'axes.axisbelow': True,
    'axes.spines.top': False, 'axes.spines.right': False, 'axes.unicode_minus': False,
})


def load(path):
    g = collections.defaultdict(list)
    for l in open(path):
        r = json.loads(l)
        if r['done']:
            g[(r['scenario'][0], r['count'], r['mq'])].append(r)
    return {k: (st.median(x['tps'] for x in v), st.median(x['p95'] for x in v)) for k, v in g.items()}


def human(n, _=None):
    n = int(n)
    return f'{n // 10_000}만' if n >= 10_000 and n % 10_000 == 0 else f'{n:,}'


def end_labels(ax, items, gap=13):
    """선 끝 라벨이 겹치지 않도록 화면 좌표에서 최소 gap(px)만큼 위아래로 벌린다. items: [(x, y, text)]"""
    if not items:
        return
    ax.figure.canvas.draw()
    to_px, to_data = ax.transData, ax.transData.inverted()
    pts = sorted(((to_px.transform((x, y)), x, t) for x, y, t in items), key=lambda p: p[0][1])
    ys = [p[0][1] for p in pts]
    for i in range(1, len(ys)):
        ys[i] = max(ys[i], ys[i - 1] + gap)
    for (px, x, t), y in zip(pts, ys):
        ax.annotate(t, xy=(x, to_data.transform((px[0], y))[1]), xytext=(6, 0), textcoords='offset points',
                    va='center', fontsize=9, color=INK2, annotation_clip=False)


def series(data, kind, mq, idx):
    pts = sorted((c, v[idx]) for (k, c, m), v in data.items() if k == kind and m == mq)
    return [p[0] for p in pts], [p[1] for p in pts]


def lines(ax, xs_ys, fmt, log_y=False, ylim0=False):
    labels = []
    for mq, (xs, ys) in xs_ys.items():
        if not xs:
            continue
        ax.plot(xs, ys, color=COLOR[mq], marker=MARKER[mq], markersize=7, linewidth=2,
                markeredgecolor=SURFACE, markeredgewidth=1.5, label=LABEL[mq])
        labels.append((xs[-1], ys[-1], fmt(ys[-1])))
    ax.set_xscale('log')
    if log_y:
        ax.set_yscale('log')
        ax.yaxis.set_minor_formatter(FuncFormatter(lambda *_: ''))
    ax.xaxis.set_major_formatter(FuncFormatter(human))
    ax.xaxis.set_minor_formatter(FuncFormatter(lambda *_: ''))
    ax.set_xlabel('건수(이벤트 수)')
    if ylim0:
        ax.set_ylim(bottom=0)
    # 선 끝 직접 라벨(텍스트는 본문 잉크색, 옆의 마커가 계열을 표시)
    end_labels(ax, labels)


def app_chart(data, app, path):
    fig, axes = plt.subplots(2, 2, figsize=(12, 8.5))
    for col, kind in enumerate('AB'):
        tps = {m: series(data, kind, m, 0) for m in MQS}
        p95 = {m: series(data, kind, m, 1) for m in MQS}
        lines(axes[0][col], tps, lambda v: f'{int(v):,}', ylim0=True)
        axes[0][col].set_title(f'{KIND[kind]} · 처리량', loc='left', fontsize=11)
        axes[0][col].set_ylabel('TPS (초당 처리 건수, 높을수록 좋음)')
        axes[0][col].yaxis.set_major_formatter(FuncFormatter(lambda v, _: f'{int(v):,}'))
        lines(axes[1][col], p95, lambda v: f'{v / 1000:,.1f}s' if v >= 1000 else f'{int(v)}ms', log_y=True)
        axes[1][col].set_title(f'{KIND[kind]} · 지연 p95', loc='left', fontsize=11)
        axes[1][col].set_ylabel('p95 지연(ms, 로그, 낮을수록 좋음)')
        axes[1][col].yaxis.set_major_formatter(FuncFormatter(lambda v, _: f'{int(v):,}'))
    handles, labels = axes[0][0].get_legend_handles_labels()
    fig.legend(handles, labels, loc='upper right', ncol=3, frameon=False)
    fig.suptitle(f'{app}: MQ별 규모에 따른 처리량과 지연', x=0.01, ha='left', fontsize=14)
    fig.tight_layout(rect=(0, 0, 1, 0.95))
    fig.savefig(path, dpi=130)
    plt.close(fig)


def ratio_chart(sp, ne, path):
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.8), sharey=True)
    for col, kind in enumerate('AB'):
        ax = axes[col]
        ax.axhline(1, color=INK2, linewidth=1, linestyle='--')
        r = {}
        for m in MQS:
            pts = sorted((c, ne[(k, c, mm)][0] / sp[(k, c, mm)][0]) for (k, c, mm) in sp
                         if k == kind and mm == m and (k, c, mm) in ne and sp[(k, c, mm)][0] and ne[(k, c, mm)][0])
            r[m] = ([p[0] for p in pts], [p[1] for p in pts])
        lines(ax, r, lambda v: f'{v:.2f}')
        ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f'{v:.1f}'))
        ax.set_title(KIND[kind], loc='left', fontsize=11)
    axes[0].set_ylabel('Nest TPS ÷ Spring TPS\n(1보다 위 = Nest가 빠름)')
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc='upper right', ncol=3, frameon=False)
    fig.suptitle('같은 MQ를 썼을 때 Nest와 Spring의 처리량 비율', x=0.01, ha='left', fontsize=14)
    fig.tight_layout(rect=(0, 0, 1, 0.92))
    fig.savefig(path, dpi=130)
    plt.close(fig)


def tool_chart(k6s, locusts, path):
    """B 방식: 같은 시나리오를 k6와 Locust로 돌렸을 때 처리량. 색=MQ, 실선=k6, 점선=Locust"""
    fig, axes = plt.subplots(1, len(k6s), figsize=(12, 4.8), sharey=True)
    for ax, (app, k6), (_, lc) in zip(axes, k6s, locusts):
        labels = []
        for m in MQS:
            for data, style, tool in ((k6, '-', 'k6'), (lc, '--', 'Locust')):
                xs, ys = series(data, 'B', m, 0)
                if not xs:
                    continue
                ax.plot(xs, ys, color=COLOR[m], marker=MARKER[m], markersize=7, linewidth=2, linestyle=style,
                        markeredgecolor=SURFACE, markeredgewidth=1.5, label=f'{LABEL[m]} · {tool}')
                labels.append((xs[-1], ys[-1], f'{int(ys[-1]):,}'))
        ax.set_xscale('log')
        ax.xaxis.set_major_formatter(FuncFormatter(human))
        ax.xaxis.set_minor_formatter(FuncFormatter(lambda *_: ''))
        ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f'{int(v):,}'))
        ax.set_ylim(bottom=0)
        ax.set_xlabel('건수(요청 수 = 이벤트 수)')
        ax.set_title(app, loc='left', fontsize=11)
        end_labels(ax, labels)
    axes[0].set_ylabel('TPS (초당 처리 건수)')
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc='upper right', ncol=3, frameon=False, fontsize=9)
    fig.suptitle('B 방식 부하 도구 비교: k6(실선) vs Locust(점선), VU(동시 사용자) 100', x=0.01, ha='left', fontsize=14)
    fig.tight_layout(rect=(0, 0, 1, 0.86))
    fig.savefig(path, dpi=130)
    plt.close(fig)


if __name__ == '__main__':
    os.makedirs(OUT, exist_ok=True)
    sp, ne = load(sys.argv[1]), load(sys.argv[2])
    app_chart(sp, 'Spring Boot', os.path.join(OUT, 'spring-mq.png'))
    app_chart(ne, 'NestJS', os.path.join(OUT, 'nest-mq.png'))
    ratio_chart(sp, ne, os.path.join(OUT, 'nest-vs-spring.png'))
    if len(sys.argv) > 4:
        ls, ln = load(sys.argv[3]), load(sys.argv[4])
        tool_chart([('Spring Boot', sp), ('NestJS', ne)], [('Spring Boot', ls), ('NestJS', ln)],
                   os.path.join(OUT, 'k6-vs-locust.png'))
    print('saved:', ', '.join(sorted(os.listdir(OUT))))
