#!/usr/bin/env python3
"""MQ 개념 문서용 도식 -> docs/mq/img/*.png
사용: python3 docs/mq/diagrams.py
필요: pip install matplotlib, 한글 폰트(NanumGothic 등)
색 규칙: 생산자 = 파랑, 브로커(큐/토픽/스트림) = 주황, 소비자 = 청록. 글자는 본문 잉크색.
"""
import os

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib import font_manager
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'img')
PRODUCER, BROKER, CONSUMER = '#2a78d6', '#eb6834', '#1baf7a'
SURFACE, INK, INK2, MUTED, LINE = '#ffffff', '#0b0b0b', '#52514e', '#8a8984', '#c9c8c3'
BAD = '#e34948'

for name in ('NanumGothic', 'Noto Sans CJK KR', 'Noto Sans KR'):
    if any(name in f.name for f in font_manager.fontManager.ttflist):
        plt.rcParams['font.family'] = name
        break
plt.rcParams['axes.unicode_minus'] = False


def tint(hex_color, amount=0.86):
    """색을 흰색과 섞어 연하게(채움용)"""
    h = hex_color.lstrip('#')
    r, g, b = (int(h[i:i + 2], 16) for i in (0, 2, 4))
    return '#%02x%02x%02x' % tuple(int(c + (255 - c) * amount) for c in (r, g, b))


def canvas(w, h, title=None):
    fig, ax = plt.subplots(figsize=(w, h))
    fig.patch.set_facecolor(SURFACE)
    ax.set_xlim(0, w * 10)
    ax.set_ylim(0, h * 10)
    ax.axis('off')
    if title:
        ax.text(2, h * 10 - 2.5, title, fontsize=15, fontweight='bold', color=INK, va='top')
    return fig, ax


def box(ax, x, y, w, h, text, color=None, size=11, sub=None, bold=False, fill=None, dashed=False):
    edge = color or LINE
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle='round,pad=0,rounding_size=1.2',
                                facecolor=fill if fill is not None else (tint(color) if color else SURFACE),
                                edgecolor=edge, linewidth=1.6, linestyle='--' if dashed else '-'))
    cy = y + h / 2 + (1.1 if sub else 0)
    ax.text(x + w / 2, cy, text, ha='center', va='center', fontsize=size, color=INK,
            fontweight='bold' if bold else 'normal')
    if sub:
        ax.text(x + w / 2, y + h / 2 - 1.6, sub, ha='center', va='center', fontsize=size - 2.5, color=INK2)


def arrow(ax, p1, p2, text=None, color=INK2, both=False, dashed=False, text_offset=(0, 1.2), size=9.5, rad=0.0):
    ax.add_patch(FancyArrowPatch(p1, p2, arrowstyle='<|-|>' if both else '-|>', mutation_scale=13,
                                 color=color, linewidth=1.5, linestyle='--' if dashed else '-',
                                 connectionstyle=f'arc3,rad={rad}', shrinkA=2, shrinkB=2))
    if text:
        mx, my = (p1[0] + p2[0]) / 2 + text_offset[0], (p1[1] + p2[1]) / 2 + text_offset[1]
        ax.text(mx, my, text, ha='center', va='bottom', fontsize=size, color=INK2)


def note(ax, x, y, text, size=10, color=INK2, ha='left'):
    ax.text(x, y, text, fontsize=size, color=color, ha=ha, va='top', linespacing=1.5)


def msg(ax, x, y, label, color=BROKER, w=4.2, h=3.2, size=9):
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle='round,pad=0,rounding_size=0.5',
                                facecolor=SURFACE, edgecolor=color, linewidth=1.3))
    ax.text(x + w / 2, y + h / 2, label, ha='center', va='center', fontsize=size, color=INK)


def legend(ax, x, y):
    for i, (c, t) in enumerate(((PRODUCER, '생산자(Producer)'), (BROKER, '브로커(MQ)'), (CONSUMER, '소비자(Consumer)'))):
        ax.add_patch(FancyBboxPatch((x + i * 22, y), 2.4, 2.4, boxstyle='round,pad=0,rounding_size=0.4',
                                    facecolor=tint(c), edgecolor=c, linewidth=1.4))
        ax.text(x + i * 22 + 3.4, y + 1.2, t, fontsize=9.5, color=INK2, va='center')


def save(fig, name):
    os.makedirs(OUT, exist_ok=True)
    fig.savefig(os.path.join(OUT, name), dpi=130, facecolor=SURFACE, bbox_inches='tight', pad_inches=0.25)
    plt.close(fig)


# ---------------------------------------------------------------- 1. 동기 호출 vs MQ
def sync_vs_async():
    fig, ax = canvas(13, 9.4, '동기 호출과 MQ(비동기)의 차이')
    # 위: 동기
    note(ax, 2, 81, '① 동기 호출: 주문 서비스가 다른 서비스를 차례로 부르고 응답을 기다린다', 11.5, INK)
    box(ax, 3, 60, 18, 10, '주문 서비스', PRODUCER, bold=True)
    for i, (t, s) in enumerate((('결제', '200ms'), ('재고', '100ms'), ('알림', '1초 · 장애 중'))):
        x = 33 + i * 32
        box(ax, x, 60, 18, 10, t + ' 서비스', LINE, sub=s, fill=SURFACE)
        arrow(ax, (x - 12 if i == 0 else x - 14, 65), (x, 65), '호출·대기')
    ax.add_patch(FancyBboxPatch((97, 60), 18, 10, boxstyle='round,pad=0,rounding_size=1.2',
                                facecolor='none', edgecolor=BAD, linewidth=2))
    note(ax, 3, 56, '· 응답 시간 = 모든 호출의 합(1.3초+)   · 알림 서비스 하나가 죽으면 주문도 실패   · 주문 서비스가 세 서비스를 모두 알아야 한다', 10)

    # 아래: MQ
    ax.plot([2, 128], [46, 46], color=LINE, linewidth=1)
    note(ax, 2, 42, '② MQ: 주문 서비스는 "주문 생성" 이벤트만 넣고 바로 응답한다. 각 서비스는 MQ에서 꺼내 자기 속도로 처리한다', 11.5, INK)
    box(ax, 3, 16, 18, 14, '주문 서비스', PRODUCER, bold=True, sub='응답 20ms')
    box(ax, 36, 13, 34, 20, '', BROKER)
    ax.text(53, 29.5, 'MQ (큐 / 토픽)', ha='center', fontsize=11, fontweight='bold', color=INK)
    for i in range(4):
        msg(ax, 38.5 + i * 7.8, 17, f'주문{i + 1}', w=6.4)
    arrow(ax, (21, 23), (36, 23), '발행', color=PRODUCER)
    for i, (t, extra) in enumerate((('결제 서비스', ''), ('재고 서비스', ''), ('알림 서비스', '느려도·죽어도\n주문은 성공'))):
        y = 34 - i * 11
        box(ax, 90, y - 4, 22, 8, t, CONSUMER)
        arrow(ax, (70, 23), (90, y), color=CONSUMER)
        if extra:
            note(ax, 113.5, y + 3, extra, 9.5)
    note(ax, 3, 7, '· 응답이 빠르다   · 소비자가 잠시 죽어도 메시지는 MQ에 남아 나중에 처리된다   · 새 서비스는 MQ만 구독하면 된다(주문 서비스 수정 없음)', 10)
    save(fig, 'mq-sync-vs-async.png')


# ---------------------------------------------------------------- 2. 구성 요소
def components():
    fig, ax = canvas(13, 6, 'MQ의 구성 요소와 메시지 흐름')
    for i, t in enumerate(('주문 API', '배치 작업')):
        box(ax, 3, 30 - i * 14, 20, 9, t, PRODUCER, sub='Producer')
        arrow(ax, (23, 34.5 - i * 14), (40, 27), '① 발행(publish)' if i == 0 else None, color=PRODUCER, text_offset=(-3, 3))
    box(ax, 40, 10, 46, 34, '', BROKER)
    ax.text(63, 40, '브로커(Broker) — 메시지를 받아 저장하고 전달하는 서버', ha='center', fontsize=10.5, fontweight='bold', color=INK)
    box(ax, 44, 17, 38, 16, '', BROKER, fill=SURFACE)
    ax.text(63, 30, '큐 / 토픽 / 스트림', ha='center', fontsize=10, color=INK2)
    for i in range(5):
        msg(ax, 46.5 + i * 7, 19.5, f'm{i + 1}', w=5.5, h=6)
    note(ax, 45, 15.5, '② 저장: 디스크·메모리에 보관(영속성)', 9.5)
    for i, t in enumerate(('결제 처리', '알림 발송')):
        box(ax, 105, 30 - i * 14, 20, 9, t, CONSUMER, sub='Consumer')
        arrow(ax, (86, 27), (105, 34.5 - i * 14), '③ 전달(push/pull)' if i == 0 else None, color=CONSUMER, text_offset=(-1, 3.5))
    arrow(ax, (105, 18.5), (86, 14), '④ ack(처리 완료)', color=INK2, dashed=True, text_offset=(0, -4.5))
    note(ax, 3, 6.5, 'ack를 받아야 "처리 끝"으로 본다. ack 전에 소비자가 죽으면 메시지는 다시 전달된다(at-least-once).', 10)
    legend(ax, 70, 52)
    save(fig, 'mq-components.png')


# ---------------------------------------------------------------- 3. 전달 방식
def patterns():
    fig, ax = canvas(13, 6.2, '두 가지 전달 방식')
    # 왼쪽: 점대점
    note(ax, 2, 55, '① 점대점(작업 분배): 한 메시지는 소비자 하나만 받는다', 11, INK)
    box(ax, 2, 22, 11, 9, '생산자', PRODUCER)
    box(ax, 19, 18, 24, 17, '', BROKER)
    ax.text(31, 32, '큐', ha='center', fontsize=10, fontweight='bold')
    for i in range(4):
        msg(ax, 20.5 + i * 5.5, 22, f'm{i + 1}', w=4.6, h=6, size=8.5)
    arrow(ax, (13, 26.5), (19, 26.5), color=PRODUCER)
    box(ax, 50, 33, 12, 9, '소비자 1', CONSUMER)
    box(ax, 50, 12, 12, 9, '소비자 2', CONSUMER)
    arrow(ax, (43, 28), (50, 37.5), color=CONSUMER)
    arrow(ax, (43, 24), (50, 16.5), color=CONSUMER)
    note(ax, 50, 46.5, 'm1, m3', 9.5)
    note(ax, 50, 10.5, 'm2, m4', 9.5)
    note(ax, 2, 8, '예: 주문 처리 워커 여러 대가 일을 나눠 맡는다\nRabbitMQ 큐, Redis Streams 같은 그룹, Kafka 같은 그룹', 9.5)

    ax.plot([67, 67], [5, 55], color=LINE, linewidth=1)
    # 오른쪽: 발행/구독
    note(ax, 70, 55, '② 발행/구독(팬아웃): 구독자마다 모든 메시지를 받는다', 11, INK)
    box(ax, 70, 22, 11, 9, '생산자', PRODUCER)
    box(ax, 87, 18, 17, 17, '', BROKER)
    ax.text(95.5, 30, '토픽 /\nexchange', ha='center', va='center', fontsize=10, fontweight='bold')
    arrow(ax, (81, 26.5), (87, 26.5), color=PRODUCER)
    for i, t in enumerate(('정산 서비스', '알림 서비스', '분석 서비스')):
        y = 37 - i * 12
        box(ax, 111, y, 16, 8, t, CONSUMER)
        arrow(ax, (104, 26.5), (111, y + 4), color=CONSUMER)
        note(ax, 111, y - 0.3, 'm1, m2, m3, m4', 8.5)
    note(ax, 70, 8, '예: 주문 생성 이벤트를 여러 서비스가 각자 받는다\nRabbitMQ fanout, Kafka 다른 그룹, Redis Streams 다른 그룹', 9.5)
    save(fig, 'mq-patterns.png')


# ---------------------------------------------------------------- 4. Redis Streams
def redis_streams():
    fig, ax = canvas(13, 7.2, 'Redis Streams 구조')
    box(ax, 2, 46, 14, 9, '생산자', PRODUCER, sub='XADD')
    box(ax, 22, 40, 104, 21, '', BROKER)
    ax.text(24, 58, '스트림  bench:stream  (지우기 전까지 남는 로그, 오른쪽이 최신)', fontsize=10.5, fontweight='bold', va='center')
    for i in range(6):
        msg(ax, 25 + i * 16.5, 43, f'ID …00{i + 1}-0\norderId={i + 1}', w=14.5, h=10, size=8.5)
    arrow(ax, (16, 50.5), (25, 50.5), color=PRODUCER)
    note(ax, 24, 39.3, 'ID = 밀리초 타임스탬프-순번(자동 생성)', 9)
    cx = [25 + i * 16.5 + 7.25 for i in range(6)]  # 엔트리 중심 x

    # 그룹 g1: 소비자 c1(001~002), c2(003~005)
    box(ax, 2, 2, 94, 31, '', CONSUMER, fill=tint(CONSUMER, 0.93), dashed=True)
    box(ax, 22, 15, 20, 11, '소비자 c1', CONSUMER, sub='받음: 001, 002')
    box(ax, 50, 15, 22, 11, '소비자 c2', CONSUMER, sub='받음: 003, 004, 005')
    arrow(ax, (cx[0], 43), (32, 26), 'XREADGROUP', color=CONSUMER, text_offset=(9, -1))
    arrow(ax, (cx[3], 43), (61, 26), color=CONSUMER)
    box(ax, 77, 12, 17, 17, '', LINE, fill=SURFACE)
    ax.text(85.5, 26.5, 'PEL', ha='center', fontsize=9.5, fontweight='bold')
    note(ax, 79, 24.5, '002  c1\n005  c2', 9.5)
    note(ax, 79, 18.2, 'ack 대기 목록', 8.5)
    arrow(ax, (72, 20.5), (77, 20.5), dashed=True)
    ax.text(4, 8.5, '컨슈머 그룹 g1  — 같은 그룹은 나눠 받는다', fontsize=10.5, fontweight='bold', va='center')
    note(ax, 4, 6.2, 'XACK한 001, 003, 004는 PEL에서 빠졌다. ack 전에 소비자가 죽으면 PEL에 남고, XAUTOCLAIM으로 다른 소비자가 가져간다', 8.5)

    # 그룹 g2
    box(ax, 100, 2, 26, 31, '', CONSUMER, fill=tint(CONSUMER, 0.93), dashed=True)
    box(ax, 103, 15, 20, 11, '소비자 x1', CONSUMER, sub='받음: 001~006')
    arrow(ax, (cx[5], 43), (113, 26), color=CONSUMER)
    ax.text(102, 8.5, '컨슈머 그룹 g2', fontsize=10.5, fontweight='bold', va='center')
    note(ax, 102, 6.2, '그룹이 다르면 전부 따로 받는다', 8.5)
    save(fig, 'redis-streams.png')


# ---------------------------------------------------------------- 5. RabbitMQ
def rabbitmq():
    fig, ax = canvas(13, 7.4, 'RabbitMQ 구조: Exchange → Binding → Queue')
    box(ax, 2, 44, 14, 10, '생산자', PRODUCER, sub='routing key 지정')
    box(ax, 22, 3, 70, 60, '', BROKER, fill=tint(BROKER, 0.94))
    ax.text(24, 60, '브로커(RabbitMQ)', fontsize=10.5, fontweight='bold', va='center')

    # direct
    box(ax, 26, 41, 18, 14, 'direct\nexchange', BROKER, size=10)
    arrow(ax, (16, 49), (26, 49), 'key=order', color=PRODUCER, text_offset=(0, 1))
    box(ax, 62, 48, 26, 9, '', BROKER, fill=SURFACE)
    ax.text(64, 52.5, '큐 orders', fontsize=9.5, va='center')
    for i in range(3):
        msg(ax, 74 + i * 4.3, 49.5, '', w=3.6, h=6)
    box(ax, 62, 35, 26, 9, '', BROKER, fill=SURFACE)
    ax.text(64, 39.5, '큐 payments', fontsize=9.5, va='center')
    arrow(ax, (44, 50), (62, 52.5), 'binding: order', text_offset=(0, 0.8), size=8.5)
    arrow(ax, (44, 45), (62, 39.5), 'binding: payment', text_offset=(0, -3.8), size=8.5, color=MUTED)
    note(ax, 26, 38.5, 'direct: routing key가 같은\nbinding의 큐로만', 9)

    # fanout
    box(ax, 26, 11, 18, 14, 'fanout\nexchange', BROKER, size=10)
    arrow(ax, (9, 44), (26, 18), color=PRODUCER, rad=0.25)
    ax.text(4, 27, 'event=\nuser-signed-up', fontsize=8.5, color=INK2, va='center')
    box(ax, 62, 19, 26, 8, '큐 email', BROKER, fill=SURFACE, size=9.5)
    box(ax, 62, 7, 26, 8, '큐 sms', BROKER, fill=SURFACE, size=9.5)
    arrow(ax, (44, 20), (62, 23))
    arrow(ax, (44, 16), (62, 11))
    note(ax, 26, 8.5, 'fanout: 묶인 모든 큐에 복사', 9)

    # 소비자
    box(ax, 99, 52, 25, 9, '소비자 A1', CONSUMER, size=10)
    box(ax, 99, 41, 25, 9, '소비자 A2', CONSUMER, size=10)
    arrow(ax, (88, 52.5), (99, 56.5), color=CONSUMER)
    arrow(ax, (88, 52.5), (99, 45.5), color=CONSUMER)
    note(ax, 99, 39, '같은 큐 = 나눠 받음(라운드 로빈)\nprefetch 수만큼 ack 전 메시지를 받음', 9)
    box(ax, 99, 14, 25, 15, '', LINE, fill=SURFACE)
    note(ax, 101, 27, 'ack  → 큐에서 삭제\n\nnack / 연결 끊김\n→ 큐로 복귀(redelivered)', 9, INK)
    save(fig, 'rabbitmq.png')


# ---------------------------------------------------------------- 6. Kafka
def kafka():
    fig, ax = canvas(13, 8, 'Kafka 구조: 토픽 · 파티션 · 오프셋 · 컨슈머 그룹')
    box(ax, 2, 56, 15, 11, '생산자', PRODUCER, sub='key=userId')
    box(ax, 23, 34, 104, 39, '', BROKER, fill=tint(BROKER, 0.94))
    ax.text(25, 70, '토픽 orders  (파티션 3개 · 읽어도 지워지지 않음 · 보존 기간이 지나면 삭제)', fontsize=10.5, fontweight='bold', va='center')
    rows = {0: 6, 1: 5, 2: 4}
    committed = {1: 2}  # 그룹 A가 파티션 1은 오프셋 2까지 처리(다음에 읽을 위치 = 2)
    for p, n in rows.items():
        y = 58 - p * 9
        ax.text(26, y + 3, f'파티션 {p}', fontsize=9.5, va='center')
        for o in range(n):
            unread = p in committed and o >= committed[p]
            msg(ax, 38 + o * 8, y, str(o), color=BAD if unread else BROKER, w=7, h=6, size=9)
    ax.text(38 + 6 * 8 + 1, 61, '← 새 메시지는 끝에 추가', fontsize=8.5, color=MUTED, va='center')
    ax.text(38 + 5 * 8 + 1, 52, '← 그룹 A는 파티션 1을 오프셋 1까지 처리(커밋 = 2)\n    빨간 칸 3개 = lag(아직 안 읽은 수)', fontsize=8.5, color=BAD, va='center')
    arrow(ax, (17, 61.5), (23, 61.5), color=PRODUCER)
    ax.text(5, 54, 'hash(key) % 3\n→ 파티션 선택', fontsize=8.5, color=INK2, va='top')
    note(ax, 26, 38.5, '칸 안의 숫자 = 오프셋. 같은 key는 항상 같은 파티션 → 순서는 파티션 안에서만 보장', 9)

    # 그룹 A
    box(ax, 2, 2, 62, 26, '', CONSUMER, fill=tint(CONSUMER, 0.93), dashed=True)
    box(ax, 6, 12, 24, 11, '소비자 a1', CONSUMER, sub='파티션 0')
    box(ax, 36, 12, 24, 11, '소비자 a2', CONSUMER, sub='파티션 1, 2')
    arrow(ax, (25, 34), (18, 23), color=CONSUMER)
    arrow(ax, (50, 34), (48, 23), color=CONSUMER)
    ax.text(4, 8, '컨슈머 그룹 A  — 파티션을 나눠 맡는다', fontsize=10.5, fontweight='bold', va='center')
    note(ax, 4, 5.8, '파티션 하나는 그룹 안의 소비자 하나만 읽는다(소비자 > 파티션이면 남는 소비자는 논다)', 8.5)

    # 그룹 B
    box(ax, 70, 2, 57, 26, '', CONSUMER, fill=tint(CONSUMER, 0.93), dashed=True)
    box(ax, 84, 12, 30, 11, '소비자 b1', CONSUMER, sub='파티션 0, 1, 2 전부')
    arrow(ax, (100, 34), (99, 23), color=CONSUMER)
    ax.text(72, 8, '컨슈머 그룹 B  — 다른 서비스', fontsize=10.5, fontweight='bold', va='center')
    note(ax, 72, 5.8, '그룹마다 커밋 오프셋을 따로 저장 → 같은 데이터를 각자 읽는다', 8.5)
    save(fig, 'kafka.png')


if __name__ == '__main__':
    for f in (sync_vs_async, components, patterns, redis_streams, rabbitmq, kafka):
        f()
    print('saved:', ', '.join(sorted(os.listdir(OUT))))
