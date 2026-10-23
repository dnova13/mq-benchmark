"""시나리오 B (Locust): 요청 1건 = 이벤트 1건

실행 예:
  MQ=kafka COUNT=10000 DB=true locust -f load/locust/locustfile.py \
      --headless -u 100 -r 100 --host http://localhost:3000
"""
import json
import os
import time

import gevent
import requests
from locust import HttpUser, constant, events, task
from locust.exception import StopUser

MQ = os.getenv("MQ", "kafka")
COUNT = int(os.getenv("COUNT", "1000"))
SIZE = os.getenv("SIZE", "256")
DB = os.getenv("DB", "true")
# 대용량용 시간 설정(초). 기본값은 기존 동작과 같다.
WAIT_SEC = float(os.getenv("WAIT_SEC", "300"))
POLL_SEC = float(os.getenv("POLL_SEC", "0.1"))
STALL_SEC = float(os.getenv("STALL_SEC", "15"))

state = {"runId": None, "sent": 0, "completed": 0, "quitting": False}


@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    host = environment.host
    requests.delete(f"{host}/runs")  # 이전 결과 초기화
    r = requests.post(f"{host}/runs", params={"mq": MQ, "expected": COUNT, "size": SIZE, "db": DB})
    state["runId"] = r.json()["runId"]
    state["sent"] = 0
    state["completed"] = 0
    state["quitting"] = False


@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
    # 소비와 DB 저장이 끝날 때까지 대기 후 결과 출력
    url = f"{environment.host}/runs/{state['runId']}"
    result = {}
    last, last_change = -1, time.time()
    deadline = time.time() + WAIT_SEC
    while time.time() < deadline:
        result = requests.get(url).json()
        if result.get("done"):
            break
        # 처리 건수가 STALL_SEC초간 늘지 않으면 유실로 보고 종료
        if result.get("processed") != last:
            last, last_change = result.get("processed"), time.time()
        if time.time() - last_change >= STALL_SEC:
            break
        time.sleep(POLL_SEC)
    # 미완료로 끝나면 지연과 dbCount를 강제로 계산해 받는다
    if not result.get("done"):
        result = requests.get(url, params={"final": "true"}).json()
    print(f"RESULT {json.dumps(result)}")


class OrderUser(HttpUser):
    wait_time = constant(0)

    @task
    def order(self):
        i = state["sent"]
        if i >= COUNT:
            raise StopUser()  # 목표 건수를 다 보냈으면 이 사용자는 멈춘다
        state["sent"] += 1
        self.client.post(
            f"/orders?runId={state['runId']}",
            json={"userId": (i * 7919) % 1000 + 1, "amount": (i * 37) % 99900 + 100},
            name="/orders",
        )
        # 보낸 요청의 응답이 모두 돌아온 뒤에 종료한다. 바로 quit하면 진행 중이던 요청이 끊겨 몇 건이 빠진다.
        state["completed"] += 1
        if state["completed"] >= COUNT and not state["quitting"]:
            state["quitting"] = True
            gevent.spawn(self.environment.runner.quit)
