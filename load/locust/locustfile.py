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

state = {"runId": None, "sent": 0, "quitting": False}


@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    host = environment.host
    requests.delete(f"{host}/runs")  # 이전 결과 초기화
    r = requests.post(f"{host}/runs", params={"mq": MQ, "expected": COUNT, "size": SIZE, "db": DB})
    state["runId"] = r.json()["runId"]
    state["sent"] = 0
    state["quitting"] = False


@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
    # 소비와 DB 저장이 끝날 때까지 대기 후 결과 출력
    result = {}
    last, stalled = -1, 0
    for _ in range(3000):
        result = requests.get(f"{environment.host}/runs/{state['runId']}").json()
        if result.get("done"):
            break
        # 처리 건수가 15초간 늘지 않으면 유실로 보고 종료
        stalled = stalled + 1 if result.get("processed") == last else 0
        last = result.get("processed")
        if stalled >= 150:
            break
        time.sleep(0.1)
    print(f"RESULT {json.dumps(result)}")


class OrderUser(HttpUser):
    wait_time = constant(0)

    @task
    def order(self):
        i = state["sent"]
        if i >= COUNT:
            # 목표 건수를 모두 보냈으면 종료 (별도 greenlet에서 한 번만 quit 호출)
            if not state["quitting"]:
                state["quitting"] = True
                gevent.spawn(self.environment.runner.quit)
            raise StopUser()
        state["sent"] += 1
        self.client.post(
            f"/orders?runId={state['runId']}",
            json={"userId": (i * 7919) % 1000 + 1, "amount": (i * 37) % 99900 + 100},
            name="/orders",
        )
