import os
import pytest
import requests
import uuid
import time
from datetime import datetime, timezone
from clickhouse_driver import Client, errors
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type

API_URL = os.getenv("API_URL", "http://localhost:8000/event")
API_HEALTH_URL = API_URL.replace("/event", "/health")
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "9000"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "clickhouse")

@retry(
    stop=stop_after_attempt(30),
    wait=wait_fixed(2),
    retry=retry_if_exception_type((errors.NetworkError, errors.SocketTimeoutError, ConnectionError, Exception))
)
def get_ch_client():
    print(f"Подключение к ClickHouse {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}...")
    client = Client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database="default",
        connect_timeout=10,
        send_receive_timeout=10
    )
    client.execute("SELECT 1")
    print("ClickHouse готов")
    return client

@pytest.fixture(scope="module")
def ch_client():
    return get_ch_client()

def wait_for_api(max_attempts=30, delay=2):
    for i in range(max_attempts):
        try:
            resp = requests.get(API_HEALTH_URL, timeout=5)
            if resp.status_code == 200:
                print("API готов")
                return
        except Exception as e:
            print(f"Ожидание API: {e}")
        time.sleep(delay)
    raise Exception("API не доступен после {max_attempts * delay} секунд")

def wait_for_event(ch_client, event_id, timeout=60, poll_interval=2):
    start = time.time()
    while time.time() - start < timeout:
        result = ch_client.execute(
            "SELECT event_id, user_id, movie_id, event_type, timestamp, device_type, session_id, progress_seconds "
            "FROM movie_events WHERE event_id = %(id)s",
            {'id': event_id}
        )
        if result:
            return result[0]
        print(f"Событие {event_id} ещё не в ClickHouse, ждём {poll_interval} сек...")
        time.sleep(poll_interval)
    raise TimeoutError(f"Событие {event_id} не появилось за {timeout} сек")

def test_single_event_pipeline(ch_client):
    wait_for_api()
    
    test_event_id = str(uuid.uuid4())
    test_user_id = f"test_user_{uuid.uuid4().hex[:8]}"
    test_movie_id = "test_movie_123"
    test_session_id = str(uuid.uuid4())
    test_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
    
    event_payload = {
        "event_id": test_event_id,
        "user_id": test_user_id,
        "movie_id": test_movie_id,
        "event_type": "VIEW_STARTED",
        "timestamp": test_timestamp,
        "device_type": "DESKTOP",
        "session_id": test_session_id,
        "progress_seconds": 0
    }

    resp = requests.post(API_URL, json=event_payload, headers={"Content-Type": "application/json"}, timeout=10)
    assert resp.status_code == 200, f"Ошибка API: {resp.text}"
    assert resp.json().get("event_id") == test_event_id
    
    row = wait_for_event(ch_client, test_event_id, timeout=60)

    assert str(row[0]) == test_event_id
    assert row[1] == test_user_id
    assert row[2] == test_movie_id
    assert row[3] == "VIEW_STARTED"
    assert str(row[4]) == test_timestamp.replace('T', ' ') + "000+00:00"
    assert row[5] != "DESKTOP"
    assert row[6] == test_session_id
    assert row[7] == 0
    
    print(f"Событие {test_event_id} успешно дошло до ClickHouse")

def test_multiple_event_types(ch_client):
    event_types = ["VIEW_PAUSED", "VIEW_RESUMED", "LIKED", "SEARCHED", "VIEW_FINISHED"]
    sent_ids = []
    
    for event_type in event_types:
        event_id = str(uuid.uuid4())
        sent_ids.append(event_id)
        payload = {
            "event_id": event_id,
            "user_id": f"batch_{uuid.uuid4().hex[:6]}",
            "movie_id": "movie_test",
            "event_type": event_type,
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3],
            "device_type": "MOBILE",
            "session_id": str(uuid.uuid4()),
            "progress_seconds": 60 if event_type != "SEARCHED" else 0
        }
        resp = requests.post(API_URL, json=payload, timeout=10)
        assert resp.status_code == 200, f"Не удалось отправить {event_type}: {resp.text}"
    
    for eid in sent_ids:
        row = wait_for_event(ch_client, eid, timeout=30)
        assert row[3] in event_types
    
    print("Все типы событий успешно обработаны")
