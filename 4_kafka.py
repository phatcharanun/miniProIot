"""
============================================================
SETUP KAFKA PIPELINE (โค้ดล้วน ไม่ต้องคลิก UI)
============================================================
ทำ 3 อย่างในสคริปต์เดียว:
  1. สร้าง Kafka topic ที่ยังไม่มี ผ่าน Admin API (port 9092)
  2. ลบ connector เดิม (ถ้ามี) แล้วสร้างใหม่ ผ่าน Kafka Connect REST API (port 8083)
  3. เช็คสถานะ connector จนกว่าจะ RUNNING หรือ error

ติดตั้ง library ก่อนรัน (รันครั้งเดียวพอ):
    pip install kafka-python requests

วิธีรัน:
    python setup_kafka_pipeline.py
============================================================
"""

import json
import time
import requests
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from kafka import KafkaConsumer, TopicPartition

# ============================================================
# CONFIG - แก้ตรงนี้ให้ตรงกับของคุณ
# ============================================================

KAFKA_BROKER = "172.16.2.117:9092"          # Kafka broker (data plane)
KAFKA_CONNECT_URL = "http://172.16.2.117:8083"  # Kafka Connect REST API (control plane)

TOPIC_NAME = "traffic-events-6610301001"
TOPIC_PARTITIONS = 1
TOPIC_REPLICATION_FACTOR = 1

CONNECTOR_NAME = "traffic-mqtt-source-6610301001"

CONNECTOR_CONFIG = {
    "name": CONNECTOR_NAME,
    "config": {
        "connector.class": "io.confluent.connect.mqtt.MqttSourceConnector",
        "tasks.max": "1",

        "mqtt.server.uri": "tcp://vernemq1:1883",
        "mqtt.topics": "traffic/aggregated",
        "mqtt.qos": "1",
        "mqtt.clean.session.enabled": "false",
        "mqtt.client.id": f"kafka-connect-{CONNECTOR_NAME}",

        "kafka.topic": TOPIC_NAME,

        # สำคัญ: MQTT connector ส่ง value เป็น byte[] ดิบๆ
        # ถ้าใช้ StringConverter จะได้ "[B@hashcode" (บั๊ก Java toString บน byte array)
        # ต้องใช้ ByteArrayConverter เพื่อเก็บ bytes ตามจริง แล้วค่อย decode UTF-8 ตอนอ่าน
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",

        "confluent.topic.bootstrap.servers": "kafka:29092",
        "confluent.topic.replication.factor": "1"
    }
}


# ============================================================
# STEP 1: สร้าง Kafka Topic (ผ่าน 9092)
# ============================================================

def create_topic():
    print("=" * 60)
    print(f"[1/3] สร้าง Topic: {TOPIC_NAME}")
    print("=" * 60)

    admin = KafkaAdminClient(
        bootstrap_servers=KAFKA_BROKER,
        client_id="setup-script"
    )

    topic = NewTopic(
        name=TOPIC_NAME,
        num_partitions=TOPIC_PARTITIONS,
        replication_factor=TOPIC_REPLICATION_FACTOR
    )

    try:
        admin.create_topics(new_topics=[topic], validate_only=False)
        print(f"[OK] สร้าง topic '{TOPIC_NAME}' สำเร็จ")

    except TopicAlreadyExistsError:
        print(f"[SKIP] topic '{TOPIC_NAME}' มีอยู่แล้ว ไม่ต้องสร้างใหม่")

    except Exception as e:
        print(f"[ERROR] สร้าง topic ไม่สำเร็จ: {e}")
        raise

    finally:
        admin.close()


# ============================================================
# STEP 2: ลบ Connector เดิม (ถ้ามี) แล้วสร้างใหม่ (ผ่าน 8083)
# ============================================================

def recreate_connector():
    print("\n" + "=" * 60)
    print(f"[2/3] Reset Connector: {CONNECTOR_NAME}")
    print("=" * 60)

    # ลบของเดิม ถ้ามี (เพื่อบังคับให้ config ใหม่มีผล)
    delete_url = f"{KAFKA_CONNECT_URL}/connectors/{CONNECTOR_NAME}"
    resp = requests.delete(delete_url, timeout=10)

    if resp.status_code in (204, 404):
        print(f"[OK] เคลียร์ connector เดิมแล้ว (status={resp.status_code})")
    else:
        print(f"[WARN] ลบ connector เดิม status={resp.status_code}: {resp.text}")

    time.sleep(2)  # ให้ Connect cluster sync state ก่อนสร้างใหม่

    # สร้างใหม่
    create_url = f"{KAFKA_CONNECT_URL}/connectors"
    resp = requests.post(
        create_url,
        headers={"Content-Type": "application/json"},
        data=json.dumps(CONNECTOR_CONFIG),
        timeout=10
    )

    if resp.status_code == 201:
        print(f"[OK] สร้าง connector '{CONNECTOR_NAME}' สำเร็จ")
    else:
        print(f"[ERROR] สร้าง connector ไม่สำเร็จ status={resp.status_code}")
        print(resp.text)
        raise RuntimeError("Connector creation failed")


# ============================================================
# STEP 3: เช็คสถานะ Connector จนกว่าจะ RUNNING
# ============================================================

def wait_for_connector_running(timeout_sec=30):
    print("\n" + "=" * 60)
    print(f"[3/3] ตรวจสอบสถานะ Connector (timeout {timeout_sec}s)")
    print("=" * 60)

    status_url = f"{KAFKA_CONNECT_URL}/connectors/{CONNECTOR_NAME}/status"
    start = time.time()

    while time.time() - start < timeout_sec:
        resp = requests.get(status_url, timeout=10)

        if resp.status_code != 200:
            print(f"[WAIT] ยังเรียก status ไม่ได้ status={resp.status_code} รออีก 2 วินาที...")
            time.sleep(2)
            continue

        data = resp.json()
        connector_state = data.get("connector", {}).get("state")
        tasks = data.get("tasks", [])
        task_states = [t.get("state") for t in tasks]

        print(f"[STATUS] connector={connector_state}, tasks={task_states}")

        if connector_state == "RUNNING" and all(s == "RUNNING" for s in task_states):
            print("\n[SUCCESS] Connector และ task ทั้งหมด RUNNING แล้ว")
            print(json.dumps(data, ensure_ascii=False, indent=2))
            return True

        if connector_state == "FAILED" or "FAILED" in task_states:
            print("\n[FAILED] Connector หรือ task ล้มเหลว รายละเอียด:")
            print(json.dumps(data, ensure_ascii=False, indent=2))
            return False

        time.sleep(2)

    print("\n[TIMEOUT] เช็คสถานะไม่ทันภายในเวลาที่กำหนด")
    return False


# ============================================================
# STEP 4: ดึงข้อความล่าสุดใน topic มา decode เป็น JSON ให้ดู
# ============================================================

def verify_latest_message(timeout_sec=15):
    print("\n" + "=" * 60)
    print(f"[4/4] ดึงข้อความล่าสุดจาก topic '{TOPIC_NAME}' มาตรวจสอบ")
    print("=" * 60)

    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="latest",
        enable_auto_commit=False,
        consumer_timeout_ms=timeout_sec * 1000
    )

    tp = TopicPartition(TOPIC_NAME, 0)
    consumer.assign([tp])

    end_offset = consumer.end_offsets([tp])[tp]

    if end_offset == 0:
        print("[EMPTY] topic ยังไม่มีข้อความเลย")
        consumer.close()
        return

    # seek ไปอ่านข้อความสุดท้าย
    consumer.seek(tp, max(end_offset - 1, 0))

    found = False

    for message in consumer:
        raw_bytes = message.value

        print(f"\nOffset      : {message.offset}")
        print(f"Raw bytes   : {raw_bytes[:80]}...")

        try:
            decoded = raw_bytes.decode("utf-8")
            parsed = json.loads(decoded)

            print("[OK] Decode เป็น JSON สำเร็จ:")
            print(json.dumps(parsed, ensure_ascii=False, indent=2))
            found = True

        except UnicodeDecodeError:
            print("[ERROR] decode UTF-8 ไม่ได้ ข้อมูลยังเสียหายอยู่")

        except json.JSONDecodeError:
            print("[WARN] decode UTF-8 ได้ แต่ parse JSON ไม่ได้:")
            print(decoded)

        break  # อ่านแค่ข้อความล่าสุดพอ

    consumer.close()

    if not found:
        print("[TIMEOUT] ไม่ได้รับข้อความภายในเวลาที่กำหนด "
              "(ลองรอให้ครบรอบ 60 วินาทีของ Gateway แล้วรันใหม่)")


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    try:
        create_topic()
        recreate_connector()

        ok = wait_for_connector_running(timeout_sec=30)

        if ok:
            print("\nรอ Gateway aggregate รอบถัดไป (~60 วินาที) "
                  "แล้วจะดึงข้อความล่าสุดมาตรวจสอบให้...")
            time.sleep(65)
            verify_latest_message(timeout_sec=15)

    except Exception as e:
        print(f"\n[FATAL ERROR] สคริปต์หยุดทำงาน: {e}")