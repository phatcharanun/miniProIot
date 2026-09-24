"""
============================================================
CONSUMER A: ISOLATION FOREST (Anomaly Detection)
Kafka -> Python ML Worker -> InfluxDB -> Grafana
============================================================
หน้าที่:
  1. Subscribe Kafka topic ที่มีข้อมูลจราจร (JSON aggregated)
  2. เก็บ buffer ข้อมูลล่าสุด (rolling window) มาเทรน IsolationForest
  3. หา anomaly score ของข้อมูลล่าสุดที่เข้ามา
  4. เขียนผล (anomaly_score, is_anomaly) กลับเข้า InfluxDB
     เป็น measurement ใหม่ ไม่ปนกับของเดิม -> Grafana เอาไปทำ panel ต่อได้เลย

ติดตั้ง library ก่อนรัน:
    pip install kafka-python scikit-learn requests numpy

รัน:
    python ml_worker_isolation_forest.py
============================================================
"""

import json
import time
from collections import deque

import numpy as np
import requests
from kafka import KafkaConsumer
from sklearn.ensemble import IsolationForest

# ============================================================
# CONFIG
# ============================================================

KAFKA_BROKER = "172.16.2.117:9092"
KAFKA_TOPIC = "traffic-events-6610301001"
CONSUMER_GROUP_ID = "ml-worker-isolation-forest"

INFLUXDB_URL = "http://172.16.2.117:8086"
INFLUXDB_ORG = "e761e698e5720d2f"
INFLUXDB_BUCKET = "mini_project"
INFLUXDB_TOKEN = "ai3V3vxPXNMwE_4aGePni-5uLU7MumFckIpHbYi5_O52LkpH38b9IM4WucD6RoKSm8oBx-Wy8ZOfSITZqrTQ7A=="

# measurement ใหม่ แยกจาก traffic_6610301001 เดิม กัน panel ปนกัน
ML_MEASUREMENT = "ML_6610301001"

# ต้องมีข้อมูลอย่างน้อยเท่านี้ใน buffer ก่อนเริ่มเทรนโมเดล
MIN_BUFFER_SIZE = 5

# สัดส่วนข้อมูลที่ยอมให้เป็น anomaly โดยประมาณ
ANOMALY_CONTAMINATION = 0.02

# เก็บข้อมูลย้อนหลังไว้กี่ record สำหรับเทรน (rolling window)
BUFFER_MAX_SIZE = 200

VEHICLE_TYPES = ["car", "truck", "bus"]


# ============================================================
# แปลง payload JSON -> feature vector สำหรับโมเดล
# ============================================================

def extract_features(payload: dict):
    """
    ดึงตัวเลขที่อยากให้โมเดลมองหา anomaly:
      - total_vehicles รวม
      - avg/max speed ของแต่ละประเภทรถ
    คืนค่าเป็น list ตัวเลข (feature vector) ความยาวคงที่เสมอ
    """
    summary = payload.get("summary", {})
    speed_summary = summary.get("vehicle_speed_summary_by_type", {})

    features = [float(summary.get("total_vehicles", 0) or 0)]

    for vtype in VEHICLE_TYPES:
        stats = speed_summary.get(vtype, {})
        features.append(float(stats.get("avg_speed_kmh", 0) or 0))
        features.append(float(stats.get("max_speed_kmh", 0) or 0))

    return features


# ============================================================
# เขียนผล ML กลับ InfluxDB (Line Protocol)
# ============================================================

def escape_tag(value):
    value = str(value)
    return (
        value.replace("\\", "\\\\")
        .replace(" ", "\\ ")
        .replace(",", "\\,")
        .replace("=", "\\=")
    )


def write_ml_result_to_influxdb(payload, anomaly_score, is_anomaly):
    device = escape_tag(payload.get("device_id", "unknown"))
    location = escape_tag(payload.get("location_name", "unknown"))

    from datetime import datetime
    ts = datetime.fromisoformat(payload["timestamp"])
    timestamp = int(ts.timestamp())

    line = (
        f"{ML_MEASUREMENT},"
        f"device={device},location={location} "
        f"anomaly_score={anomaly_score},"
        f"is_anomaly={1 if is_anomaly else 0}i "
        f"{timestamp}"
    )

    url = f"{INFLUXDB_URL}/api/v2/write"
    params = {"org": INFLUXDB_ORG, "bucket": INFLUXDB_BUCKET, "precision": "s"}
    headers = {
        "Authorization": f"Token {INFLUXDB_TOKEN}",
        "Content-Type": "text/plain; charset=utf-8"
    }

    resp = requests.post(url, params=params, headers=headers, data=line, timeout=10)

    if resp.status_code == 204:
        print(f"[InfluxDB] เขียนผล ML สำเร็จ (anomaly_score={anomaly_score:.3f}, "
              f"is_anomaly={is_anomaly})")
    else:
        print(f"[InfluxDB ERROR] {resp.status_code}: {resp.text}")


# ============================================================
# MAIN LOOP
# ============================================================

def main():
    print("=" * 60)
    print("ML Worker: Isolation Forest (Anomaly Detection)")
    print("=" * 60)
    print(f"Kafka topic : {KAFKA_TOPIC}")
    print(f"InfluxDB    : {INFLUXDB_URL} -> measurement '{ML_MEASUREMENT}'")
    print("=" * 60)

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=CONSUMER_GROUP_ID,
        auto_offset_reset="latest",
        enable_auto_commit=True,
    )

    feature_buffer = deque(maxlen=BUFFER_MAX_SIZE)

    for message in consumer:
        raw_bytes = message.value

        # ค่า value จาก connector เป็น byte[] ดิบ ต้อง decode UTF-8 ก่อนเสมอ
        try:
            payload = json.loads(raw_bytes.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            print(f"[SKIP] decode/parse ไม่ได้: {e}")
            continue

        print(f"\n[RECEIVED] {payload.get('timestamp')} "
              f"total_vehicles={payload.get('summary', {}).get('total_vehicles')}")

        features = extract_features(payload)

        # ใช้ข้อมูลก่อนหน้าเป็น baseline แล้วค่อยตรวจ record ปัจจุบัน
        if len(feature_buffer) < MIN_BUFFER_SIZE:
            feature_buffer.append(features)
            print(f"[BUFFERING] รอข้อมูลอีก {MIN_BUFFER_SIZE - len(feature_buffer)} "
                  f"record ก่อนเริ่มตรวจ anomaly")
            continue

        # เทรนจากข้อมูลก่อนหน้า ไม่รวม record ปัจจุบันใน baseline
        X = np.array(feature_buffer)

        model = IsolationForest(
            n_estimators=200,
            contamination=ANOMALY_CONTAMINATION,
            random_state=42
        )
        model.fit(X)

        # ตรวจ record ล่าสุดที่เพิ่งเข้ามา
        latest = np.array(features).reshape(1, -1)

        # score_samples: ยิ่งค่าน้อย (ติดลบมาก) ยิ่งผิดปกติ
        raw_score = model.score_samples(latest)[0]
        prediction = model.predict(latest)[0]  # -1 = anomaly, 1 = ปกติ

        is_anomaly = (prediction == -1)

        print(f"[ML] anomaly_score={raw_score:.4f}, is_anomaly={is_anomaly}")

        write_ml_result_to_influxdb(payload, raw_score, is_anomaly)
        feature_buffer.append(features)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[STOPPED] หยุดโดยผู้ใช้")
    except Exception as e:
        print(f"\n[FATAL ERROR] {e}")