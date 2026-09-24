"""
============================================================
CONSUMER B: TRAFFIC DENSITY FORECASTING (% ความหนาแน่นถนน)
Kafka -> Python ML Worker -> InfluxDB -> Grafana
============================================================
หน้าที่:
  1. Subscribe Kafka topic เดียวกับ Consumer A (ข้อมูลจราจรรวมทุก 1 นาที)
  2. เก็บ buffer จำนวนรถย้อนหลัง (time-series)
  3. คำนวณ % ความหนาแน่นถนน ณ ปัจจุบัน เทียบกับความจุถนนสูงสุดที่ตั้งไว้
  4. เทรนโมเดล regression จาก lag features เพื่อ "พยากรณ์ล่วงหน้า 10 นาที"
  5. เขียนผล (current + predicted density %) กลับ InfluxDB
     เป็น measurement ใหม่ ไม่ปนกับของเดิมหรือของ Consumer A

ติดตั้ง library ก่อนรัน (ถ้ายังไม่มีจาก Consumer A):
    pip install kafka-python scikit-learn requests numpy

รัน (แยก terminal จาก ml_worker_isolation_forest.py):
    python ml_worker_density_forecast.py
============================================================
"""

import json
from collections import deque
from datetime import datetime

import numpy as np
import requests
from kafka import KafkaConsumer
from sklearn.ensemble import RandomForestRegressor

# ============================================================
# CONFIG
# ============================================================

KAFKA_BROKER = "172.16.2.117:9092"
KAFKA_TOPIC = "traffic-events-6610301001"
CONSUMER_GROUP_ID = "ml-worker-density-forecast"

INFLUXDB_URL = "http://172.16.2.117:8086"
INFLUXDB_ORG = "e761e698e5720d2f"
INFLUXDB_BUCKET = "mini_project"
INFLUXDB_TOKEN = "ai3V3vxPXNMwE_4aGePni-5uLU7MumFckIpHbYi5_O52LkpH38b9IM4WucD6RoKSm8oBx-Wy8ZOfSITZqrTQ7A=="

# measurement ใหม่ แยกจาก traffic_6610301001 และ ML_6610301001 เดิม
ML_MEASUREMENT = "ML_forecast_6610301001"

# ----------------------------------------------------------
# ตั้งค่าการพยากรณ์ (ปรับได้ตามข้อมูลจริงของถนนจุดนี้)
# ----------------------------------------------------------

# ถนนจุดนี้รองรับรถได้ประมาณกี่คันต่อนาที ณ ระดับ "แน่นเต็มที่ (100%)"
# ปรับค่านี้จากข้อมูลจริงที่สังเกตได้ (เช่น ดูค่าสูงสุดที่เคยวัดได้จาก log ย้อนหลัง)
ROAD_CAPACITY_VEHICLES_PER_MIN = 40

# แต่ละ payload จาก Gateway คือ 1 นาที (interval_sec=60) ดังนั้น
# LOOKBACK_MINUTES = ใช้ข้อมูลย้อนหลังกี่นาทีเป็น input ให้โมเดล
LOOKBACK_MINUTES = 5

# FORECAST_HORIZON_MINUTES = พยากรณ์ล่วงหน้ากี่นาที (โจทย์ต้องการ 10 นาที)
FORECAST_HORIZON_MINUTES = 10

# ต้องมีข้อมูลอย่างน้อยเท่านี้คู่ (X, y) ก่อนเริ่มเทรนโมเดลได้จริง
MIN_TRAINING_SAMPLES = 5

# เก็บประวัติไว้กี่นาทีสำหรับเทรน (rolling window)
BUFFER_MAX_SIZE = 200


# ============================================================
# คำนวณ % ความหนาแน่นถนนจากจำนวนรถ
# ============================================================

def vehicles_to_density_pct(total_vehicles: float) -> float:
    """แปลงจำนวนรถต่อนาที -> % ความหนาแน่น เทียบกับความจุสูงสุดที่ตั้งไว้ (0-100%)"""
    pct = (total_vehicles / ROAD_CAPACITY_VEHICLES_PER_MIN) * 100.0
    return round(max(0.0, min(pct, 100.0)), 1)


# ============================================================
# สร้าง training set แบบ lag features จาก time-series buffer
# ============================================================

def build_training_set(history: list[float]):
    """
    แปลงประวัติจำนวนรถ (list เรียงตามเวลา) เป็นคู่ (X, y) สำหรับเทรน regression
    X = จำนวนรถย้อนหลัง LOOKBACK_MINUTES นาที
    y = จำนวนรถที่ FORECAST_HORIZON_MINUTES นาทีถัดจากจุดสุดท้ายของ X
    """
    X, y = [], []
    window_span = LOOKBACK_MINUTES + FORECAST_HORIZON_MINUTES

    for i in range(len(history) - window_span + 1):
        lookback_window = history[i: i + LOOKBACK_MINUTES]
        target = history[i + window_span - 1]
        X.append(lookback_window)
        y.append(target)

    return np.array(X), np.array(y)


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


def write_density_forecast_to_influxdb(
    payload,
    current_total_vehicles,
    current_density_pct,
    predicted_total_vehicles,
    predicted_density_pct,
):
    device = escape_tag(payload.get("device_id", "unknown"))
    location = escape_tag(payload.get("location_name", "unknown"))

    ts = datetime.fromisoformat(payload["timestamp"])
    timestamp = int(ts.timestamp())

    line = (
        f"{ML_MEASUREMENT},"
        f"device={device},location={location} "
        f"current_total_vehicles={current_total_vehicles},"
        f"current_density_pct={current_density_pct},"
        f"predicted_total_vehicles_10min={predicted_total_vehicles},"
        f"predicted_density_pct_10min={predicted_density_pct} "
        f"{timestamp}"
    )

    url = f"{INFLUXDB_URL}/api/v2/write"
    params = {"org": INFLUXDB_ORG, "bucket": INFLUXDB_BUCKET, "precision": "s"}
    headers = {
        "Authorization": f"Token {INFLUXDB_TOKEN}",
        "Content-Type": "text/plain; charset=utf-8",
    }

    resp = requests.post(url, params=params, headers=headers, data=line, timeout=10)

    if resp.status_code == 204:
        print(
            f"[InfluxDB] เขียนผลสำเร็จ | ปัจจุบัน {current_density_pct}% "
            f"-> พยากรณ์ 10 นาทีข้างหน้า {predicted_density_pct}%"
        )
    else:
        print(f"[InfluxDB ERROR] {resp.status_code}: {resp.text}")


# ============================================================
# MAIN LOOP
# ============================================================

def main():
    print("=" * 60)
    print("ML Worker: Traffic Density Forecast (+10 นาที)")
    print("=" * 60)
    print(f"Kafka topic         : {KAFKA_TOPIC}")
    print(f"Road capacity/min   : {ROAD_CAPACITY_VEHICLES_PER_MIN} คัน = 100%")
    print(f"Lookback / Horizon  : {LOOKBACK_MINUTES} นาที / {FORECAST_HORIZON_MINUTES} นาที")
    print(f"InfluxDB            : {INFLUXDB_URL} -> measurement '{ML_MEASUREMENT}'")
    print("=" * 60)

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=CONSUMER_GROUP_ID,
        auto_offset_reset="latest",
        enable_auto_commit=True,
    )

    # เก็บ (timestamp, total_vehicles) ย้อนหลัง เรียงตามเวลาเข้ามา
    vehicle_history = deque(maxlen=BUFFER_MAX_SIZE)
    min_required = LOOKBACK_MINUTES + FORECAST_HORIZON_MINUTES + MIN_TRAINING_SAMPLES

    for message in consumer:
        raw_bytes = message.value

        try:
            payload = json.loads(raw_bytes.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            print(f"[SKIP] decode/parse ไม่ได้: {e}")
            continue

        total_vehicles = float(
            payload.get("summary", {}).get("total_vehicles", 0) or 0
        )
        current_density_pct = vehicles_to_density_pct(total_vehicles)

        print(
            f"\n[RECEIVED] {payload.get('timestamp')} "
            f"total_vehicles={total_vehicles} "
            f"(density ปัจจุบัน {current_density_pct}%)"
        )

        vehicle_history.append(total_vehicles)

        # ยังมีข้อมูลไม่พอสำหรับเทรนโมเดลพยากรณ์
        if len(vehicle_history) < min_required:
            print(
                f"[BUFFERING] รอข้อมูลอีก {min_required - len(vehicle_history)} "
                f"นาที ก่อนเริ่มพยากรณ์ล่วงหน้า {FORECAST_HORIZON_MINUTES} นาทีได้"
            )
            # ระหว่างรอ ให้เขียนแค่ค่าปัจจุบันลง InfluxDB ก่อน (ยังไม่มีค่าพยากรณ์)
            write_density_forecast_to_influxdb(
                payload, total_vehicles, current_density_pct,
                predicted_total_vehicles=0.0,
                predicted_density_pct=0.0,
            )
            continue

        history_list = list(vehicle_history)

        X_train, y_train = build_training_set(history_list)

        if len(X_train) < MIN_TRAINING_SAMPLES:
            print("[BUFFERING] ข้อมูลสำหรับเทรนยังไม่พอ รอรอบถัดไป")
            continue

        model = RandomForestRegressor(
            n_estimators=200,
            random_state=42,
        )
        model.fit(X_train, y_train)

        # ใช้ LOOKBACK_MINUTES นาทีล่าสุดพยากรณ์ค่าที่ 10 นาทีข้างหน้า
        latest_window = np.array(history_list[-LOOKBACK_MINUTES:]).reshape(1, -1)
        predicted_total_vehicles = float(model.predict(latest_window)[0])
        predicted_total_vehicles = max(0.0, round(predicted_total_vehicles, 1))

        predicted_density_pct = vehicles_to_density_pct(predicted_total_vehicles)

        print(
            f"[FORECAST] อีก {FORECAST_HORIZON_MINUTES} นาทีข้างหน้า "
            f"คาดว่ารถ {predicted_total_vehicles} คัน "
            f"(density {predicted_density_pct}%)"
        )

        write_density_forecast_to_influxdb(
            payload,
            total_vehicles,
            current_density_pct,
            predicted_total_vehicles,
            predicted_density_pct,
        )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[STOPPED] หยุดโดยผู้ใช้")
    except Exception as e:
        print(f"\n[FATAL ERROR] {e}")