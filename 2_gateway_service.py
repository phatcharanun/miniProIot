#เพิ่ม
from kafka import KafkaProducer

import json
import time
import threading
import paho.mqtt.client as mqtt
from flask import Flask, request, jsonify

#เพิ่มkafka
KAFKA_BROKER = "172.16.46.117:9092"
KAFKA_TOPIC = "traffic-events"
# ============================================================
# GATEWAY SERVICE
# รับข้อมูลจาก YOLO
# รวมข้อมูล 15 วินาที -> 60 วินาที
# แล้ว Publish ไป MQTT
# ============================================================

FIELD_ID = "6610301001"

GATEWAY_HOST = "0.0.0.0"
GATEWAY_PORT = 5000

MQTT_BROKER = "172.16.2.117"
MQTT_PORT = 1883
MQTT_TOPIC = "traffic/aggregated"

AGGREGATION_WINDOW_SEC = 60

VEHICLE_TYPES = [
    "car",
    "truck",
    "bus"
]

app = Flask(__name__)

buffer = []
buffer_lock = threading.Lock()


# ============================================================
# MQTT
# ============================================================

mqtt_client = mqtt.Client()


def on_connect(client, userdata, flags, rc):

    if rc == 0:
        print(
            f"[MQTT] Connected to "
            f"{MQTT_BROKER}:{MQTT_PORT}"
        )
    else:
        print(
            f"[MQTT] Connection failed rc={rc}"
        )


mqtt_client.on_connect = on_connect


def start_mqtt():

    try:

        mqtt_client.connect(
            MQTT_BROKER,
            MQTT_PORT,
            60
        )

        mqtt_client.loop_start()

    except Exception as e:

        print(
            f"[MQTT ERROR] {e}"
        )


# ============================================================
# AGGREGATION
# ============================================================

def aggregate_payloads(payloads):

    counts = {
        vehicle_type: 0
        for vehicle_type in VEHICLE_TYPES
    }

    speed_sum = {
        vehicle_type: 0.0
        for vehicle_type in VEHICLE_TYPES
    }

    speed_count = {
        vehicle_type: 0
        for vehicle_type in VEHICLE_TYPES
    }

    max_speed = {
        vehicle_type: 0.0
        for vehicle_type in VEHICLE_TYPES
    }

    min_speed = {
        vehicle_type: None
        for vehicle_type in VEHICLE_TYPES
    }

    last_device_id = "unknown"
    last_location = "unknown"

    for payload in payloads:

        last_device_id = payload.get(
            "device_id",
            last_device_id
        )

        last_location = payload.get(
            "location_name",
            last_location
        )

        summary = payload.get(
            "summary",
            {}
        )

        speed_summary = summary.get(
            "vehicle_speed_by_type",
            {}
        )

        for vehicle_type in VEHICLE_TYPES:

            stats = speed_summary.get(
                vehicle_type,
                {}
            )

            total = int(
                stats.get(
                    "total_vehicles",
                    0
                )
                or 0
            )

            avg_speed = float(
                stats.get(
                    "avg_speed_kmh",
                    0
                )
                or 0
            )

            current_max = float(
                stats.get(
                    "max_speed_kmh",
                    0
                )
                or 0
            )

            current_min = float(
                stats.get(
                    "min_speed_kmh",
                    0
                )
                or 0
            )

            counts[vehicle_type] += total

            if total > 0:

                speed_sum[vehicle_type] += (
                    avg_speed * total
                )

                speed_count[vehicle_type] += total

                if current_max > max_speed[
                    vehicle_type
                ]:
                    max_speed[vehicle_type] = (
                        current_max
                    )

                if current_min > 0:

                    if (
                        min_speed[vehicle_type]
                        is None
                    ):
                        min_speed[
                            vehicle_type
                        ] = current_min

                    else:
                        min_speed[
                            vehicle_type
                        ] = min(
                            min_speed[
                                vehicle_type
                            ],
                            current_min
                        )

    result_speed_summary = {}

    for vehicle_type in VEHICLE_TYPES:

        count = speed_count[
            vehicle_type
        ]

        if count > 0:

            avg = round(
                speed_sum[vehicle_type]
                / count,
                1
            )

            minimum = round(
                min_speed[vehicle_type],
                1
            )

            maximum = round(
                max_speed[vehicle_type],
                1
            )

        else:

            avg = 0.0
            minimum = 0.0
            maximum = 0.0

        result_speed_summary[
            vehicle_type
        ] = {
            "total_vehicles":
                counts[vehicle_type],

            "avg_speed_kmh":
                avg,

            "max_speed_kmh":
                maximum,

            "min_speed_kmh":
                minimum
        }

    from datetime import datetime, timezone, timedelta

    timestamp = datetime.now(
        timezone(
            timedelta(hours=7)
        )
    ).isoformat(
        timespec="seconds"
    )

    return {
        "id": FIELD_ID,

        "timestamp": timestamp,

        "device_id":
            last_device_id,

        "location_name":
            last_location,

        "interval_sec":
            AGGREGATION_WINDOW_SEC,

        "summary": {

            "total_vehicles":
                sum(counts.values()),

            "vehicle_counts_by_type":
                counts,

            "vehicle_speed_by_type":
                result_speed_summary
        }
    }


# ============================================================
# PUBLISH EVERY 60 SECONDS
# ============================================================

def aggregation_loop():

    while True:

        time.sleep(
            AGGREGATION_WINDOW_SEC
        )

        with buffer_lock:

            payloads = buffer.copy()
            buffer.clear()

        if not payloads:

            print(
                "[GATEWAY] "
                "ไม่มีข้อมูลในช่วง 60 วินาที"
            )

            continue

        aggregated = aggregate_payloads(
            payloads
        )

        print(
            "\n=========================================="
        )

        print(
            "[GATEWAY] Aggregated 60 seconds"
        )

        print(
            json.dumps(
                aggregated,
                ensure_ascii=False,
                indent=2
            )
        )

        print(
            "=========================================="
        )

        try:

            result = mqtt_client.publish(
                MQTT_TOPIC,
                json.dumps(
                    aggregated,
                    ensure_ascii=False
                ),
                qos=1
            )

            if result.rc == mqtt.MQTT_ERR_SUCCESS:

                print(
                    "[GATEWAY -> MQTT] SUCCESS"
                )

                print(
                    f"Topic: {MQTT_TOPIC}"
                )

                print(
                    f"Field ID: {FIELD_ID}"
                )

            else:

                print(
                    f"[GATEWAY -> MQTT ERROR] "
                    f"rc={result.rc}"
                )

        except Exception as e:

            print(
                f"[GATEWAY -> MQTT ERROR] {e}"
            )


# ============================================================
# HTTP ENDPOINT
# ============================================================

@app.route(
    "/traffic-events",
    methods=["POST"]
)
def receive_traffic():

    try:

        payload = request.get_json(
            force=True
        )

        if not isinstance(
            payload,
            dict
        ):
            return jsonify({
                "status": "rejected",
                "reason":
                    "Payload ต้องเป็น JSON object"
            }), 400

        # บังคับ Field ID
        payload["id"] = FIELD_ID

        required_fields = [
            "timestamp",
            "device_id",
            "location_name",
            "interval_sec",
            "summary"
        ]

        for field in required_fields:

            if field not in payload:

                return jsonify({
                    "status": "rejected",
                    "reason":
                        f"ไม่มี field: {field}"
                }), 400

        with buffer_lock:

            buffer.append(
                payload
            )

            current_size = len(
                buffer
            )

        print(
            "\n[GATEWAY] Received from YOLO"
        )

        print(
            f"Field ID: {FIELD_ID}"
        )

        print(
            f"Buffered payloads: "
            f"{current_size}"
        )

        return jsonify({
            "status": "accepted",
            "field_id": FIELD_ID,
            "buffered_payloads":
                current_size
        }), 201

    except Exception as e:

        print(
            f"[GATEWAY ERROR] {e}"
        )

        return jsonify({
            "status": "error",
            "reason": str(e)
        }), 500


# ============================================================
# HEALTH CHECK
# ============================================================

@app.route(
    "/health",
    methods=["GET"]
)
def health():

    with buffer_lock:
        current_size = len(buffer)

    return jsonify({
        "status": "ok",
        "field_id": FIELD_ID,
        "buffered_payloads":
            current_size,
        "mqtt_topic":
            MQTT_TOPIC
    })


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    print("=" * 60)
    print("Traffic Gateway Service")
    print("=" * 60)

    print(
        f"Field ID       : {FIELD_ID}"
    )

    print(
        f"HTTP Server    : "
        f"http://{GATEWAY_HOST}:{GATEWAY_PORT}"
    )

    print(
        f"MQTT Broker    : "
        f"{MQTT_BROKER}:{MQTT_PORT}"
    )

    print(
        f"MQTT Topic     : "
        f"{MQTT_TOPIC}"
    )

    print(
        f"Aggregation    : "
        f"{AGGREGATION_WINDOW_SEC} seconds"
    )

    print("=" * 60)

    # MQTT
    start_mqtt()

    # Aggregation thread
    thread = threading.Thread(
        target=aggregation_loop,
        daemon=True
    )

    thread.start()

    # Flask
    app.run(
        host=GATEWAY_HOST,
        port=GATEWAY_PORT,
        debug=False,
        threaded=True
    )
