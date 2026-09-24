import json
from datetime import datetime

import requests
import paho.mqtt.client as mqtt

# ============================================================
# INFLUXDB COLLECTOR
# MQTT -> InfluxDB
# ============================================================

FIELD_ID = "6610301001"

MQTT_BROKER = "172.16.2.117"
MQTT_PORT = 1883
MQTT_TOPIC = "traffic/aggregated"

INFLUXDB_URL = "http://172.16.2.117:8086"
INFLUXDB_ORG = "e761e698e5720d2f"
INFLUXDB_BUCKET = "mini_project"

# IMPORTANT:
# ใส่ InfluxDB Token ใหม่ของคุณ
INFLUXDB_TOKEN = "ai3V3vxPXNMwE_4aGePni-5uLU7MumFckIpHbYi5_O52LkpH38b9IM4WucD6RoKSm8oBx-Wy8ZOfSITZqrTQ7A=="


# ============================================================
# ESCAPE TAG
# ============================================================

def escape_tag(value):

    value = str(value)

    return (
        value
        .replace("\\", "\\\\")
        .replace(" ", "\\ ")
        .replace(",", "\\,")
        .replace("=", "\\=")
    )


# ============================================================
# JSON -> LINE PROTOCOL
# ============================================================

def convert_to_line_protocol(payload):

    timestamp_str = payload.get(
        "timestamp"
    )

    if not timestamp_str:
        raise ValueError(
            "Payload ไม่มี timestamp"
        )

    dt = datetime.fromisoformat(
        timestamp_str
    )

    timestamp = int(
        dt.timestamp()
    )

    device = escape_tag(
        payload.get(
            "device_id",
            "unknown"
        )
    )

    location = escape_tag(
        payload.get(
            "location_name",
            "unknown"
        )
    )

    field_id = escape_tag(
        FIELD_ID
    )

    summary = payload.get(
        "summary",
        {}
    )

    counts = summary.get(
        "vehicle_counts_by_type",
        {}
    )

    speed_summary = summary.get(
        "vehicle_speed_by_type",
        {}
    )

    lines = []

    for vehicle_type in [
        "car",
        "truck",
        "bus"
    ]:

        count = int(
            counts.get(
                vehicle_type,
                0
            )
            or 0
        )

        stats = speed_summary.get(
            vehicle_type,
            {}
        )

        avg_speed = float(
            stats.get(
                "avg_speed_kmh",
                0
            )
            or 0
        )

        max_speed = float(
            stats.get(
                "max_speed_kmh",
                0
            )
            or 0
        )

        min_speed = float(
            stats.get(
                "min_speed_kmh",
                0
            )
            or 0
        )

        vtype = escape_tag(
            vehicle_type
        )

        # ----------------------------------------------------
        # Measurement : traffic_6610301001
        #
        # TAGS:
        #   device
        #   location
        #   type
        #   field_id
        #
        # FIELDS:
        #   count
        #   avg_speed
        #   max_speed
        #   min_speed
        # ----------------------------------------------------

        line = (
            f"traffic_6610301001,"
            f"device={device},"
            f"location={location},"
            f"type={vtype},"
            f"field_id={field_id} "
            f"count={count}i,"
            f"avg_speed={avg_speed},"
            f"max_speed={max_speed},"
            f"min_speed={min_speed} "
            f"{timestamp}"
        )

        lines.append(line)

    return "\n".join(lines)


# ============================================================
# WRITE INFLUXDB
# ============================================================

def send_to_influxdb(
    line_protocol
):

    url = (
        f"{INFLUXDB_URL}"
        f"/api/v2/write"
    )

    params = {
        "org": INFLUXDB_ORG,
        "bucket": INFLUXDB_BUCKET,
        "precision": "s"
    }

    headers = {
        "Authorization":
            f"Token {INFLUXDB_TOKEN}",

        "Content-Type":
            "text/plain; charset=utf-8"
    }

    try:

        response = requests.post(
            url,
            params=params,
            headers=headers,
            data=line_protocol,
            timeout=10
        )

        if response.status_code == 204:

            print(
                "[InfluxDB] "
                "Write successful"
            )

            return True

        print(
            f"[InfluxDB ERROR] "
            f"HTTP {response.status_code}"
        )

        print(
            response.text
        )

        return False

    except Exception as e:

        print(
            f"[InfluxDB ERROR] {e}"
        )

        return False


# ============================================================
# MQTT CONNECT
# ============================================================

def on_connect(
    client,
    userdata,
    flags,
    rc
):

    if rc == 0:

        print(
            "[MQTT] Connected to Broker"
        )

        print(
            f"[MQTT] "
            f"{MQTT_BROKER}:{MQTT_PORT}"
        )

        client.subscribe(
            MQTT_TOPIC,
            qos=1
        )

        print(
            f"[MQTT] Subscribed to "
            f"{MQTT_TOPIC}"
        )

        print(
            f"[FIELD ID] "
            f"{FIELD_ID}"
        )

    else:

        print(
            f"[MQTT] "
            f"Connection failed rc={rc}"
        )


# ============================================================
# MQTT MESSAGE
# ============================================================

def on_message(
    client,
    userdata,
    msg
):

    try:

        print(
            "\n=========================================="
        )

        print(
            "[MQTT] Received aggregated data"
        )

        print(
            f"Topic: {msg.topic}"
        )

        payload = json.loads(
            msg.payload.decode(
                "utf-8"
            )
        )

        # บังคับ ID ให้ตรงกับ Field ID
        payload["id"] = FIELD_ID

        print(
            "\n[JSON]"
        )

        print(
            json.dumps(
                payload,
                ensure_ascii=False,
                indent=2
            )
        )

        line_protocol = (
            convert_to_line_protocol(
                payload
            )
        )

        print(
            "\n[LINE PROTOCOL]"
        )

        print(
            line_protocol
        )

        success = send_to_influxdb(
            line_protocol
        )

        if success:

            print(
                f"[Collector] "
                f"Field ID {FIELD_ID} "
                f"saved to InfluxDB"
            )

        print(
            "==========================================\n"
        )

    except json.JSONDecodeError as e:

        print(
            f"[JSON ERROR] {e}"
        )

    except Exception as e:

        print(
            f"[COLLECTOR ERROR] {e}"
        )


# ============================================================
# DISCONNECT
# ============================================================

def on_disconnect(
    client,
    userdata,
    rc
):

    print(
        f"[MQTT] Disconnected rc={rc}"
    )


# ============================================================
# MAIN
# ============================================================

def main():

    print("=" * 60)
    print("InfluxDB Collector")
    print("=" * 60)

    print(
        f"Field ID    : {FIELD_ID}"
    )

    print(
        f"MQTT Broker : "
        f"{MQTT_BROKER}:{MQTT_PORT}"
    )

    print(
        f"MQTT Topic  : "
        f"{MQTT_TOPIC}"
    )

    print(
        f"InfluxDB    : "
        f"{INFLUXDB_URL}"
    )

    print(
        f"Bucket      : "
        f"{INFLUXDB_BUCKET}"
    )

    print("=" * 60)

    client = mqtt.Client()

    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect

    try:

        print(
            "[MQTT] Connecting..."
        )

        client.connect(
            MQTT_BROKER,
            MQTT_PORT,
            keepalive=60
        )

        print(
            "[MQTT] Waiting for data..."
        )

        client.loop_forever()

    except Exception as e:

        print(
            f"[MQTT ERROR] {e}"
        )


if __name__ == "__main__":
    main()
