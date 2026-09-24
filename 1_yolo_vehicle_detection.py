import cv2
import json
import numpy as np
import time
import requests
import torch
from datetime import datetime, timezone, timedelta
from ultralytics import YOLO

# ============================================================
# YOLO VEHICLE DETECTION
# YOLO -> Gateway
# ============================================================

FIELD_ID = "6610301001"

GATEWAY_URL = "http://127.0.0.1:5000/traffic-events"

VIDEO_FILE = "Speed3.mp4"

DEVICE_ID = "camera_pak_kret_01"
LOCATION_NAME = "HW304 Pak Kret IN Km.3+100"

JSON_INTERVAL = 15

REAL_DISTANCE_A_TO_B = 27.0

LINE_A = [2, 1070, 1239, 843]
LINE_B = [117, 432, 453, 401]

BLUE_ZONE = np.array([
    [136, 375],
    [20, 803],
    [10, 1070],
    [1802, 1044],
    [434, 349],
], np.int32)
CLASS_NAMES = {
    2: "car",
    5: "bus",
    7: "truck"
}

TARGET_CLASSES = list(CLASS_NAMES.keys())


def send_payload_to_gateway(payload):
    headers = {
        "Content-Type": "application/json; charset=utf-8"
    }

    try:
        response = requests.post(
            GATEWAY_URL,
            json=payload,
            headers=headers,
            timeout=3
        )

        if response.status_code in (200, 201):
            print("[YOLO -> Gateway SUCCESS]")
            print(response.text)
            return True

        print(
            f"[YOLO -> Gateway REJECTED] "
            f"Code: {response.status_code}"
        )
        print(response.text)
        return False

    except Exception as exc:
        print(
            f"[YOLO -> Gateway ERROR] "
            f"ไม่สามารถเชื่อมต่อ Gateway ได้: {exc}"
        )
        return False


def is_inside_polygon(point, polygon):
    polygon_np = np.array(polygon, dtype=np.int32)

    result = cv2.pointPolygonTest(
        polygon_np,
        (float(point[0]), float(point[1])),
        False
    )

    return result >= 0


def is_crossing_line(point, line_coords, threshold=30):
    px, py = point
    lx1, ly1, lx2, ly2 = line_coords

    min_x = min(lx1, lx2)
    max_x = max(lx1, lx2)

    if min_x - 40 <= px <= max_x + 40:
        if lx2 - lx1 != 0:
            line_y = (
                ly1
                + (ly2 - ly1)
                * (px - lx1)
                / (lx2 - lx1)
            )
        else:
            line_y = ly1

        return abs(py - line_y) <= threshold

    return False


def has_reached_line(point, line_coords, threshold=30):
    px, py = point
    lx1, ly1, lx2, ly2 = line_coords

    min_x = min(lx1, lx2)
    max_x = max(lx1, lx2)

    if not min_x - 40 <= px <= max_x + 40:
        return False

    if lx2 - lx1 != 0:
        line_y = (
            ly1
            + (ly2 - ly1)
            * (px - lx1)
            / (lx2 - lx1)
        )
    else:
        line_y = ly1

    return py <= line_y + threshold


def summarize_type_speeds(type_speeds):
    if not type_speeds:
        return {
            "total_vehicles": 0,
            "avg_speed_kmh": 0.0,
            "max_speed_kmh": 0.0,
            "min_speed_kmh": 0.0
        }

    return {
        "total_vehicles": len(type_speeds),
        "avg_speed_kmh": round(
            sum(type_speeds) / len(type_speeds),
            1
        ),
        "max_speed_kmh": round(
            max(type_speeds),
            1
        ),
        "min_speed_kmh": round(
            min(type_speeds),
            1
        )
    }


def main():

    print("=" * 60)
    print("YOLO Vehicle Detection")
    print("=" * 60)
    print(f"Field ID : {FIELD_ID}")
    print(f"Gateway  : {GATEWAY_URL}")
    print(f"Video    : {VIDEO_FILE}")
    print("=" * 60)

    # GPU / CPU
    if torch.cuda.is_available():
        torch.backends.cudnn.benchmark = True
        torch.backends.cuda.matmul.allow_tf32 = True
        torch.backends.cudnn.allow_tf32 = True
        device = "cuda:0"
    else:
        device = "cpu"

    print(f"[YOLO] Device: {device}")

    # Load model
    model = YOLO("yolov8n.pt").to(device)

    # Open video
    cap = cv2.VideoCapture(VIDEO_FILE)
    cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)

    if not cap.isOpened():
        print(f"[ERROR] เปิดไฟล์ {VIDEO_FILE} ไม่ได้")
        return

    vehicle_records = {}
    completed_vehicles_log = []

    last_json_time = time.time()

    frame_count = 0
    SKIP_FRAMES = 1
    video_fps = cap.get(cv2.CAP_PROP_FPS)
    if not video_fps or video_fps <= 0:
        video_fps = 30.0

    window_name = "YOLO Traffic Detection"

    cv2.namedWindow(
        window_name,
        cv2.WINDOW_NORMAL
    )

    cv2.resizeWindow(
        window_name,
        1280,
        720
    )

    while cap.isOpened():

        ret, frame = cap.read()

        if not ret:
            print(f"[YOLO] วิดีโอจบแล้ว เริ่ม {VIDEO_FILE} ใหม่")
            cap.release()
            cap = cv2.VideoCapture(VIDEO_FILE)
            cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)

            if not cap.isOpened():
                print(f"[ERROR] เปิดไฟล์ {VIDEO_FILE} ใหม่ไม่ได้")
                break

            # เริ่มการติดตามรถใหม่เมื่อวนกลับไปต้นคลิป
            frame_count = 0
            vehicle_records.clear()
            model.predictor = None
            continue

        frame_count += 1

        frame_time = cap.get(cv2.CAP_PROP_POS_MSEC) / 1000.0
        if frame_time <= 0:
            frame_time = frame_count / video_fps

        if frame_count % SKIP_FRAMES != 0:
            continue

        current_time = time.time()

        current_dt = datetime.now(
            timezone(timedelta(hours=7))
        )

        # ----------------------------------------------------
        # YOLO Tracking
        # ----------------------------------------------------

        results = model.track(
            frame,
            persist=True,
            classes=TARGET_CLASSES,
            conf=0.3,
            verbose=False,
            device=device,
            imgsz=640
        )

        if (
            results[0].boxes is not None
            and results[0].boxes.id is not None
        ):

            boxes = (
                results[0]
                .boxes
                .xyxy
                .cpu()
                .numpy()
            )

            track_ids = (
                results[0]
                .boxes
                .id
                .int()
                .cpu()
                .numpy()
            )

            classes = (
                results[0]
                .boxes
                .cls
                .int()
                .cpu()
                .numpy()
            )

            for bbox, track_id, cls in zip(
                boxes,
                track_ids,
                classes
            ):

                x1, y1, x2, y2 = map(
                    int,
                    bbox
                )

                bottom_center = (
                    (x1 + x2) // 2,
                    y2
                )

                vehicle_type = CLASS_NAMES.get(
                    cls,
                    "vehicle"
                )

                # ประมวลผลเฉพาะรถที่จุดกึ่งกลางด้านล่างอยู่ในกรอบสีชมพู
                if not is_inside_polygon(
                    bottom_center,
                    BLUE_ZONE
                ):
                    continue

                # ------------------------------------------------
                # เริ่มจับเวลาที่ Line A ไม่ใช่ตอนเข้าโซน
                # ------------------------------------------------

                crossed_line_a = has_reached_line(
                    bottom_center,
                    LINE_A
                )

                if (
                    track_id not in vehicle_records
                    and crossed_line_a
                ):

                    vehicle_records[track_id] = {
                        "type": vehicle_type,
                        "t_a": frame_time,
                        "t_b": None,
                        "speed_kmh": None,
                        "entry_time_iso":
                            current_dt.isoformat(),
                        "exit_time_iso": None
                    }

                # รถที่ยังไม่ผ่าน Line A ยังนำมาคำนวณไม่ได้
                if track_id not in vehicle_records:
                    continue

                rec = vehicle_records.get(track_id)

                if rec is None:
                    continue

                # ------------------------------------------------
                # ตรวจ Line B
                # ------------------------------------------------

                if (
                    rec["t_a"] is not None
                    and rec["t_b"] is None
                ):

                    crossed = has_reached_line(
                        bottom_center,
                        LINE_B
                    )

                    if crossed:

                        rec["t_b"] = frame_time

                        rec["exit_time_iso"] = (
                            current_dt.isoformat()
                        )

                        travel_time = (
                            rec["t_b"]
                            - rec["t_a"]
                        )

                        if travel_time > 0.1:

                            speed = (
                                REAL_DISTANCE_A_TO_B
                                / travel_time
                            ) * 3.6

                            rec["speed_kmh"] = round(
                                speed,
                                1
                            )

                            completed_vehicles_log.append(
                                {
                                    "track_id":
                                        int(track_id),

                                    "vehicle_type":
                                        vehicle_type,

                                    "speed_kmh":
                                        rec["speed_kmh"],

                                    "travel_time_sec":
                                        round(
                                            travel_time,
                                            2
                                        ),

                                    "entry_time":
                                        rec["entry_time_iso"],

                                    "exit_time":
                                        rec["exit_time_iso"],

                                    "exit_time_epoch":
                                        current_time
                                }
                            )

                # ------------------------------------------------
                # วาด Bounding Box
                # ------------------------------------------------

                color = (
                    (0, 255, 0)
                    if rec["speed_kmh"] is not None
                    else (255, 255, 0)
                )

                label = (
                    f"ID:{track_id} "
                    f"[{vehicle_type}]"
                )

                if rec["speed_kmh"] is not None:
                    label += (
                        f" {rec['speed_kmh']} km/h"
                    )
                else:
                    label += " [TIMING...]"

                cv2.rectangle(
                    frame,
                    (x1, y1),
                    (x2, y2),
                    color,
                    2
                )

                cv2.putText(
                    frame,
                    label,
                    (
                        x1,
                        max(y1 - 10, 20)
                    ),
                    cv2.FONT_HERSHEY_SIMPLEX,
                    0.5,
                    color,
                    2
                )

        # --------------------------------------------------------
        # Draw Line A
        # --------------------------------------------------------

        cv2.line(
            frame,
            (LINE_A[0], LINE_A[1]),
            (LINE_A[2], LINE_A[3]),
            (0, 255, 255),
            3
        )

        cv2.putText(
            frame,
            "LINE A (ENTRY)",
            (LINE_A[0], LINE_A[1] - 10),
            cv2.FONT_HERSHEY_SIMPLEX,
            0.6,
            (0, 255, 255),
            2
        )

        # --------------------------------------------------------
        # Draw Line B
        # --------------------------------------------------------

        cv2.line(
            frame,
            (LINE_B[0], LINE_B[1]),
            (LINE_B[2], LINE_B[3]),
            (0, 165, 255),
            3
        )

        cv2.putText(
            frame,
            "LINE B (EXIT)",
            (LINE_B[0], LINE_B[1] - 10),
            cv2.FONT_HERSHEY_SIMPLEX,
            0.6,
            (0, 165, 255),
            2
        )

        # --------------------------------------------------------
        # Draw detection zone (pink polygon)
        # --------------------------------------------------------

        cv2.polylines(
            frame,
            [np.array(BLUE_ZONE, dtype=np.int32)],
            True,
            (255, 0, 255),
            3
        )

        # ========================================================
        # SEND PAYLOAD EVERY 15 SECONDS
        # ========================================================

        if (
            current_time - last_json_time
            >= JSON_INTERVAL
        ):

            interval_vehicles = [
                v
                for v in completed_vehicles_log
                if (
                    v.get("speed_kmh") is not None
                    and
                    isinstance(
                        v.get("exit_time_epoch"),
                        (int, float)
                    )
                    and
                    current_time
                    - v["exit_time_epoch"]
                    <= JSON_INTERVAL
                )
            ]

            counts_by_type = {
                "car": 0,
                "truck": 0,
                "bus": 0
            }

            speeds_by_type = {
                "car": [],
                "truck": [],
                "bus": []
            }

            for v in interval_vehicles:

                vehicle_type = v.get(
                    "vehicle_type",
                    "car"
                )

                if vehicle_type in counts_by_type:

                    counts_by_type[
                        vehicle_type
                    ] += 1

                    speeds_by_type[
                        vehicle_type
                    ].append(
                        float(v["speed_kmh"])
                    )

            payload = {
                "id": FIELD_ID,

                "timestamp":
                    current_dt.isoformat(
                        timespec="seconds"
                    ),

                "device_id":
                    DEVICE_ID,

                "location_name":
                    LOCATION_NAME,

                "interval_sec":
                    JSON_INTERVAL,

                "summary": {

                    "total_vehicles":
                        len(interval_vehicles),

                    "vehicle_counts_by_type":
                        counts_by_type,

                    "vehicle_speed_by_type":
                    {
                        vehicle_type:
                            summarize_type_speeds(
                                speeds_by_type[
                                    vehicle_type
                                ]
                            )
                        for vehicle_type
                        in ["car", "truck", "bus"]
                    }
                }
            }

            print("\n=== Generated JSON ===")
            print(
                json.dumps(
                    payload,
                    ensure_ascii=False,
                    indent=2
                )
            )

            send_payload_to_gateway(payload)

            last_json_time = current_time

        cv2.imshow(
            window_name,
            frame
        )

        if cv2.waitKey(1) & 0xFF == ord("q"):
            break

    cap.release()
    cv2.destroyAllWindows()

    print("[YOLO] Stopped")


if __name__ == "__main__":
    main()
