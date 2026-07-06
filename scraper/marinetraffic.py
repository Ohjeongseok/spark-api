"""
AISstream.io WebSocket 스크래퍼 - 백그라운드 상시 연결 + 캐시 방식

기존에는 검색 요청이 올 때마다 새로 WebSocket에 연결해서 짧은 시간(10초) 동안
해당 선박의 신호가 오기를 기다렸다. 그런데 한반도 연안은 AISstream의 지상 수신국이
많지 않아 10초 동안 전체 수신 메시지가 20개 안팎에 불과했고, 그 안에 원하는 배의
신호가 우연히 들어있어야만 위치를 찾을 수 있었다 (실제로 자주 실패).

이제는 서버가 켜져 있는 동안 WebSocket 연결을 한 번 맺어서 계속 유지하고,
수신되는 모든 선박의 위치 정보를 메모리 캐시에 누적 저장해둔다. 검색 시점에는
이미 쌓여있는 캐시에서 즉시 조회하므로 응답이 훨씬 빠르고, 서버가 오래 켜져
있을수록(=캐시가 쌓일수록) 적중률도 높아진다.
"""

import asyncio
import json
import logging
import os
import time
import websockets

logger = logging.getLogger(__name__)
AISSTREAM_WS_URL = "wss://stream.aisstream.io/v0/stream"

# 한반도 연안 해역 (포항/마산·삼천포/부산/울산/여수·광양·하동 전부 포함)
KOREA_BOUNDING_BOX = [[32.0, 123.0], [39.5, 132.5]]

# 선박명(공백 제거, 대문자 정규화) -> 최신 위치 정보
_AIS_CACHE: dict[str, dict] = {}
_CACHE_MAX_AGE_SEC = 30 * 60  # 30분 넘게 갱신 안 된 데이터는 신뢰하지 않음


def _normalize(name: str) -> str:
    return (name or "").strip().upper().replace(" ", "")


async def start_ais_listener():
    """서버 시작 시 백그라운드 태스크로 실행. 끊어지면 자동 재연결하며 영구 실행된다."""
    api_key = os.environ.get("AISSTREAM_API_KEY", "").strip()
    if not api_key:
        logger.warning("AISSTREAM_API_KEY 미설정 - AIS 실시간 위치 기능 비활성화")
        return

    subscribe_msg = {
        "APIKey": api_key,
        "MessageType": "subscribe",
        "BoundingBoxes": [KOREA_BOUNDING_BOX],
        "FilterMessageTypes": ["PositionReport", "ShipStaticData"],
    }

    while True:
        try:
            async with websockets.connect(AISSTREAM_WS_URL, ping_timeout=20) as ws:
                await ws.send(json.dumps(subscribe_msg))
                logger.info("AISstream 백그라운드 연결 성공 - 한반도 연안 수신 시작")
                count = 0
                async for raw in ws:
                    count += 1
                    if count % 200 == 0:
                        logger.info(f"AISstream 누적 수신 {count}개, 캐시된 선박 수: {len(_AIS_CACHE)}")
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue
                    _handle_message(msg)
        except Exception as e:
            logger.error(f"AISstream 연결 끊김, 5초 후 재연결 시도: {e}")
            await asyncio.sleep(5)


def _handle_message(msg: dict):
    metadata = msg.get("MetaData", {})
    ship_name = metadata.get("ShipName", "").strip().upper()
    if not ship_name:
        return

    msg_type = msg.get("MessageType", "")
    if msg_type not in ("PositionReport", "ShipStaticData"):
        return

    lat = round(metadata.get("latitude", 0), 4)
    lon = round(metadata.get("longitude", 0), 4)
    mmsi = metadata.get("MMSI", "")

    key = _normalize(ship_name)
    existing = _AIS_CACHE.get(key, {})

    entry = {
        "position": f"{lat}°N, {lon}°E",
        "lat": lat,
        "lon": lon,
        "destination": metadata.get("Destination") or existing.get("destination", "N/A"),
        "map_url": f"https://www.marinetraffic.com/en/ais/details/ships/mmsi:{mmsi}",
        "speed": existing.get("speed", "N/A"),
        "course": existing.get("course", "N/A"),
        "nav_status": existing.get("nav_status", "N/A"),
        "eta": existing.get("eta", "N/A"),
        "updated_at": time.time(),
    }

    if msg_type == "PositionReport":
        pos = msg.get("Message", {}).get("PositionReport", {})
        entry["speed"] = pos.get("Sog", "N/A")
        entry["course"] = pos.get("Cog", "N/A")
        entry["nav_status"] = _nav_status(pos.get("NavigationalStatus", 15))
    elif msg_type == "ShipStaticData":
        static = msg.get("Message", {}).get("ShipStaticData", {})
        entry["eta"] = _format_eta(static.get("Eta", {}))
        entry["destination"] = static.get("Destination") or entry["destination"]

    _AIS_CACHE[key] = entry


def get_vessel_info(vessel_name: str, timeout: int = 10) -> dict | None:
    """캐시에서 즉시 조회한다 (부분 매칭, 공백 무시). timeout 인자는 하위 호환용으로 남겨두되 사용하지 않는다."""
    vessel_key = _normalize(vessel_name)
    if not vessel_key:
        return None

    hit = _AIS_CACHE.get(vessel_key)
    if hit is None:
        # 정확히 일치하는 키가 없으면 부분 매칭 (양방향 포함 관계)
        for key, data in _AIS_CACHE.items():
            if vessel_key in key or key in vessel_key:
                hit = data
                break

    if hit is None:
        return None

    if time.time() - hit["updated_at"] > _CACHE_MAX_AGE_SEC:
        return None  # 너무 오래된 데이터는 신뢰하지 않음

    return {k: v for k, v in hit.items() if k != "updated_at"}


def _format_eta(eta) -> str:
    if not eta:
        return "N/A"
    if isinstance(eta, str):
        return eta
    if isinstance(eta, dict):
        month = eta.get("Month", 0)
        day = eta.get("Day", 0)
        hour = eta.get("Hour", 0)
        minute = eta.get("Minute", 0)
        if month and day:
            return f"{month:02d}/{day:02d} {hour:02d}:{minute:02d} UTC"
    return str(eta)


def _nav_status(code: int) -> str:
    status_map = {
        0: "항행 중",
        1: "닻 내림",
        2: "조종 불능",
        3: "조종 제한",
        5: "계류 중",
        8: "예항 중",
        15: "미정의",
    }
    return status_map.get(code, f"상태 {code}")
