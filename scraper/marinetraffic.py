"""
AISstream.io WebSocket 스크래퍼
"""

import asyncio
import json
import logging
import os
import websockets

logger = logging.getLogger(__name__)
AISSTREAM_WS_URL = "wss://stream.aisstream.io/v0/stream"


def get_vessel_info(vessel_name: str, timeout: int = 10) -> dict | None:
    try:
        loop = asyncio.new_event_loop()
        result = loop.run_until_complete(_fetch_ais(vessel_name, timeout=timeout))
        loop.close()
        return result
    except Exception as e:
        logger.error(f"AISstream 조회 실패: {e}")
        return None


async def _fetch_ais(vessel_name: str, timeout: int = 10) -> dict | None:
    api_key = os.environ.get("AISSTREAM_API_KEY", "").strip()
    key_preview = api_key[:8] if api_key else "없음"
    logger.info(f"AISSTREAM KEY: {key_preview}")

    if not api_key:
        logger.warning("AISSTREAM_API_KEY 미설정")
        return None

    # 검색어 변형 준비 (공백 제거, 부분 매칭)
    vessel_clean = vessel_name.strip().upper()
    vessel_nospace = vessel_clean.replace(" ", "")

    subscribe_msg = {
        "APIKey": api_key,
        "MessageType": "subscribe",
        "BoundingBoxes": [[[-90, -180], [90, 180]]],
        "FilterMessageTypes": ["PositionReport", "ShipStaticData"],
    }

    try:
        async with websockets.connect(AISSTREAM_WS_URL, ping_timeout=20) as ws:
            await ws.send(json.dumps(subscribe_msg))
            logger.info(f"AISstream 연결, [{vessel_clean}] {timeout}초 검색 중...")

            deadline = asyncio.get_event_loop().time() + timeout
            count = 0

            while asyncio.get_event_loop().time() < deadline:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=3.0)
                    msg = json.loads(raw)
                    count += 1
                except asyncio.TimeoutError:
                    continue
                except Exception:
                    break

                metadata = msg.get("MetaData", {})
                ship_name = metadata.get("ShipName", "").strip().upper()
                ship_nospace = ship_name.replace(" ", "")

                # 부분 매칭 - 공백 유무 관계없이
                if vessel_clean not in ship_name and vessel_nospace not in ship_nospace:
                    continue

                msg_type = msg.get("MessageType", "")
                logger.info(f"AISstream 발견: {ship_name} ({msg_type}), 수신 메시지: {count}개")

                lat = round(metadata.get("latitude", 0), 4)
                lon = round(metadata.get("longitude", 0), 4)
                mmsi = metadata.get("MMSI", "")

                if msg_type == "PositionReport":
                    pos = msg.get("Message", {}).get("PositionReport", {})
                    return {
                        "position": f"{lat}°N, {lon}°E",
                        "lat": lat,
                        "lon": lon,
                        "speed": pos.get("Sog", "N/A"),
                        "course": pos.get("Cog", "N/A"),
                        "destination": metadata.get("Destination", "N/A"),
                        "eta": _format_eta(metadata.get("Eta", "")),
                        "nav_status": _nav_status(pos.get("NavigationalStatus", 15)),
                        "map_url": f"https://www.marinetraffic.com/en/ais/details/ships/mmsi:{mmsi}",
                    }

                elif msg_type == "ShipStaticData":
                    static = msg.get("Message", {}).get("ShipStaticData", {})
                    return {
                        "position": f"{lat}°N, {lon}°E",
                        "lat": lat,
                        "lon": lon,
                        "speed": "N/A",
                        "course": "N/A",
                        "destination": static.get("Destination", "N/A"),
                        "eta": _format_eta(static.get("Eta", {})),
                        "nav_status": "N/A",
                        "map_url": f"https://www.marinetraffic.com/en/ais/details/ships/mmsi:{mmsi}",
                    }

            logger.info(f"AISstream: [{vessel_clean}] {timeout}초 내 미발견 (수신: {count}개)")
            return None

    except Exception as e:
        logger.error(f"AISstream WebSocket 오류: {e}")
        return None


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
