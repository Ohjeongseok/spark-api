"""
예도선 스테이션 스크래퍼
"""

import re
import time
import logging
import requests
from bs4 import BeautifulSoup
from config import HEADERS, REQUEST_TIMEOUT, REQUEST_RETRY, REQUEST_DELAY, PILOT_STATION_URLS

logger = logging.getLogger(__name__)


def get_pob_info(vessel_name: str, port: str = None) -> dict | None:
    search_ports = PILOT_STATION_URLS
    if port and port in PILOT_STATION_URLS:
        search_ports = {port: PILOT_STATION_URLS[port]}

    if not search_ports:
        return {"status": "⚠️ 예도선 사이트 미설정"}

    for port_name, config in search_ports.items():
        logger.info(f"{port_name} 예도선 조회 중: {vessel_name}")

        if port_name == "마산":
            result = _parse_masan_pilot(vessel_name, config["url"])
        elif port_name == "포항":
            result = _parse_pohang_pilot(vessel_name, config)
        else:
            result = _parse_generic_pilot(vessel_name, config["url"])

        if result:
            result["port"] = port_name
            return result

    return None


def _login_pohang(config: dict) -> requests.Session | None:
    session = requests.Session()
    session.headers.update(HEADERS)
    try:
        session.get("http://www.dsmarine.co.kr/", timeout=REQUEST_TIMEOUT)
        login_data = {"user_id": config["id"], "user_pass": config["pw"]}
        resp = session.post(
            "http://www.dsmarine.co.kr/loginok.asp",
            data=login_data,
            timeout=REQUEST_TIMEOUT,
            allow_redirects=True
        )
        resp.encoding = "euc-kr"
        logger.info(f"포항 로그인 응답: {resp.status_code} / URL: {resp.url}")
        return session
    except Exception as e:
        logger.error(f"포항 로그인 실패: {e}")
        return None


def _parse_pohang_pilot(vessel_name: str, config: dict) -> dict | None:
    """
    포항 도선사회 파서
    컬럼: 선명 | GT(LOA) | 입출 | From→To | 예선 | Pilot | 작업일시 | 선사(대리점) | 비고/ETA | 수정
    """
    session = _login_pohang(config)
    if not session:
        return None

    vessel_name_upper = vessel_name.strip().upper()

    try:
        resp = session.get(
            "http://www.dsmarine.co.kr/order/order_view.asp",
            timeout=REQUEST_TIMEOUT
        )
        resp.encoding = "euc-kr"

        if resp.status_code != 200:
            logger.error(f"포항 페이지 접근 실패: {resp.status_code}")
            return None

        soup = BeautifulSoup(resp.text, "html.parser")
        tables = soup.find_all("table")
        logger.info(f"포항 테이블 수: {len(tables)}")

        for table in tables:
            rows = table.find_all("tr")
            for row in rows:
                cells = row.find_all("td")

                # 정확히 9~10개 셀이 있는 데이터 행만 처리
                if len(cells) < 8:
                    continue

                ship_name_raw = cells[0].get_text(strip=True)

                # 헤더행 또는 설명행 제외 (너무 긴 텍스트)
                if len(ship_name_raw) > 30:
                    continue

                # 선박명이 영문+숫자인지 확인 (한글 행 제외)
                if not re.search(r'[A-Za-z]', ship_name_raw):
                    continue

                ship_name_upper = ship_name_raw.upper()
                logger.info(f"포항 선박 확인: [{ship_name_raw}]")

                if vessel_name_upper not in ship_name_upper:
                    continue

                logger.info(f"포항 선박 매칭 성공: {ship_name_raw}")

                gt_loa    = cells[1].get_text(strip=True)
                direction = cells[2].get_text(strip=True)
                from_to   = cells[3].get_text(strip=True)
                tug       = cells[4].get_text(strip=True)
                pilot     = cells[5].get_text(strip=True)
                work_time = cells[6].get_text(strip=True)
                agent     = cells[7].get_text(strip=True) if len(cells) > 7 else "N/A"
                remark    = cells[8].get_text(strip=True) if len(cells) > 8 else "N/A"

                return {
                    "ship_name":  ship_name_raw,
                    "gt_loa":     gt_loa,
                    "status":     _get_move_type(from_to),
                    "from_to":    from_to,
                    "pilot_time": work_time,
                    "tug":        tug,
                    "pilot":      pilot,
                    "agent":      agent,
                    "berth":      remark,
                }

        logger.info(f"포항에서 [{vessel_name_upper}] 선박을 찾지 못함")

    except Exception as e:
        logger.error(f"포항 파싱 실패: {e}")

    return None


def _get_move_type(from_to: str) -> str:
    ft = from_to.upper().replace(" ", "")
    if ft.startswith("PS"):
        return "입항 🟢"
    elif ft.endswith("PS"):
        return "출항 🔴"
    elif "PS" not in ft and "→" in ft:
        return "Shifting 🔄"
    return from_to


def _fetch_page(url: str):
    session = requests.Session()
    session.headers.update(HEADERS)
    for attempt in range(REQUEST_RETRY):
        try:
            resp = session.get(url, timeout=REQUEST_TIMEOUT)
            resp.encoding = "utf-8"
            if resp.status_code == 200:
                return BeautifulSoup(resp.text, "html.parser")
            time.sleep(REQUEST_DELAY)
        except requests.RequestException as e:
            logger.error(f"페이지 로드 실패 ({attempt+1}): {e}")
            time.sleep(REQUEST_DELAY * (attempt + 1))
    return None


def _parse_masan_pilot(vessel_name: str, url: str) -> dict | None:
    soup = _fetch_page(url)
    if not soup:
        return None

    vessel_name_upper = vessel_name.strip().upper()
    table = soup.find("table")
    if not table:
        return None

    for row in table.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) < 11:
            continue
        if vessel_name_upper not in cells[3].get_text(strip=True).upper():
            continue

        from_to = f"{cells[9].get_text(strip=True)} → {cells[10].get_text(strip=True)}"
        move_type = _get_move_type(
            cells[9].get_text(strip=True) + "→" + cells[10].get_text(strip=True)
        )
        return {
            "ship_name":  cells[3].get_text(strip=True),
            "callsign":   cells[4].get_text(strip=True),
            "agent":      cells[5].get_text(strip=True),
            "gross_ton":  f"{cells[6].get_text(strip=True)} G/T",
            "draft":      f"{cells[7].get_text(strip=True)} m",
            "status":     f"{cells[8].get_text(strip=True)} ({move_type})",
            "pilot_time": f"{cells[1].get_text(strip=True)}일 {cells[2].get_text(strip=True)}",
            "from_to":    from_to,
            "berth":      cells[-1].get_text(strip=True),
        }
    return None


def _parse_generic_pilot(vessel_name: str, url: str) -> dict | None:
    soup = _fetch_page(url)
    if not soup:
        return None
    vessel_name_upper = vessel_name.strip().upper()
    for table in soup.find_all("table"):
        for row in table.find_all("tr"):
            if vessel_name_upper in row.get_text().upper():
                cells = [c.get_text(strip=True) for c in row.find_all("td")]
                times = re.findall(r"\d{2}:\d{2}", " ".join(cells))
                return {
                    "ship_name":  vessel_name,
                    "status":     "정보 있음",
                    "pilot_time": times[0] if times else "N/A",
                    "berth":      cells[-1] if cells else "N/A",
                }
    return None
