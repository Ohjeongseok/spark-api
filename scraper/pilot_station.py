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
        elif port_name == "부산":
            result = _parse_busan_pilot(vessel_name)
        elif port_name == "울산":
            result = _parse_ulsan_pilot(vessel_name)
        elif port_name == "여수":
            result = _parse_yeosu_pilot(vessel_name)
        else:
            result = _parse_generic_pilot(vessel_name, config["url"])

        if result:
            result["port"] = port_name
            return result

    return None


def get_all_pob_info(port: str) -> list[dict]:
    """지정 항구의 도선예보 전체 목록 반환 (선박명 필터 없음)"""
    if port not in PILOT_STATION_URLS:
        return []

    config = PILOT_STATION_URLS[port]
    logger.info(f"{port} 예도선 전체 목록 조회 중")

    if port == "마산":
        results = _parse_masan_pilot_all(config["url"])
    elif port == "포항":
        results = _parse_pohang_pilot_all(config)
    elif port == "부산":
        results = _parse_busan_pilot_all()
    elif port == "울산":
        results = _parse_ulsan_pilot_all()
    elif port == "여수":
        results = _parse_yeosu_pilot_all()
    else:
        results = []

    for r in results:
        r["port"] = port
    return results


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


def _parse_pohang_pilot_all(config: dict) -> list[dict]:
    """포항 도선사회 - 전체 선박 목록 (선박명 필터 없음)"""
    session = _login_pohang(config)
    if not session:
        return []

    results = []
    try:
        resp = session.get(
            "http://www.dsmarine.co.kr/order/order_view.asp",
            timeout=REQUEST_TIMEOUT
        )
        resp.encoding = "euc-kr"

        if resp.status_code != 200:
            logger.error(f"포항 페이지 접근 실패: {resp.status_code}")
            return []

        soup = BeautifulSoup(resp.text, "html.parser")
        tables = soup.find_all("table")

        for table in tables:
            for row in table.find_all("tr"):
                cells = row.find_all("td")

                if len(cells) < 8:
                    continue

                ship_name_raw = cells[0].get_text(strip=True)

                if len(ship_name_raw) > 30:
                    continue
                if not re.search(r'[A-Za-z]', ship_name_raw):
                    continue

                gt_loa    = cells[1].get_text(strip=True)
                from_to   = cells[3].get_text(strip=True)
                tug       = cells[4].get_text(strip=True)
                pilot     = cells[5].get_text(strip=True)
                work_time = cells[6].get_text(strip=True)
                agent     = cells[7].get_text(strip=True) if len(cells) > 7 else "N/A"
                remark    = cells[8].get_text(strip=True) if len(cells) > 8 else "N/A"

                results.append({
                    "ship_name":  ship_name_raw,
                    "gt_loa":     gt_loa,
                    "status":     _get_move_type(from_to),
                    "from_to":    from_to,
                    "pilot_time": work_time,
                    "tug":        tug,
                    "pilot":      pilot,
                    "agent":      agent,
                    "berth":      remark,
                })

        logger.info(f"포항 전체 목록: {len(results)}건")

    except Exception as e:
        logger.error(f"포항 전체 목록 파싱 실패: {e}")

    return results


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


def _parse_masan_pilot_all(url: str) -> list[dict]:
    """마산 도선사회 - 전체 선박 목록 (선박명 필터 없음)"""
    soup = _fetch_page(url)
    if not soup:
        return []

    table = soup.find("table")
    if not table:
        return []

    results = []
    for row in table.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) < 11:
            continue

        from_to = f"{cells[9].get_text(strip=True)} → {cells[10].get_text(strip=True)}"
        move_type = _get_move_type(
            cells[9].get_text(strip=True) + "→" + cells[10].get_text(strip=True)
        )
        results.append({
            "ship_name":  cells[3].get_text(strip=True),
            "callsign":   cells[4].get_text(strip=True),
            "agent":      cells[5].get_text(strip=True),
            "gross_ton":  f"{cells[6].get_text(strip=True)} G/T",
            "draft":      f"{cells[7].get_text(strip=True)} m",
            "status":     f"{cells[8].get_text(strip=True)} ({move_type})",
            "pilot_time": f"{cells[1].get_text(strip=True)}일 {cells[2].get_text(strip=True)}",
            "from_to":    from_to,
            "berth":      cells[-1].get_text(strip=True),
        })

    logger.info(f"마산 전체 목록: {len(results)}건")
    return results


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


# ── 공통 헬퍼 (부산/울산/여수 - PS 코드 기반 입출항 판정) ──────────

def _classify_move_generic(from_code: str, to_code: str) -> str:
    """From/To 코드에 'PS'(Pilot Station) 계열 표기가 있으면 입출항으로 판정"""
    def norm(s):
        return re.sub(r'[^A-Za-z0-9]', '', (s or '')).upper()
    f, t = norm(from_code), norm(to_code)
    if f.startswith('PS'):
        return '입항 🟢'
    if t.startswith('PS'):
        return '출항 🔴'
    return 'Shifting 🔄'


def _status_emoji_busan(status: str) -> str:
    s = (status or '').strip()
    if s == '입항':
        return '입항 🟢'
    if s == '출항':
        return '출항 🔴'
    return s or 'Shifting 🔄'


# ── 부산 도선사회 ──────────────────────────
# 컬럼: NO|시간|선명|GRT/LOA|DT|FM|TO|접현|PT|C/S(IMO)|TUGS|검역|LINE|항행|AGENT|RMK(대리점)

def _fetch_busan_rows() -> list[tuple]:
    soup = _fetch_page("http://www.busanpilot.co.kr/popup/monitoring")
    if not soup:
        return []

    tables = soup.find_all("table")
    if len(tables) < 3:
        return []

    rows = []
    current_date = None
    for tr in tables[2].find_all("tr"):
        td_cells = tr.find_all("td")

        if td_cells:
            texts = [c.get_text(strip=True) for c in td_cells]
            if len(texts) < 16:
                continue
            rows.append((current_date, texts))
            continue

        # td가 없는 행: 날짜 구분행(<th colspan> 1개) 또는 헤더행(<th> 16개) 중 하나
        th_texts = [c.get_text(strip=True) for c in tr.find_all("th")]
        if len(th_texts) == 1 and re.match(r'^\d{4}-\d{2}-\d{2}$', th_texts[0]):
            current_date = th_texts[0]
        # 그 외(헤더행 등)는 건너뜀

    return rows


def _busan_row_to_dict(date: str, texts: list) -> dict:
    time_str = re.sub(r'\s+', ' ', texts[1]).strip()
    from_code, to_code = texts[5], texts[6]
    return {
        "ship_name":  texts[2],
        "gt_loa":     texts[3],
        "draft":      f"{texts[4]} m" if texts[4] else "N/A",
        "from_to":    f"{from_code} → {to_code}",
        "status":     _status_emoji_busan(texts[13]),
        "berth_side": texts[7],
        "pilot":      texts[8],
        "callsign":   texts[9],
        "tug":        texts[10],
        "agent":      texts[14] or texts[12],
        "berth":      texts[15],
        "pilot_time": f"{date} {time_str}".strip() if date else time_str,
    }


def _parse_busan_pilot_all() -> list[dict]:
    try:
        return [_busan_row_to_dict(date, texts) for date, texts in _fetch_busan_rows()]
    except Exception as e:
        logger.error(f"부산 전체 목록 파싱 실패: {e}")
        return []


def _parse_busan_pilot(vessel_name: str) -> dict | None:
    vessel_upper = vessel_name.strip().upper()
    try:
        for date, texts in _fetch_busan_rows():
            if vessel_upper in texts[2].upper():
                return _busan_row_to_dict(date, texts)
    except Exception as e:
        logger.error(f"부산 파싱 실패: {e}")
    return None


# ── 울산 도선사회 ──────────────────────────
# 컬럼: NO|STATUS|C/F|TIME|SHIP'S NAME|PILOT|C/SIGN|G/T|LOA|DFT|FROM|TO|L/A|CA|S/A|B/T|T|L|Q|REMARKS
# 오늘/내일 예보가 각각 별도 AJAX(POST)로 내려옴

def _fetch_ulsan_rows() -> list[tuple]:
    endpoints = [
        ("get_cz_or_assign_s.php", "오늘"),
        ("get_cz_or_assign_s02.php", "내일"),
    ]
    payload = {f"s_fg_status{i}": "" for i in range(1, 7)}

    rows = []
    for path, label in endpoints:
        try:
            session = requests.Session()
            session.headers.update(HEADERS)
            resp = session.post(
                f"http://www.ulsanpilot.co.kr/main/{path}",
                data=payload,
                timeout=REQUEST_TIMEOUT,
            )
            resp.encoding = "utf-8"
            soup = BeautifulSoup(f"<table><tbody>{resp.text}</tbody></table>", "html.parser")

            for tr in soup.find_all("tr"):
                cells = tr.find_all("td")
                texts = [c.get_text(strip=True) for c in cells]
                if len(texts) < 20:
                    continue
                rows.append((label, texts))
        except Exception as e:
            logger.error(f"울산 조회 실패 ({label}): {e}")

    return rows


def _ulsan_row_to_dict(label: str, texts: list) -> dict:
    from_code, to_code = texts[10], texts[11]
    gt, loa, draft = texts[7], texts[8], texts[9]
    return {
        "ship_name":  texts[4],
        "gt_loa":     f"{gt} ({loa})" if gt or loa else "N/A",
        "draft":      f"{draft} m" if draft else "N/A",
        "from_to":    f"{from_code} → {to_code}",
        "status":     _classify_move_generic(from_code, to_code),
        "pilot":      texts[5],
        "callsign":   texts[6],
        "agent":      texts[12],
        "berth":      texts[19],
        "pilot_time": f"{label} {texts[3]}".strip(),
    }


def _parse_ulsan_pilot_all() -> list[dict]:
    try:
        return [_ulsan_row_to_dict(label, texts) for label, texts in _fetch_ulsan_rows()]
    except Exception as e:
        logger.error(f"울산 전체 목록 파싱 실패: {e}")
        return []


def _parse_ulsan_pilot(vessel_name: str) -> dict | None:
    vessel_upper = vessel_name.strip().upper()
    try:
        for label, texts in _fetch_ulsan_rows():
            if vessel_upper in texts[4].upper():
                return _ulsan_row_to_dict(label, texts)
    except Exception as e:
        logger.error(f"울산 파싱 실패: {e}")
    return None


# ── 여수/광양/하동 도선사회 (통합 페이지) ──────────────────────────
# 컬럼(colspan 확장 후): NO|Spec|SW|Vessel|GRT|DFT|Seq|POB|From|To|B|PLTs*4|수습|CF|B/T|Tug|C|AGT|Line|C/S

def _fetch_yeosu_rows() -> list[list]:
    soup = _fetch_page("http://www.yspilot.co.kr/forecast")
    if not soup:
        return []

    tables = soup.find_all("table")
    if len(tables) < 5:
        return []

    rows = []
    trs = tables[4].find_all("tr")
    for tr in trs[1:]:
        cells = tr.find_all("td")
        texts = [c.get_text(strip=True) for c in cells]
        if len(texts) < 23:
            continue
        rows.append(texts)

    return rows


def _yeosu_row_to_dict(texts: list) -> dict:
    from_code, to_code = texts[8], texts[9]
    grt, dft = texts[4], texts[5]
    agent, line = texts[20], texts[21]
    return {
        "ship_name":  texts[3],
        "gross_ton":  f"{grt} G/T" if grt else "N/A",
        "draft":      f"{dft} m" if dft else "N/A",
        "from_to":    f"{from_code} → {to_code}",
        "status":     _classify_move_generic(from_code, to_code),
        "berth_side": texts[10],
        "pilot":      texts[11],
        "tug":        texts[18],
        "agent":      f"{agent} {line}".strip(),
        "callsign":   texts[22],
        "pilot_time": texts[7],
    }


def _parse_yeosu_pilot_all() -> list[dict]:
    try:
        return [_yeosu_row_to_dict(texts) for texts in _fetch_yeosu_rows()]
    except Exception as e:
        logger.error(f"여수 전체 목록 파싱 실패: {e}")
        return []


def _parse_yeosu_pilot(vessel_name: str) -> dict | None:
    vessel_upper = vessel_name.strip().upper()
    try:
        for texts in _fetch_yeosu_rows():
            if vessel_upper in texts[3].upper():
                return _yeosu_row_to_dict(texts)
    except Exception as e:
        logger.error(f"여수 파싱 실패: {e}")
    return None
