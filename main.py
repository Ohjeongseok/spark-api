"""
Spark Mobile - 선박 조회 API 서버
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import psycopg2
import psycopg2.extras
import os
import asyncio
import json
import random
import logging
import re
import time
from datetime import datetime, timedelta, timezone
import firebase_admin
from firebase_admin import credentials, messaging
from scraper.pilot_station import get_pob_info, get_all_pob_info
from scraper.marinetraffic import get_vessel_info, start_ais_listener

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

app = FastAPI(title="Spark Mobile API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

DATABASE_URL = os.environ.get("DATABASE_URL", "")
SUPPORTED_PORTS = ["포항", "마산", "부산", "울산", "여수"]
MISSING_RESET_THRESHOLD = 3  # 예보 연속 미검출 n회 → 타겟 상태 초기화
REMIND_BEFORE_MIN = int(os.environ.get("REMIND_BEFORE_MIN", "60"))  # 도선 몇 분 전에 임박 알림을 보낼지
KST = timezone(timedelta(hours=9), "KST")  # 한국은 서머타임이 없어 고정 오프셋으로 충분

# /monitor가 겹쳐 실행되면(이전 실행이 아직 안 끝났는데 다음 호출이 오는 경우) 같은 알림이
# 두 번 갈 수 있으므로 한 번에 하나만 돌게 한다.
_monitor_lock = asyncio.Lock()

# Firebase Admin SDK 초기화
_firebase_initialized = False

def init_firebase():
    global _firebase_initialized
    if _firebase_initialized:
        return True
    try:
        cred_json = os.environ.get("FIREBASE_CREDENTIALS", "")
        if not cred_json:
            print("⚠️  FIREBASE_CREDENTIALS 환경변수 없음 - 푸시 알림 비활성화")
            return False
        cred_dict = json.loads(cred_json)
        cred = credentials.Certificate(cred_dict)
        firebase_admin.initialize_app(cred)
        _firebase_initialized = True
        print("✅ Firebase Admin SDK 초기화 완료")
        return True
    except Exception as e:
        print(f"❌ Firebase 초기화 실패: {e}")
        return False


def get_conn():
    return psycopg2.connect(DATABASE_URL)


def init_db():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS targets (
                    id SERIAL PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    port TEXT NOT NULL,
                    vessel TEXT NOT NULL,
                    last_key TEXT,
                    fcm_token TEXT,
                    UNIQUE(user_id, port, vessel)
                )
            """)
            # 예보가 연속으로 몇 번 안 보였는지. 일정 횟수 이상이면 last_key를 비워
            # 같은 선박이 다시 잡힐 때 "변경"이 아니라 "등록" 알림이 가도록 한다.
            cur.execute("ALTER TABLE targets ADD COLUMN IF NOT EXISTS missing_count INTEGER DEFAULT 0")
            # 임박 알림을 이미 보낸 작업일시. 작업일시가 바뀌면(연기 등) 새 시각 기준으로 다시 알린다.
            cur.execute("ALTER TABLE targets ADD COLUMN IF NOT EXISTS reminded_for TEXT")
            # 선박별 마지막으로 잡힌 AIS 위치. AIS 캐시는 메모리라 서버가 재시작되거나
            # 선박이 수신 범위를 벗어나면 사라지므로, "마지막으로 본 위치"를 DB에 남겨둔다.
            cur.execute("""
                CREATE TABLE IF NOT EXISTS ais_last_seen (
                    vessel TEXT PRIMARY KEY,
                    data TEXT NOT NULL,
                    seen_at DOUBLE PRECISION NOT NULL
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS fcm_tokens (
                    user_id TEXT PRIMARY KEY,
                    token TEXT NOT NULL,
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            """)
        conn.commit()


# ── AIS 마지막 위치 보존 ──────────────────────

def _vessel_key(name: str) -> str:
    return (name or "").strip().upper().replace(" ", "")


def remember_ais(vessel: str, ais: dict | None):
    """실시간 AIS가 잡혔을 때 DB에 마지막 위치로 저장 (실패해도 조회 자체는 계속)."""
    if not ais or ais.get("lat") is None or ais.get("lon") is None:
        return
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO ais_last_seen (vessel, data, seen_at) VALUES (%s, %s, %s)
                    ON CONFLICT (vessel) DO UPDATE SET data = EXCLUDED.data, seen_at = EXCLUDED.seen_at
                """, (_vessel_key(vessel), json.dumps(ais, ensure_ascii=False),
                      float(ais.get("updated_at") or time.time())))
            conn.commit()
    except Exception as e:
        print(f"AIS 마지막 위치 저장 실패: {e}")


def load_last_ais(vessel: str) -> dict | None:
    """실시간 AIS가 없을 때 보여줄 마지막 위치. stale=True 와 updated_at(수신 시각)을 붙여준다."""
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT data, seen_at FROM ais_last_seen WHERE vessel = %s", (_vessel_key(vessel),))
                row = cur.fetchone()
        if not row:
            return None
        data = json.loads(row[0])
        data["updated_at"] = int(row[1])
        data["stale"] = True
        return data
    except Exception as e:
        print(f"AIS 마지막 위치 조회 실패: {e}")
        return None


def lookup_ais(vessel: str) -> tuple[dict | None, dict | None]:
    """(실시간 ais, 마지막 위치 last_ais). 실시간이 있으면 저장하고 last_ais는 None."""
    ais = get_vessel_info(vessel)
    if ais:
        remember_ais(vessel, ais)
        return ais, None
    return None, load_last_ais(vessel)


# ── 도선예보 변경 내용 요약 ────────────────────

_CHANGE_LABELS = [
    ("pilot_time", "작업일시"),
    ("status", "상태"),
    ("from_to", "From→To"),
    ("berth", "선석/비고"),
    ("pilot", "도선사"),
    ("tug", "예선"),
    ("berth_side", "접현"),
    ("draft", "흘수"),
    ("agent", "대리점"),
]


def _clean(v) -> str:
    return " ".join(str(v if v is not None else "").split())


def summarize_pob(pob: dict) -> str:
    """등록 알림용 한 줄 요약: 작업일시 · 상태 · From→To"""
    parts = [_clean(pob.get(k)) for k in ("pilot_time", "status", "from_to")]
    return " · ".join(p for p in parts if p and p != "N/A")


def describe_changes(old_key: str, pob: dict) -> str:
    """이전 상태(JSON 문자열)와 현재 예보를 비교해 무엇이 바뀌었는지 설명한다."""
    try:
        old = json.loads(old_key) if old_key else {}
    except Exception:
        old = {}
    if not isinstance(old, dict):
        old = {}
    changes = []
    for field, label in _CHANGE_LABELS:
        before, after = _clean(old.get(field)), _clean(pob.get(field))
        if before == after:
            continue
        if before and after:
            changes.append(f"{label} {before} → {after}")
        elif after:
            changes.append(f"{label} {after}")
        else:
            changes.append(f"{label} 삭제")
    return "\n".join(changes[:4])


# ── 도선 임박 알림 ──────────────────────────

def parse_pilot_time(raw: str, now: datetime | None = None) -> datetime | None:
    """항구마다 다른 작업일시 표기를 KST 시각으로 바꾼다.
    "11-11 08:30"(포항), "11일 08:00"(마산), "2026-11-11 10:00 +"(부산), "오늘/내일 10:00"(울산), "09:30"(여수)"""
    if not raw:
        return None
    now = now or datetime.now(KST)
    t = re.search(r"(\d{1,2}):(\d{2})", raw)
    if not t:
        return None
    hh, mm = int(t.group(1)), int(t.group(2))
    if hh > 23 or mm > 59:
        return None
    year, month, day = now.year, now.month, now.day
    full = re.search(r"(\d{4})-(\d{1,2})-(\d{1,2})", raw)
    md = re.search(r"(?<!\d)(\d{1,2})-(\d{1,2})(?!\d)", raw)
    d_only = re.search(r"(\d{1,2})일", raw)
    try:
        if full:
            year, month, day = int(full.group(1)), int(full.group(2)), int(full.group(3))
            return datetime(year, month, day, hh, mm, tzinfo=KST)
        if md:
            month, day = int(md.group(1)), int(md.group(2))
            if month == 1 and now.month == 12:
                year += 1  # 연말에 올라온 다음 해 1월 예보
            return datetime(year, month, day, hh, mm, tzinfo=KST)
        if d_only:
            day = int(d_only.group(1))
            if day < now.day - 15:  # 월말에 올라온 다음 달 초 예보
                month, year = (1, year + 1) if month == 12 else (month + 1, year)
            return datetime(year, month, day, hh, mm, tzinfo=KST)
        base = datetime(year, month, day, hh, mm, tzinfo=KST)
        if "내일" in raw:
            return base + timedelta(days=1)
        if "오늘" not in raw and base < now - timedelta(hours=12):
            # 날짜 없이 시각만 있는 경우(여수): 한참 지난 시각이면 다음 날 작업으로 본다
            return base + timedelta(days=1)
        return base
    except ValueError:
        return None


def maybe_send_reminder(target: dict, pob: dict, fcm_token: str) -> str | None:
    """작업일시가 REMIND_BEFORE_MIN 분 이내로 다가왔고 아직 알리지 않았으면 임박 알림을 보낸다."""
    pilot_time = _clean(pob.get("pilot_time"))
    when = parse_pilot_time(pilot_time)
    if not when:
        return None
    minutes = (when - datetime.now(KST)).total_seconds() / 60
    if not (0 <= minutes <= REMIND_BEFORE_MIN):
        return None
    if (target.get("reminded_for") or "") == pilot_time:
        return None  # 이 작업일시로는 이미 알림

    vessel, port = target["vessel"], target["port"]
    left = max(1, int(round(minutes)))
    title = f"⏰ {vessel} 도선 {left}분 전"
    parts = [f"{port}항", when.strftime("%H:%M")] + [
        _clean(pob.get(k)) for k in ("status", "from_to") if _clean(pob.get(k)) not in ("", "N/A")
    ]
    body = " · ".join(parts)
    send_push_notification(fcm_token, title, body, {"port": port, "vessel": vessel},
                           channel_id="pilot_imminent_channel")
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE targets SET reminded_for = %s WHERE user_id = %s AND port = %s AND vessel = %s",
                (pilot_time, target["user_id"], port, vessel),
            )
        conn.commit()
    print(f"⏰ {vessel}({port}) 도선 {left}분 전 알림 발송")
    return f"reminded_{left}min"


# ── 푸시 알림 전송 ──────────────────────────

def send_push_notification(fcm_token: str, title: str, body: str, data: dict = None, channel_id: str = None):
    if not _firebase_initialized:
        print("Firebase 미초기화 - 알림 전송 건너뜀")
        return False
    try:
        # 도선예보 "등록"과 "변경"은 서로 다른 알림음을 쓴다.
        # Android 8+ 에서는 채널 생성 시점에 고정된 소리가 우선 적용되므로
        # (앱에서 pilot_registered_channel / schedule_changed_channel 을 미리 만들어둠)
        # channel_id 로 구분해서 보내고, 구버전 Android를 위해 sound 필드도 함께 지정한다.
        android_notification_kwargs = {"priority": "high"}
        if channel_id:
            android_notification_kwargs["channel_id"] = channel_id
            android_notification_kwargs["sound"] = channel_id.replace("_channel", "")
        else:
            android_notification_kwargs["sound"] = "default"

        message = messaging.Message(
            notification=messaging.Notification(title=title, body=body),
            data={k: str(v) for k, v in (data or {}).items()},
            token=fcm_token,
            android=messaging.AndroidConfig(
                priority="high",
                notification=messaging.AndroidNotification(**android_notification_kwargs),
            ),
        )
        response = messaging.send(message)
        print(f"✅ 푸시 발송 성공: {response}")
        return True
    except Exception as e:
        print(f"❌ 푸시 발송 실패: {e}")
        return False


@app.on_event("startup")
async def startup():
    try:
        init_db()
    except Exception as e:
        print(f"DB 초기화 실패: {e}")
    init_firebase()
    # AIS 백그라운드 리스너: 서버가 켜져 있는 동안 계속 한반도 연안 AIS 신호를
    # 수신해서 캐시에 쌓아둔다. 검색 시점에는 이 캐시를 즉시 조회하므로
    # 매 요청마다 새로 연결해서 짧게 기다리는 것보다 훨씬 빠르고 적중률이 높다.
    asyncio.create_task(start_ais_listener())


# ── 타겟 모니터링 (Cron Job에서 호출) ──────────

@app.api_route("/monitor", methods=["GET", "POST"])
async def monitor_targets():
    """외부 크론(GitHub Actions)이 주기적으로 호출 - 타겟 선박 체크 후 변경/임박 시 푸시 발송"""
    if _monitor_lock.locked():
        return {"checked": 0, "results": [], "skipped": "이전 점검이 아직 실행 중"}
    async with _monitor_lock:
        return await _run_monitor()


async def _run_monitor():
    results = []
    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                # 타겟 저장 시점에 FCM 토큰이 아직 없었던 경우(앱 첫 실행 직후)를 위해
                # fcm_tokens 테이블의 최신 토큰으로 보완한다.
                cur.execute("""
                    SELECT t.user_id, t.port, t.vessel, t.last_key,
                           COALESCE(NULLIF(t.fcm_token, ''), f.token) AS fcm_token,
                           COALESCE(t.missing_count, 0) AS missing_count,
                           t.reminded_for
                    FROM targets t
                    LEFT JOIN fcm_tokens f ON f.user_id = t.user_id
                    WHERE COALESCE(NULLIF(t.fcm_token, ''), f.token) IS NOT NULL
                """)
                targets = [dict(r) for r in cur.fetchall()]

        print(f"🔍 타겟 {len(targets)}개 확인 중...")

        for target in targets:
            try:
                user_id = target['user_id']
                port = target['port']
                vessel = target['vessel']
                last_key = target.get('last_key') or ''
                fcm_token = target['fcm_token']

                # 도선예보 조회
                loop = asyncio.get_event_loop()
                pob = await loop.run_in_executor(None, get_pob_info, vessel, port)

                if not pob:
                    # 스크래퍼 일시 장애로 한두 번 비는 건 무시하고, 연속 3회(≈45분) 이상
                    # 안 보이면 예보가 실제로 내려간 것으로 보고 last_key를 초기화한다.
                    missing = int(target.get('missing_count') or 0) + 1
                    with get_conn() as conn:
                        with conn.cursor() as cur:
                            if missing >= MISSING_RESET_THRESHOLD and last_key:
                                cur.execute(
                                    "UPDATE targets SET last_key = '', missing_count = 0 WHERE user_id = %s AND port = %s AND vessel = %s",
                                    (user_id, port, vessel)
                                )
                                print(f"🧹 {vessel}({port}) 예보 {missing}회 연속 미검출 → 상태 초기화")
                            else:
                                cur.execute(
                                    "UPDATE targets SET missing_count = %s WHERE user_id = %s AND port = %s AND vessel = %s",
                                    (missing, user_id, port, vessel)
                                )
                        conn.commit()
                    results.append({"vessel": vessel, "status": "no_data", "missing_count": missing})
                    continue

                # 현재 상태 키 생성
                current_key = json.dumps(pob, ensure_ascii=False, sort_keys=True)

                if current_key != last_key:
                    if last_key == '':
                        title = f"🚢 {vessel} 도선예보 등록"
                        summary = summarize_pob(pob)
                        body = f"{port}항 · {summary}" if summary else f"{port} 항구 도선예보가 등록됐습니다"
                        channel_id = "pilot_registered_channel"
                    else:
                        title = f"🔔 {vessel} 도선예보 변경"
                        # 무엇이 바뀌었는지(예: 작업일시 14:00 → 16:30)를 본문에 보여준다
                        body = describe_changes(last_key, pob) or f"{port} 항구 도선예보 내용이 변경됐습니다"
                        channel_id = "schedule_changed_channel"

                    send_push_notification(fcm_token, title, body, {
                        "port": port,
                        "vessel": vessel,
                    }, channel_id=channel_id)

                    with get_conn() as conn:
                        with conn.cursor() as cur:
                            cur.execute(
                                "UPDATE targets SET last_key = %s, missing_count = 0 WHERE user_id = %s AND port = %s AND vessel = %s",
                                (current_key, user_id, port, vessel)
                            )
                        conn.commit()

                    print(f"✅ {vessel}({port}) 변경 감지 → 푸시 발송")
                    entry = {"vessel": vessel, "status": "pushed"}
                else:
                    if int(target.get('missing_count') or 0) > 0:
                        with get_conn() as conn:
                            with conn.cursor() as cur:
                                cur.execute(
                                    "UPDATE targets SET missing_count = 0 WHERE user_id = %s AND port = %s AND vessel = %s",
                                    (user_id, port, vessel)
                                )
                            conn.commit()
                    print(f"⏸ {vessel}({port}) 변경 없음")
                    entry = {"vessel": vessel, "status": "no_change"}

                # 도선 임박 알림 (변경 알림과 별개로 판단)
                try:
                    reminder = maybe_send_reminder(target, pob, fcm_token)
                    if reminder:
                        entry["reminder"] = reminder
                except Exception as e:
                    print(f"임박 알림 판단 실패({vessel}): {e}")
                results.append(entry)

            except Exception as e:
                print(f"❌ {target['vessel']} 체크 실패: {e}")
                results.append({"vessel": target['vessel'], "status": "error", "error": str(e)})

    except Exception as e:
        print(f"❌ 모니터링 오류: {e}")
        raise HTTPException(status_code=500, detail=str(e))

    return {"checked": len(results), "results": results}


@app.api_route("/", methods=["GET", "HEAD"])
def root():
    # UptimeRobot 등 무료 플랜 모니터링 서비스는 HEAD 요청으로 상태를 체크하는데,
    # HEAD를 명시적으로 허용하지 않으면 405가 나서 "다운"으로 오탐지된다.
    return {"status": "ok", "message": "Spark Mobile API"}


@app.get("/ports")
def get_ports():
    return {"ports": SUPPORTED_PORTS}


@app.get("/schedule/{port}")
async def get_schedule(port: str):
    """지정 항구의 도선예보 전체 목록 (타겟 등록 없이 바로 조회)"""
    port_key = None
    for p in SUPPORTED_PORTS:
        if p.upper() == port.upper():
            port_key = p
            break

    if not port_key:
        raise HTTPException(status_code=400, detail=f"지원하지 않는 항구: {port}")

    loop = asyncio.get_event_loop()
    vessels = await loop.run_in_executor(None, get_all_pob_info, port_key)

    return {"port": port_key, "count": len(vessels), "vessels": vessels}


@app.get("/search/{port}/{vessel}")
async def search_vessel(port: str, vessel: str):
    port = port.upper()
    vessel = vessel.upper()

    if port not in [p.upper() for p in SUPPORTED_PORTS]:
        raise HTTPException(status_code=400, detail=f"지원하지 않는 항구: {port}")

    loop = asyncio.get_event_loop()
    # pob(도선예보)와 ais(실시간 위치)를 동시에 조회해서 대기 시간을 줄인다
    # (순차 실행 시 최악의 경우 pob 시간 + AIS 최대 대기(10초)가 그대로 더해짐)
    pob, (ais, last_ais) = await asyncio.gather(
        loop.run_in_executor(None, get_pob_info, vessel, port),
        loop.run_in_executor(None, lookup_ais, vessel),
    )

    return {
        "port": port,
        "vessel": vessel,
        "pob": pob,
        "ais": ais,            # 최근 30분 이내 실시간 위치 (없으면 null)
        "last_ais": last_ais,  # 실시간이 없을 때: 마지막으로 잡혔던 위치 (stale=true, updated_at 포함)
    }


# ── FCM 토큰 등록 ──────────────────────────

class FcmTokenRequest(BaseModel):
    user_id: str
    token: str


@app.post("/fcm-token")
def register_fcm_token(req: FcmTokenRequest):
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO fcm_tokens (user_id, token, updated_at)
                    VALUES (%s, %s, NOW())
                    ON CONFLICT (user_id)
                    DO UPDATE SET token = EXCLUDED.token, updated_at = NOW()
                """, (req.user_id, req.token))
                cur.execute("""
                    UPDATE targets SET fcm_token = %s WHERE user_id = %s
                """, (req.token, req.user_id))
            conn.commit()
        return {"success": True, "message": "FCM 토큰 등록 완료"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── 테스트용 푸시 발송 ──────────────────────

class PushTestRequest(BaseModel):
    user_id: str
    title: str = "🚢 테스트 알림"
    body: str = "Spark 푸시 알림 테스트입니다"


@app.post("/push-test")
def push_test(req: PushTestRequest):
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT token FROM fcm_tokens WHERE user_id = %s", (req.user_id,))
                row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="해당 user_id의 FCM 토큰 없음")
        success = send_push_notification(row[0], req.title, req.body)
        return {"success": success, "token_preview": row[0][:20] + "..."}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── 타겟 CRUD ──────────────────────────────

class TargetRequest(BaseModel):
    user_id: str
    port: str
    vessel: str
    fcm_token: str = ""


@app.post("/targets")
def add_target(req: TargetRequest):
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                # xmax = 0 이면 새 행(INSERT), 아니면 기존 행 갱신(UPDATE) — 앱이
                # "등록됨"과 "이미 등록된 선박"을 구분해서 안내할 수 있게 created 플래그를 준다.
                # 기존 토큰이 비어 있지 않은데 빈 토큰이 오면 덮어쓰지 않는다.
                cur.execute("""
                    INSERT INTO targets (user_id, port, vessel, fcm_token)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (user_id, port, vessel)
                    DO UPDATE SET fcm_token = COALESCE(NULLIF(EXCLUDED.fcm_token, ''), targets.fcm_token)
                    RETURNING (xmax = 0) AS created
                """, (req.user_id, req.port.upper(), req.vessel.upper(), req.fcm_token))
                created = bool(cur.fetchone()[0])
            conn.commit()
        return {"success": True, "created": created}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/targets")
def remove_target(req: TargetRequest):
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM targets WHERE user_id=%s AND port=%s AND vessel=%s",
                    (req.user_id, req.port.upper(), req.vessel.upper())
                )
                deleted = cur.rowcount > 0
            conn.commit()
        return {"success": deleted}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/targets/{user_id}")
def get_targets(user_id: str):
    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("SELECT * FROM targets WHERE user_id=%s", (user_id,))
                rows = [dict(r) for r in cur.fetchall()]
        return {"targets": rows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/targets/{user_id}/details")
async def get_target_details(user_id: str):
    """타겟 목록 + 각 선박의 도선예보/AIS 위치를 한 번에 돌려준다.
    (타겟 탭 카드와 '전체 지도' 화면용 — 선박마다 /search를 따로 부르지 않아도 된다)"""
    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("SELECT port, vessel, last_key FROM targets WHERE user_id=%s ORDER BY id", (user_id,))
                rows = [dict(r) for r in cur.fetchall()]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    loop = asyncio.get_event_loop()
    # 항구별 예보 목록은 한 번씩만 긁는다 (같은 항구 타겟이 여러 척이어도 1회)
    ports = sorted({r["port"] for r in rows if r["port"] in SUPPORTED_PORTS})
    schedules = await asyncio.gather(*[loop.run_in_executor(None, get_all_pob_info, p) for p in ports])
    by_port = dict(zip(ports, schedules))

    def find_pob(port: str, vessel: str):
        v = vessel.strip().upper()
        for item in by_port.get(port, []):
            if v and v in str(item.get("ship_name", "")).upper():
                return item
        return None

    ais_pairs = await asyncio.gather(*[loop.run_in_executor(None, lookup_ais, r["vessel"]) for r in rows])

    targets = []
    for r, (ais, last_ais) in zip(rows, ais_pairs):
        targets.append({
            "port": r["port"],
            "vessel": r["vessel"],
            "watching": bool(r.get("last_key")),
            "pob": find_pob(r["port"], r["vessel"]),
            "ais": ais,
            "last_ais": last_ais,
        })
    return {"targets": targets}
