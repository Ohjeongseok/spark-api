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
import firebase_admin
from firebase_admin import credentials, messaging
from scraper.pilot_station import get_pob_info
from scraper.marinetraffic import get_vessel_info

app = FastAPI(title="Spark Mobile API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

DATABASE_URL = os.environ.get("DATABASE_URL", "")
SUPPORTED_PORTS = ["포항", "마산"]

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
            cur.execute("""
                CREATE TABLE IF NOT EXISTS fcm_tokens (
                    user_id TEXT PRIMARY KEY,
                    token TEXT NOT NULL,
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            """)
        conn.commit()


# ── 푸시 알림 전송 ──────────────────────────

def send_push_notification(fcm_token: str, title: str, body: str, data: dict = None):
    if not _firebase_initialized:
        print("Firebase 미초기화 - 알림 전송 건너뜀")
        return False
    try:
        message = messaging.Message(
            notification=messaging.Notification(title=title, body=body),
            data={k: str(v) for k, v in (data or {}).items()},
            token=fcm_token,
            android=messaging.AndroidConfig(
                priority="high",
                notification=messaging.AndroidNotification(
                    sound="default",
                    priority="high",
                ),
            ),
        )
        response = messaging.send(message)
        print(f"✅ 푸시 발송 성공: {response}")
        return True
    except Exception as e:
        print(f"❌ 푸시 발송 실패: {e}")
        return False


# ── 타겟 모니터링 백그라운드 태스크 ────────────

async def monitor_targets():
    """15분~1시간 랜덤 간격으로 타겟 선박 체크 후 변경 시 푸시 발송"""
    await asyncio.sleep(30)  # 서버 시작 후 30초 대기
    while True:
        try:
            print("🔍 타겟 모니터링 시작...")
            with get_conn() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                    cur.execute("SELECT * FROM targets WHERE fcm_token IS NOT NULL AND fcm_token != ''")
                    targets = [dict(r) for r in cur.fetchall()]

            print(f"📋 타겟 {len(targets)}개 확인 중...")

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
                        continue

                    # 현재 상태 키 생성 (예보 내용으로 변경 감지)
                    current_key = json.dumps(pob, ensure_ascii=False, sort_keys=True)

                    if current_key != last_key:
                        # 변경 감지!
                        if last_key == '':
                            # 새로 등록된 경우
                            title = f"🚢 {vessel} 도선예보 등록"
                            body = f"{port} 항구 도선예보가 등록됐습니다"
                        else:
                            # 내용 변경된 경우
                            title = f"🔔 {vessel} 도선예보 변경"
                            body = f"{port} 항구 도선예보 내용이 변경됐습니다"

                        # 푸시 발송
                        send_push_notification(fcm_token, title, body, {
                            "port": port,
                            "vessel": vessel,
                        })

                        # DB 업데이트
                        with get_conn() as conn:
                            with conn.cursor() as cur:
                                cur.execute(
                                    "UPDATE targets SET last_key = %s WHERE user_id = %s AND port = %s AND vessel = %s",
                                    (current_key, user_id, port, vessel)
                                )
                            conn.commit()

                        print(f"✅ {vessel}({port}) 변경 감지 → 푸시 발송")
                    else:
                        print(f"⏸ {vessel}({port}) 변경 없음")

                except Exception as e:
                    print(f"❌ {target['vessel']} 체크 실패: {e}")
                    continue

        except Exception as e:
            print(f"❌ 모니터링 오류: {e}")

        # 15분~1시간 랜덤 대기
        wait = random.randint(15 * 60, 60 * 60)
        print(f"⏰ 다음 체크까지 {wait // 60}분 대기...")
        await asyncio.sleep(wait)


@app.on_event("startup")
async def startup():
    try:
        init_db()
    except Exception as e:
        print(f"DB 초기화 실패: {e}")
    init_firebase()
    # 백그라운드 모니터링 시작
    asyncio.create_task(monitor_targets())


@app.get("/")
def root():
    return {"status": "ok", "message": "Spark Mobile API"}


@app.get("/ports")
def get_ports():
    return {"ports": SUPPORTED_PORTS}


@app.get("/search/{port}/{vessel}")
async def search_vessel(port: str, vessel: str):
    port = port.upper()
    vessel = vessel.upper()

    if port not in [p.upper() for p in SUPPORTED_PORTS]:
        raise HTTPException(status_code=400, detail=f"지원하지 않는 항구: {port}")

    loop = asyncio.get_event_loop()
    pob = await loop.run_in_executor(None, get_pob_info, vessel, port)
    ais = await loop.run_in_executor(None, get_vessel_info, vessel)

    return {
        "port": port,
        "vessel": vessel,
        "pob": pob,
        "ais": ais,
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
                cur.execute("""
                    INSERT INTO targets (user_id, port, vessel, fcm_token)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (user_id, port, vessel)
                    DO UPDATE SET fcm_token = EXCLUDED.fcm_token
                """, (req.user_id, req.port.upper(), req.vessel.upper(), req.fcm_token))
            conn.commit()
        return {"success": True}
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
