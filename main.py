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


@app.on_event("startup")
async def startup():
    try:
        init_db()
    except Exception as e:
        print(f"DB 초기화 실패: {e}")
    init_firebase()


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
    """앱 시작 시 FCM 토큰을 서버에 등록/갱신"""
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


# ── 푸시 알림 전송 함수 ─────────────────────

def send_push_notification(fcm_token: str, title: str, body: str, data: dict = None):
    """FCM 토큰으로 푸시 알림 전송"""
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


# ── 테스트용 푸시 발송 ──────────────────────

class PushTestRequest(BaseModel):
    user_id: str
    title: str = "🚢 테스트 알림"
    body: str = "Spark 푸시 알림 테스트입니다"


@app.post("/push-test")
def push_test(req: PushTestRequest):
    """개발용 푸시 테스트 엔드포인트"""
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
