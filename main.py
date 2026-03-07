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
        conn.commit()


@app.on_event("startup")
async def startup():
    try:
        init_db()
    except Exception as e:
        print(f"DB 초기화 실패: {e}")


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
