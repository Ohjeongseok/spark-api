import os

BOT_TOKEN = os.environ.get("DISCORD_BOT_TOKEN", "").strip().strip('"').strip("'")
COMMAND_PREFIX = os.environ.get("COMMAND_PREFIX", "!").strip()
MARINETRAFFIC_API_KEY = os.environ.get("MARINETRAFFIC_API_KEY", "").strip()

PILOT_STATION_URLS = {
    "마산": {
        # 표시명: 마산/삼천포
        "url": "http://www.mspilot.co.kr/forecast_list_new.asp",
    },
    "포항": {
        "url": "http://www.dsmarine.co.kr/",
        "id": os.environ.get("POHANG_ID", "ag00001"),
        "pw": os.environ.get("POHANG_PW", "1"),
    },
    "부산": {
        "url": "http://www.busanpilot.co.kr/popup/monitoring",
    },
    "울산": {
        "url": "http://www.ulsanpilot.co.kr/main/",
    },
    "여수": {
        # 표시명: 여수/광양/하동 (한 페이지에서 3개 항구 통합 제공)
        "url": "http://www.yspilot.co.kr/forecast",
    },
}

REQUEST_TIMEOUT = 15
REQUEST_RETRY = 3
REQUEST_DELAY = 1.0

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8",
}
