import os

BOT_TOKEN = os.environ.get("DISCORD_BOT_TOKEN", "").strip().strip('"').strip("'")
COMMAND_PREFIX = os.environ.get("COMMAND_PREFIX", "!").strip()
MARINETRAFFIC_API_KEY = os.environ.get("MARINETRAFFIC_API_KEY", "").strip()

PILOT_STATION_URLS = {
    "마산": {
        "url": "http://www.mspilot.co.kr/forecast_list_new.asp",
    },
    "포항": {
        "url": "http://www.dsmarine.co.kr/",
        "id": os.environ.get("POHANG_ID", "ag00001"),
        "pw": os.environ.get("POHANG_PW", "1"),
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
