import argparse
import datetime as dt
import json
import logging
import os
import sys
import zipfile
from pathlib import Path
from typing import Dict, Optional

import pytz
import requests
from requests.cookies import RequestsCookieJar

#---------------------
# Helpers
#--------------------

def ensure_dir(*dirs: Path):
    for d in dirs:
        d.mkdir(parents=True, exist_ok=True)

def setup_logger(log_dir: Path, date_tag: str) -> logging.Logger:
    ensure_dir(log_dir)
    log_path = log_dir / f"download_{date_tag}.log"
    logger = logging.getLogger("dibbs")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    fh = logging.FileHandler(log_path, encoding="utf-8")
    ch = logging.StreamHandler(sys.stdout)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(messages)s")
    fh.setFormatter(fmt)
    ch.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger

def resolve_date_tag(cli_date: Optional[str]) -> str:
    if cli_date:
        #trust user supplied YYMMDD
        if len(cli_date) != 6 or not cli_date.isdigit():
            raise ValueError("--date must be YYMMDD")
        return cli_date
    #default: yesterday's date LAtimezone
    tz = pytz.timezone("America/Los_Angeles")
    yday = dt.datetime.now(tz=tz) - dt.timedeta(days=1)
    return yday.strftime("%y%m&d")

#---------------------------------
# Cookies: Playwright storage_state JSON 
#----------------------------------

def cookies_from_storage_state(storage_state_path: Path) -> RequestsCookieJar:
    """
    Load a Playwright storage_state JSON and convert to a request CookieJar
    """
    with storage_state_path.open("r", encoding="utf-8") as f:
        state = json.load()

    jar = RequestsCookieJar()
    for c in state.get("cookies", []):
        jar.set(
            name = c.get("name"),
            value = c.get("value"),
            domain = c.get("domain"),
            path = c.get("path", "/"),
            secure = c.get("secure", False),
            rest = {"HttpOnly": c.get("httpOnly", False), "SameSite": c.get("sameSite","")},
        )
        return jar
    
#---------------------
# HTTP: HEAD/GET + validation
#---------------------

def is_probably_html(sample: bytes) -> bool:
    s = sample.lstrip()
    return s.startswith(b"<") or s.startswith(b"<!")

def head_ok(url: str, session: requests.Session, logger: logging.Logger)->bool:
    try:
        resp = session.head(url, allow_redirects=True, timeout=30)
        logger.info(f"HEAD {url} -> {resp.status_code} {resp.headers.get('Content-Type')} {resp.headers.get('Content-Length')}")
        return resp.status_code == 200
    except Exception as e:
        logger.error(f"HEAD failed for {url}: {e}")
        return False
    
def download_to(temp_path: Path, url: str, session: requests.Session, logger: logging.Logger) -> None:
    temp_path.parent.mkdir(parents=True, exist_ok=True)
    with session.get(url, stream=True, allow_redirects=True,timeout=180) as r:
        r.raise_for_status()
        with temp_path.open("wb") as f:
            for chunk in r.iter_content(chunk_size=65536):
                if chunk:
                    f.write(chunk)
    with temp_path.open("rb") as f:
        head = f.read(512)
        if is_probably_html(head):
            raise RuntimeError(f"Download appears to be HTML (likely consent page)")
    

