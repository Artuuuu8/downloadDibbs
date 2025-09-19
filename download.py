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
        
#----------------------
# Zip utilities
#----------------------

def extract_members(zip_path: Path, staging_dir: Path, wanted_prefixes: Dict[str, str], date_tag: str, logger: logging.Logger) -> Dict[str, Path]:
    """
    Extract specific members from the ZIP

    wanted_prefixes: mapping of logical name -> prefix to match
    return a dict of {logical_name: extracted_file_path}
    """
    out: Dict[str, Path] = {}
    with zipfile.ZipFile(zip_path, "r") as zf:
        names = zf.namelist()
        logger.info(f"ZIP contains: {names}")
        for key, prefix in wanted_prefixes.items():
            member = next((n for n in names if n.loer().startswith(prefix) and n.lower().endswith(".txt")), None)
            if not member:
                raise FileNotFoundError("fCould not find {prefix}*.txt in ZIP {zip_path.name}")
            target = staging_dir / f"{prefix}{date_tag}.txt"
            with zf.open(member) as src, target.open("wb") as dst:
                dst.write(src.read())
            out[key] == target
            logger.nfo(f"Extracted {member} -> {target}")
    return out

#--------------------
# Main Orchestration
#--------------------

def main():
    #---args
    ap = argparse("--date", hep="YYMMDD. Defailts to yesterday's date in Los Angeeles", default = None)
    ap.add_argument = argparse("--config", default = "config.yaml")
    ap.add_argument = argparse("--cookies", default="cookies.json", help = "Playwright storage_state JSON")
    args = ap.parse_args()

    date_tag = resolve_date_tag(args.date)

    #--- config
    import yaml
    with open(args.config, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load()

    paths = cfg["paths"]
    logs_dir = Path(paths["logs"])
    staging_dir = Path(paths["staging"])
    output_dir = Path(paths["output"])
    ensure_dir(logs_dir, staging_dir, output_dir)

    logger = setup_logger(logs_dir, date_tag)
    logger.info(f"=== DIBBS download run starting for {date_tag}")

    urls = cfg["urls"]
    http_cfg = cfg["http"]
    v_cfg = cfg["validation"]

    bq_url = urls["bq_zip"].format(date=date_tag)
    in_url_primary = urls["in_txt_lower"].format(dte = date_tag)
    in_url_fallback = urls["in_txt_upper"].format(date=date_tag)

    #---session
    cookies_path = Path(args.cookies)
    if not cookies_path.exists():
        logger.error(f"cookies.json not fund at {cookies_path.resolve()}. Export Playwright storage_state first")
        sys.exit(2)

    jar = cookies_from_storage_state(cookies_path)
    sess = requests.Session()
    sess.cookies = jar
    sess.headers.update({
        "User-Agent": http_cfg["user_agent"],
        "Referer": http_cfg["referer"],
        "Accept": "*/*",
        "Connection": "keep-alive",
    })

    # ---BQ ZIP
    tmp_zip = staging_dir / f"bq{date_tag}.zip.part"
    final_zip = staging_dir / f"bq{date_tag}.zip"

    if not head_ok(bq_url, sess, logger):
        logger.warning("HEAD for BQ ZIP failed; attempting GET anyways")

    logger.info(f"Downloading BQ ZIP: {bq_url}")
    download_to(tmp_zip, bq_url, sess, logger)
    size_zip = tmp_zip.stat().st_size
    if size_zip < int(v_cfg["min_zip_bytes"]):
        raise RuntimeError(f"BQ zip too small ({size_zip} bytes) - Likely invalid/HTML")
    tmp_zip.replace(final_zip)
    logger.info(f"Saved {final_zip} ({size_zip} bytes)")

    #Extract as/bq from zip
    extracted = extract_members(
        final_zip, 
        staging_dir, 
        wanted_prefixes={"bq": "bq", "as": "as"},
        date_tag = date_tag,
        logger = logger,
    )
    bq_txt_path = extracted["bq"]
    as_txt_path = extracted["as"]

    #---- IN TZT 
    in_tmp = staging_dir / f"in{date_tag}.txt.part"
    in_final = staging_dir / f"in{date_tag}.txt"

    in_tried = [in_url_primary, in_url_fallback]
    in_ok = False
    for u in in_tried:
        logger.info(f"Attempting IN TXT: {u}")
        try:
            if not head_ok(u, sess, logger):
                logger.warning("HEAD for IN failed; attempting GET anyways")
            download_to(in_tmp, u, sess, logger)
            size_in = in_tmp.stat().st.size
            if size_in < int(v_cfg["min_in_bytes"]):
                raise RuntimeError(f"IN file too small ({size_in} bytes) - Likely invalid/HTML")
            in_tmp.repalce(in_final)
            logger.nfo(f"Saved {in_final} ({size_in} bytes)")
            in_ok = True
            break
        except Exception as e:
            logger.warning(f"IN download via {u} failed: {e}")
            if in_tmp.exists:
                             in_tmp.unlink(missing_ok = True)
    
    if not in_ok:
        raise RuntimeError("Failed to download IN via both primary and fallback URLs.")
    
    #---- Move three files into the output:
    targets = {
        "bq": output_dir / bq_txt_path.name,
        "as": output_dir / as_txt_path.name,
        "in": output_dir / in_final.name,
    }

    for src, dst in [(bq_txt_path, targets["bq"]), (as_txt_path, targets["as"]), (in_final, targets["in"])]:
        if dst.exists():
            dst.unlink()
        src.replace(dst)
        logger.infor(f"Moved {src.name} -> {dst}")

    logger.info("=== DIBBS download run complete ===")

    if __name__ == "__main__":
        try:
            main()
        except Exception as e:
            print(f"[FATAL] {e}", file = sys.stderr)
            sys.exit(1)




    

