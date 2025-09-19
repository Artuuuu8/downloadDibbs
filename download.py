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


# ---------------------------
# Helpers: fs, logging, dates
# ---------------------------

def ensure_dirs(*dirs: Path):
    for d in dirs:
        d.mkdir(parents=True, exist_ok=True)

def setup_logger(log_dir: Path, date_tag: str) -> logging.Logger:
    ensure_dirs(log_dir)
    log_path = log_dir / f"download_{date_tag}.log"
    logger = logging.getLogger("dibbs")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    fh = logging.FileHandler(log_path, encoding="utf-8")
    ch = logging.StreamHandler(sys.stdout)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    fh.setFormatter(fmt)
    ch.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger

def resolve_date_tag(cli_date: Optional[str]) -> str:
    if cli_date:
        if len(cli_date) != 6 or not cli_date.isdigit():
            raise ValueError("--date must be YYMMDD, e.g., 250903")
        return cli_date
    tz = pytz.timezone("America/Los_Angeles")
    yday = dt.datetime.now(tz=tz) - dt.timedelta(days=1)
    return yday.strftime("%y%m%d")


# ----------------------------------
# Cookies: Playwright storage_state
# ----------------------------------

def cookies_from_storage_state(storage_state_path: Path) -> RequestsCookieJar:
    """
    Load a Playwright storage_state JSON and convert to a requests CookieJar.
    """
    with storage_state_path.open("r", encoding="utf-8") as f:
        state = json.load(f)

    jar = RequestsCookieJar()
    for c in state.get("cookies", []):
        jar.set(
            name=c.get("name"),
            value=c.get("value"),
            domain=c.get("domain"),
            path=c.get("path", "/"),
            secure=c.get("secure", False),
            rest={"HttpOnly": c.get("httpOnly", False), "SameSite": c.get("sameSite", "")},
        )
    return jar


# ---------------------------
# HTTP: HEAD/GET + validation
# ---------------------------

def is_probably_html(sample: bytes) -> bool:
    s = sample.lstrip()
    return s.startswith(b"<") or s.startswith(b"<!")

def head_ok(url: str, session: requests.Session, logger: logging.Logger) -> bool:
    try:
        resp = session.head(url, allow_redirects=True, timeout=30)
        logger.info(f"HEAD {url} -> {resp.status_code} {resp.headers.get('Content-Type')} {resp.headers.get('Content-Length')}")
        return resp.status_code == 200
    except Exception as e:
        logger.error(f"HEAD failed for {url}: {e}")
        return False

def download_to(temp_path: Path, url: str, session: requests.Session, logger: logging.Logger) -> None:
    temp_path.parent.mkdir(parents=True, exist_ok=True)
    with session.get(url, stream=True, allow_redirects=True, timeout=180) as r:
        r.raise_for_status()
        with temp_path.open("wb") as f:
            for chunk in r.iter_content(chunk_size=65536):
                if chunk:
                    f.write(chunk)
    # Quick content sniff
    with temp_path.open("rb") as f:
        head = f.read(512)
    if is_probably_html(head):
        raise RuntimeError(f"Download appears to be HTML (likely consent page). URL: {url}")


# ----------------------
# Zip utilities
# ----------------------

def extract_members(zip_path: Path, staging_dir: Path, wanted_prefixes: Dict[str, str], date_tag: str, logger: logging.Logger) -> Dict[str, Path]:
    """
    Extract specific members from the ZIP.

    wanted_prefixes: mapping of logical name -> prefix to match (e.g. {"bq": "bq", "as": "as"})
    Returns a dict of {logical_name: extracted_file_path}
    """
    out: Dict[str, Path] = {}
    with zipfile.ZipFile(zip_path, "r") as zf:
        names = zf.namelist()
        logger.info(f"ZIP contains: {names}")
        for key, prefix in wanted_prefixes.items():
            member = next((n for n in names if n.lower().startswith(prefix) and n.lower().endswith(".txt")), None)
            if not member:
                raise FileNotFoundError(f"Could not find {prefix}*.txt in ZIP {zip_path.name}")
            target = staging_dir / f"{prefix}{date_tag}.txt"
            with zf.open(member) as src, target.open("wb") as dst:
                dst.write(src.read())
            out[key] = target
            logger.info(f"Extracted {member} -> {target}")
    return out


# --------------------
# Main orchestration
# --------------------

def main():
    # --- args
    ap = argparse.ArgumentParser(description="Download BQ.zip + IN.txt via cookies, extract as/bq, place all in output.")
    ap.add_argument("--date", help="YYMMDD (e.g., 250903). Defaults to yesterday in America/Los_Angeles.", default=None)
    ap.add_argument("--config", default="config.yaml")
    ap.add_argument("--cookies", default="cookies.json", help="Playwright storage_state JSON")
    args = ap.parse_args()

    date_tag = resolve_date_tag(args.date)

    # --- config
    import yaml
    with open(args.config, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    paths = cfg["paths"]
    logs_dir = Path(paths["logs"])
    staging_dir = Path(paths["staging"])
    output_dir = Path(paths["output"])
    ensure_dirs(logs_dir, staging_dir, output_dir)

    logger = setup_logger(logs_dir, date_tag)
    logger.info(f"=== DIBBS download run starting for {date_tag} ===")

    urls = cfg["urls"]
    http_cfg = cfg["http"]
    v_cfg = cfg["validation"]

    bq_url = urls["bq_zip"].format(date=date_tag)
    in_url_primary = urls["in_txt_lower"].format(date=date_tag)
    in_url_fallback = urls["in_txt_upper"].format(date=date_tag)

    # --- session
    cookies_path = Path(args.cookies)
    if not cookies_path.exists():
        logger.error(f"cookies.json not found at {cookies_path.resolve()}. Export Playwright storage_state first.")
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

    # --- BQ ZIP
    tmp_zip = staging_dir / f"bq{date_tag}.zip.part"
    final_zip = staging_dir / f"bq{date_tag}.zip"

    if not head_ok(bq_url, sess, logger):
        logger.warning("HEAD for BQ ZIP failed; attempting GET anyway.")

    logger.info(f"Downloading BQ ZIP: {bq_url}")
    download_to(tmp_zip, bq_url, sess, logger)
    size_zip = tmp_zip.stat().st_size
    if size_zip < int(v_cfg["min_zip_bytes"]):
        raise RuntimeError(f"BQ zip too small ({size_zip} bytes) — likely invalid/HTML.")
    tmp_zip.replace(final_zip)
    logger.info(f"Saved {final_zip} ({size_zip} bytes)")

    # Extract as/bq from ZIP
    extracted = extract_members(
        final_zip,
        staging_dir,
        wanted_prefixes={"bq": "bq", "as": "as"},
        date_tag=date_tag,
        logger=logger,
    )
    bq_txt_path = extracted["bq"]
    as_txt_path = extracted["as"]

    # --- IN TXT (try lowercase URL, then uppercase fallback)
    in_tmp = staging_dir / f"in{date_tag}.txt.part"
    in_final = staging_dir / f"in{date_tag}.txt"

    in_tried = [in_url_primary, in_url_fallback]
    in_ok = False
    for u in in_tried:
        logger.info(f"Attempting IN TXT: {u}")
        try:
            if not head_ok(u, sess, logger):
                logger.warning("HEAD for IN failed; attempting GET anyway.")
            download_to(in_tmp, u, sess, logger)
            size_in = in_tmp.stat().st_size
            if size_in < int(v_cfg["min_in_bytes"]):
                raise RuntimeError(f"IN file too small ({size_in} bytes) — likely invalid/HTML.")
            in_tmp.replace(in_final)
            logger.info(f"Saved {in_final} ({size_in} bytes)")
            in_ok = True
            break
        except Exception as e:
            logger.warning(f"IN download via {u} failed: {e}")
            if in_tmp.exists():
                in_tmp.unlink(missing_ok=True)

    if not in_ok:
        raise RuntimeError("Failed to download IN via both primary and fallback URLs.")

    # --- Move three files into output/
    targets = {
        "bq": output_dir / bq_txt_path.name,
        "as": output_dir / as_txt_path.name,
        "in": output_dir / in_final.name,
    }

    for src, dst in [(bq_txt_path, targets["bq"]), (as_txt_path, targets["as"]), (in_final, targets["in"])]:
        if dst.exists():
            dst.unlink()
        src.replace(dst)
        logger.info(f"Moved {src.name} -> {dst}")

    logger.info("=== DIBBS download run complete ===")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        print(f"[FATAL] {repr(e)}", file=sys.stderr)
        traceback.print_exc()
        sys.exit(1)
