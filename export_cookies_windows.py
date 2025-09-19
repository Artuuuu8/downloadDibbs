# export_cookies_windows.py
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
import time, sys, pathlib

START_URL = "https://www.dibbs.bsm.dla.mil/Rfq/RfqFileDefs.aspx"
STATE_PATH = "cookies.json"
SCREENSHOT = "consent_screenshot.png"
TRACE_ZIP = "trace.zip"

BUTTON_TEXTS = [
    "I Accept", "Accept", "I agree", "Agree", "Consent", "Continue", "Proceed", "OK", "Acknowledge"
]

def click_any_consent_button(page):
    # Try a variety of selectors that commonly appear on consent banners
    candidates = []
    # Buttons by text
    for t in BUTTON_TEXTS:
        candidates.append(page.get_by_role("button", name=t))
        candidates.append(page.locator(f"button:has-text('{t}')"))
        candidates.append(page.locator(f"text='{t}'"))
        candidates.append(page.get_by_role("link", name=t))
    # Common checkbox + continue variants
    candidates.append(page.locator("input[type=submit]"))
    candidates.append(page.locator("input[type=button]"))
    candidates.append(page.locator("button[type=submit]"))

    for loc in candidates:
        try:
            if loc.first.is_visible(timeout=500):
                loc.first.click(timeout=2000)
                return True
        except Exception:
            pass
    return False

def accept_banners_in_page(page, max_rounds=3):
    # Some sites stack multiple banners. Loop a few times.
    for _ in range(max_rounds):
        clicked = click_any_consent_button(page)
        # some banners re-render; small pause allows redirect/new content
        time.sleep(0.6)
        # If a redirect completes, page might change its load state
        try:
            page.wait_for_load_state("domcontentloaded", timeout=2000)
        except PWTimeout:
            pass
        if not clicked:
            break

def run():
    with sync_playwright() as p:
        # Use bundled Chromium; if you prefer your installed Chrome/Edge, use channel="chrome" or "msedge"
        browser = p.chromium.launch(headless=False, slow_mo=150)
        context = browser.new_context(
            ignore_https_errors=True,  # helps in corp networks with TLS inspection
            accept_downloads=True
        )

        # Start trace for easier debugging if it gets stuck
        context.tracing.start(screenshots=True, snapshots=True, sources=True)

        page = context.new_page()
        page.set_default_timeout(10000)

        try:
            page.goto(START_URL, wait_until="domcontentloaded")
        except PWTimeout:
            print("Initial navigation timed out; will still try to accept banners.", file=sys.stderr)

        # Handle banners in the main page first
        accept_banners_in_page(page)

        # Some flows open a popup/tab for the consent. Catch and process it.
        # We’ll watch briefly for a popup that loads after clicking anything.
        try:
            with page.expect_popup(timeout=3000) as pop_info:
                # Try clicking again to trigger any popup-based consent
                clicked = click_any_consent_button(page)
                if not clicked:
                    pass
            popup = pop_info.value
            popup.wait_for_load_state("domcontentloaded", timeout=5000)
            accept_banners_in_page(popup)
            # Close popup if it was only for consent
            try:
                popup.close()
            except Exception:
                pass
        except PWTimeout:
            # No popup appeared — that’s fine
            pass

        # Final pass: if the site navigated again, accept any remaining banners
        accept_banners_in_page(page)

        # Optional: ensure we’re on the RFQ definitions/content (best-effort)
        # Don’t hard-fail here; we just want valid cookies stored.
        try:
            page.wait_for_load_state("networkidle", timeout=4000)
        except PWTimeout:
            pass

        # Save storage state (cookies + localStorage)
        context.storage_state(path=STATE_PATH)
        print(f"Saved cookies -> {STATE_PATH}")

        # Take a quick screenshot to confirm the final page state
        try:
            page.screenshot(path=SCREENSHOT, full_page=True)
            print(f"Saved screenshot -> {SCREENSHOT}")
        except Exception:
            pass

        # Stop trace, write it to a zip for debugging if needed
        try:
            context.tracing.stop(path=TRACE_ZIP)
            print(f"Saved trace -> {TRACE_ZIP}")
        except Exception:
            pass

        browser.close()

if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        print("FATAL:", e, file=sys.stderr)
        # Try to be helpful: if something exploded before we could stop tracing,
        # we won’t have a trace, but that’s OK—the stderr will at least show the exception.
        sys.exit(1)
