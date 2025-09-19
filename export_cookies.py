from playwright.sync_api import sync_playwright

with sync_playwright() as p:
    browser = p.chromium.launch(headless=False)
    context = browser.new_context()
    page = context.new_page()
    page.goto("https://www.dibbs.bsm.dla.mil/Rfq/RfqFileDefs.aspx")
    input("Manually accept the consent banner, then press Enter here...")
    context.storage_state(path="cookies.json")
    browser.close()
