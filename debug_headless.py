"""
debug_headless.py — Run this on GitHub Actions to dump page HTML
Add to workflow temporarily, check the output in Actions logs.
"""
import asyncio
import os
from playwright.async_api import async_playwright

URL = "https://www.shiksha.com/university/woxsen-university-hyderabad-138295/ranking"

async def debug():
    async with async_playwright() as p:
        headless = os.getenv("HEADLESS", "false").lower() == "true"
        browser = await p.chromium.launch(
            headless=headless,
            args=["--disable-blink-features=AutomationControlled",
                  "--no-sandbox", "--disable-dev-shm-usage"]
        )
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 900},
            locale="en-IN", timezone_id="Asia/Kolkata",
        )
        await context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )
        page = await context.new_page()

        print(f"Headless mode: {headless}")
        print(f"Loading: {URL}")
        await page.goto(URL, wait_until="domcontentloaded", timeout=90000)
        await page.wait_for_timeout(10000)

        # Count bc4a0d elements
        count = await page.evaluate("() => document.querySelectorAll('.bc4a0d').length")
        print(f".bc4a0d elements found: {count}")

        # Count all tables
        tables = await page.evaluate("() => document.querySelectorAll('table').length")
        print(f"Tables found: {tables}")

        # Check if page has any rank-like content
        has_rank = await page.evaluate("""() => document.body.innerText.includes('Rank publisher')""")
        print(f"Page contains 'Rank publisher': {has_rank}")

        has_outlook = await page.evaluate("""() => document.body.innerText.includes('Outlook')""")
        print(f"Page contains 'Outlook': {has_outlook}")

        # Page title
        title = await page.title()
        print(f"Page title: {title}")

        # Dump first 3000 chars of body text
        body_text = await page.evaluate("() => document.body.innerText.slice(0, 3000)")
        print(f"\nBody text (first 3000 chars):\n{body_text}")

        await browser.close()

asyncio.run(debug())
