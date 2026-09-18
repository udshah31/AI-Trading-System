"""
Launch a Browser Use Cloud managed browser — 15 min in United States

Uses server-side BROWSER_USE_API_KEY only (never hard-coded, never logged).
Billing runs until browser is stopped or timeout expires — always stop.

Config: timeout=15, proxy_country_code="us" (per project spec)
Docs: https://docs.browser-use.com/cloud/browser/quickstart
"""
import asyncio
import os
from browser_use_sdk.v4 import AsyncBrowserUse


async def main():
    # Server-side key only; create at https://cloud.browser-use.com/settings?tab=api-keys&new=1
    if not os.getenv("BROWSER_USE_API_KEY"):
        raise SystemExit("BROWSER_USE_API_KEY not set (keys start with bu_)")

    async with AsyncBrowserUse(api_key=os.environ["BROWSER_USE_API_KEY"]) as client:
        browser = await client.browsers.create(
            proxy_country_code="us",
            timeout=15,
        )
        try:
            print(browser.cdp_url)
            print(browser.live_url)
            # --- your Playwright/Puppeteer/CDP work here ---
            # e.g. async with playwright.chromium.connect_over_cdp(browser.cdp_url) as ...
            await asyncio.sleep(2)  # placeholder
        finally:
            # Billing runs until stopped — this refunds unused time. Do NOT use client.close() only.
            await client.browsers.stop(browser.id)
            print(f"Stopped browser {browser.id}")


if __name__ == "__main__":
    asyncio.run(main())
