import asyncio
from playwright.async_api import async_playwright

async def probe():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--disable-blink-features=AutomationControlled"])
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/131.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080}, locale="ru-RU",
        )
        page = await context.new_page()
        await page.add_init_script('Object.defineProperty(navigator, "webdriver", {get: () => undefined});')
        base = "https://xn--80az8a.xn--d1aqf.xn--p1ai"

        # First go to an object page to pass anti-bot
        await page.goto(f"{base}/сервисы/каталог-новостроек/объект/52403", timeout=60000, wait_until="domcontentloaded")
        await page.wait_for_timeout(8000)

        # Now go to apartment detail
        await page.goto(f"{base}/сервисы/каталог-квартир/квартира/896d40a05607b9ac9a292b008fa503d4", timeout=60000, wait_until="domcontentloaded")
        await page.wait_for_timeout(5000)

        text = await page.evaluate("() => document.body.innerText.substring(0, 5000)")
        lines = [l.strip() for l in text.split("\n") if l.strip()]
        for i, line in enumerate(lines):
            print(f"{i}: {repr(line)}")
            if i > 40:
                break

        # Also intercept API calls
        print("\n--- Network requests ---")
        api_urls = []
        page.on('request', lambda req: api_urls.append(req.url) if 'api' in req.url else None)
        await page.reload(wait_until="domcontentloaded")
        await page.wait_for_timeout(5000)
        for u in api_urls:
            if 'yandex' not in u and 'vk.com' not in u:
                print(u)

        await browser.close()

asyncio.run(probe())
