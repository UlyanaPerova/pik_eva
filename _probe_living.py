import asyncio
import json
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
        await page.goto(f"{base}/сервисы/каталог-новостроек/объект/52403", timeout=60000, wait_until="domcontentloaded")
        await page.wait_for_timeout(8000)

        # Get full apartment data from table endpoint
        raw = await page.evaluate(
            """async (url) => {
                const resp = await fetch(url);
                if (!resp.ok) return {error: resp.status};
                return await resp.json();
            }""",
            f"{base}/portal-kn/api/sales/portal/table?externalId=52403",
        )

        if "entrances" in raw:
            ent = raw["entrances"][0]
            for floor in ent["floors"]:
                for flat in floor["flats"]:
                    if flat.get("rooms") is not None:
                        print("ALL KEYS of apartment:", sorted(flat.keys()))
                        print("FULL apartment data:")
                        print(json.dumps(flat, indent=2, ensure_ascii=False))
                        break
                else:
                    continue
                break

        # Also try the premises endpoint for field comparison
        raw2 = await page.evaluate(
            """async (url) => {
                const resp = await fetch(url);
                if (!resp.ok) return {error: resp.status};
                return await resp.json();
            }""",
            f"{base}/portal-kn/api/kn/objects/52403/flats?flatGroupType=premises&limit=1&offset=0",
        )
        if isinstance(raw2, dict) and "data" in raw2 and raw2["data"]:
            print("\nPREMISES keys:", sorted(raw2["data"][0].keys()))

        # Try to click on a specific apartment on the page to see if there's more data
        # Check the apartment detail page
        ent = raw["entrances"][0]
        elem_id = None
        for floor in ent["floors"]:
            for flat in floor["flats"]:
                if flat.get("rooms") is not None:
                    elem_id = flat.get("elemId")
                    break
            if elem_id:
                break

        if elem_id:
            detail_url = f"{base}/сервисы/каталог-квартир/квартира/{elem_id}"
            print(f"\nTrying apartment detail page: {detail_url}")
            await page.goto(detail_url, timeout=60000, wait_until="domcontentloaded")
            await page.wait_for_timeout(5000)
            text = await page.evaluate("() => document.body.innerText.substring(0, 3000)")
            lines = [l.strip() for l in text.split("\n") if l.strip()]
            for i, line in enumerate(lines):
                if any(kw in line.lower() for kw in ["жилая", "площадь", "комнат", "общая"]):
                    print(f"LINE {i}: {line}")

        await browser.close()

asyncio.run(probe())
