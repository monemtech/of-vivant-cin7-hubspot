"""
Vivant Product Scraper — FastAPI microservice
Deploy on DigitalOcean droplet

Install:
    pip install fastapi uvicorn httpx beautifulsoup4 playwright python-dotenv
    playwright install chromium

Run (dev):
    uvicorn scraper:app --host 0.0.0.0 --port 8001 --reload

Run (prod via systemd or pm2):
    uvicorn scraper:app --host 0.0.0.0 --port 8001 --workers 2
"""

import re
import httpx
import asyncio
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from bs4 import BeautifulSoup
from typing import Optional
import logging

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

app = FastAPI(title="Monem Tech Product Scraper", version="1.0.0")

# Allow your HTML app and any Vivant/Shopify domain to call this
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # Lock down to specific domains in production
    allow_methods=["GET"],
    allow_headers=["*"],
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/json,*/*",
    "Accept-Language": "en-US,en;q=0.9",
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def shopify_handle(url: str) -> Optional[str]:
    m = re.search(r"/products/([a-zA-Z0-9_-]+)", url)
    return m.group(1) if m else None

def shopify_base(url: str) -> Optional[str]:
    m = re.match(r"(https?://[^/]+)", url)
    return m.group(1) if m else None

def clean_html(raw: str, max_len: int = 160) -> str:
    text = re.sub(r"<[^>]+>", " ", raw)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:max_len]

def parse_shopify_product(prod: dict, original_url: str) -> dict:
    images = prod.get("images") or []
    img = images[0]["src"].split("?")[0] if images else ""
    variants = prod.get("variants") or []
    price = f"${float(variants[0]['price']):.2f}" if variants else ""
    desc = clean_html(prod.get("body_html") or "")
    return {
        "name": prod.get("title", ""),
        "img": img,
        "price": price,
        "desc": desc,
        "url": original_url,
        "source": "shopify_json",
    }

def parse_og_tags(html: str, original_url: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")

    def og(prop: str) -> str:
        tag = soup.find("meta", property=prop) or soup.find("meta", attrs={"name": prop})
        return (tag.get("content") or "").strip() if tag else ""

    title = og("og:title") or og("twitter:title") or (soup.title.string if soup.title else "")
    img   = og("og:image") or og("twitter:image")
    desc  = og("og:description") or og("twitter:description") or og("description")

    # Shopify-specific: grab price from meta tag
    price_tag = soup.find("meta", property="product:price:amount")
    currency_tag = soup.find("meta", property="product:price:currency")
    price = ""
    if price_tag:
        amount = price_tag.get("content", "")
        symbol = "$" if (not currency_tag or currency_tag.get("content") == "USD") else (currency_tag.get("content", "") + " ")
        try:
            price = f"{symbol}{float(amount):.2f}"
        except ValueError:
            price = amount

    return {
        "name": (title or "").strip(),
        "img": img,
        "price": price,
        "desc": desc[:160],
        "url": original_url,
        "source": "og_tags",
    }


# ── Scraping strategies ───────────────────────────────────────────────────────

async def strategy_shopify_json(url: str, client: httpx.AsyncClient) -> dict:
    """Fastest: Shopify product JSON endpoint — no JS needed."""
    handle = shopify_handle(url)
    base   = shopify_base(url)
    if not handle or not base:
        raise ValueError("Not a Shopify product URL")
    json_url = f"{base}/products/{handle}.json"
    log.info(f"[S1] Trying Shopify JSON: {json_url}")
    r = await client.get(json_url, headers=HEADERS, timeout=5)
    r.raise_for_status()
    data = r.json()
    if "product" not in data:
        raise ValueError("No product key in response")
    return parse_shopify_product(data["product"], url)


async def strategy_og_tags(url: str, client: httpx.AsyncClient) -> dict:
    """Fast: fetch page HTML and parse OG/meta tags."""
    clean_url = url.split("?")[0]
    log.info(f"[S2] Fetching OG tags: {clean_url}")
    r = await client.get(clean_url, headers=HEADERS, timeout=8, follow_redirects=True)
    r.raise_for_status()
    result = parse_og_tags(r.text, url)
    if not result["name"] and not result["img"]:
        raise ValueError("No useful OG data found")
    return result


async def strategy_playwright(url: str) -> dict:
    """Fallback: headless Chromium for JS-rendered or Cloudflare-protected pages."""
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        raise ValueError("Playwright not installed")

    log.info(f"[S3] Playwright headless: {url}")
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page(
            user_agent=HEADERS["User-Agent"],
            extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
        )
        await page.goto(url.split("?")[0], wait_until="domcontentloaded", timeout=15000)
        html = await page.content()
        await browser.close()

    result = parse_og_tags(html, url)
    result["source"] = "playwright"
    if not result["name"] and not result["img"]:
        raise ValueError("Playwright: no useful data found")
    return result


# ── Main endpoint ─────────────────────────────────────────────────────────────

@app.get("/scrape")
async def scrape(url: str = Query(..., description="Product page URL to scrape")):
    """
    Scrape product metadata from any URL.
    Tries strategies in order: Shopify JSON → OG tags → Playwright.
    Returns first successful result.
    """
    if not url.startswith("http"):
        raise HTTPException(status_code=400, detail="Invalid URL")

    async with httpx.AsyncClient() as client:
        for strategy in [
            lambda: strategy_shopify_json(url, client),
            lambda: strategy_og_tags(url, client),
            lambda: strategy_playwright(url),
        ]:
            try:
                result = await strategy()
                log.info(f"Success via {result.get('source')}: {result.get('name')}")
                return {"status": "success", "data": result}
            except Exception as e:
                log.warning(f"Strategy failed: {e}")
                continue

    raise HTTPException(
        status_code=422,
        detail="Could not extract product data from this URL after all strategies."
    )


@app.get("/health")
async def health():
    return {"status": "ok", "service": "product-scraper"}
