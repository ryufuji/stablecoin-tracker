"""F1: Data Collection -- RSS feeds and CoinGecko price snapshots."""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Any

import feedparser
import requests

logger = logging.getLogger(__name__)

_COINGECKO_PRICE_URL = "https://api.coingecko.com/api/v3/simple/price"
_REQUEST_TIMEOUT = 15  # seconds

# ------------------------------------------------------------------
# Stablecoin relevance filter keywords
# ------------------------------------------------------------------
# Articles from general crypto feeds must match at least one keyword
# to be stored.  Feeds marked stablecoin_only: false bypass the filter.

_STABLECOIN_KEYWORDS: list[str] = [
    # Core terms
    "stablecoin", "stable coin", "stablecoins",
    "ステーブルコイン", "安定通貨",
    # Major projects
    "usdt", "tether", "usdc", "circle",
    "dai", "makerdao", "maker dao", "sky protocol",
    "busd", "tusd", "frax", "lusd", "gho", "aave",
    "usd0", "usual", "ethena", "usde", "susde",
    "pyusd", "paypal usd",
    "fdusd", "first digital",
    "jpyc", "gyen",
    "eurc", "eurs",
    # Peg / depeg
    "depeg", "de-peg", "peg stability", "ペッグ", "デペッグ",
    # Regulation & law
    "mica regulation", "mica rule", "clarity act",
    "stablecoin bill", "stablecoin regulation",
    "stablecoin legislation", "genius act",
    "payment stablecoin",
    "資金決済法", "電子決済手段",
    # Use cases & payments
    "stablecoin payment", "stablecoin settlement",
    "stablecoin remittance", "stablecoin transfer",
    "tokenized deposit", "トークン化預金",
    "cbdc",
    # Reserve / backing
    "reserve attestation", "proof of reserves",
    "reserve backing", "treasury backing",
    # Yield / defi
    "stablecoin yield", "stablecoin lending",
    "stablecoin interest", "stablecoin swap",
]

_KEYWORDS_PATTERN = re.compile(
    "|".join(re.escape(kw) for kw in _STABLECOIN_KEYWORDS),
    re.IGNORECASE,
)


def _is_stablecoin_related(title: str, raw_text: str) -> bool:
    """Return True if the article is relevant to stablecoins."""
    search_text = f"{title} {raw_text}"
    return bool(_KEYWORDS_PATTERN.search(search_text))


# ------------------------------------------------------------------
# F1-1  RSS Feed Collection
# ------------------------------------------------------------------


def _parse_feed(feed_cfg: dict[str, str], storage: Any | None, apply_filter: bool = True) -> list[dict[str, Any]]:
    """Parse a single RSS feed and return new (non-duplicate) article dicts."""
    name = feed_cfg["name"]
    url = feed_cfg["url"]
    # Feeds explicitly marked stablecoin_only: true skip the filter
    is_dedicated = feed_cfg.get("stablecoin_only", False)
    articles: list[dict[str, Any]] = []

    try:
        feed = feedparser.parse(url)
    except Exception:
        logger.exception("Failed to fetch RSS feed: %s (%s)", name, url)
        return articles

    if feed.bozo and not feed.entries:
        logger.warning("RSS feed returned no entries: %s (%s)", name, url)
        return articles

    skipped = 0
    for entry in feed.entries:
        link = entry.get("link", "")
        if not link:
            continue
        if storage and storage.is_duplicate(link):
            continue

        title = entry.get("title", "(no title)")
        raw_text = entry.get("summary") or entry.get("description") or ""

        # Filter: general feeds keep only stablecoin-related articles
        if apply_filter and not is_dedicated and not _is_stablecoin_related(title, raw_text):
            skipped += 1
            continue

        # Best-effort published timestamp
        published_at: str | None = None
        if ts := entry.get("published_parsed") or entry.get("updated_parsed"):
            try:
                published_at = datetime(*ts[:6], tzinfo=timezone.utc).isoformat()
            except Exception:
                pass

        articles.append(
            {
                "title": title,
                "url": link,
                "source": name,
                "published_at": published_at,
                "raw_text": raw_text,
            }
        )

    if skipped:
        logger.info("Feed %s: %d articles kept, %d filtered out (not stablecoin-related)", name, len(articles), skipped)
    else:
        logger.info("Feed %s: %d new articles", name, len(articles))
    return articles


# ------------------------------------------------------------------
# F1-3  Main collection entry point
# ------------------------------------------------------------------


def collect_articles(config: dict[str, Any], storage: Any | None = None) -> list[dict[str, Any]]:
    """Fetch all configured RSS feeds, persist new articles, and return them.

    Parameters
    ----------
    config:
        Parsed config.yaml dict (must contain ``rss_feeds``).
    storage:
        A :class:`Any` instance used for duplicate checks and persistence.

    Returns
    -------
    list[dict]
        Newly collected articles, each with keys
        ``{title, url, source, published_at, raw_text}``.
    """
    all_articles: list[dict[str, Any]] = []

    for feed_cfg in config.get("rss_feeds", []):
        try:
            new = _parse_feed(feed_cfg, storage)
            all_articles.extend(new)
        except Exception:
            logger.exception(
                "Unexpected error processing feed: %s", feed_cfg.get("name")
            )

    logger.info("Collection complete: %d new articles total", len(all_articles))
    return all_articles


# ------------------------------------------------------------------
# F1-2  CoinGecko price snapshot
# ------------------------------------------------------------------


def fetch_prices(config: dict[str, Any]) -> list[dict[str, Any]]:
    """Query CoinGecko for stablecoin prices and flag peg deviations.

    Parameters
    ----------
    config:
        Parsed config.yaml dict (must contain ``coingecko``).

    Returns
    -------
    list[dict]
        One entry per coin with keys
        ``{coin, price_usd, market_cap, peg_deviation, peg_warning}``.
    """
    cg_cfg = config.get("coingecko", {})
    coins: list[str] = cg_cfg.get("coins", [])
    threshold: float = cg_cfg.get("peg_threshold", 0.005)

    if not coins:
        logger.warning("No coins configured for CoinGecko")
        return []

    # JPY-pegged coins need JPY pricing
    jpy_pegged = {"jpyc"}
    needs_jpy = any(c in jpy_pegged for c in coins)

    ids_str = ",".join(coins)
    vs_currencies = "usd,jpy" if needs_jpy else "usd"
    params = {
        "ids": ids_str,
        "vs_currencies": vs_currencies,
        "include_market_cap": "true",
    }

    import time

    data = None
    for attempt in range(3):
        try:
            resp = requests.get(_COINGECKO_PRICE_URL, params=params, timeout=_REQUEST_TIMEOUT)
            if resp.status_code == 429:
                wait = 2 ** attempt
                logger.warning("CoinGecko rate limited, retrying in %ds...", wait)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            data = resp.json()
            break
        except requests.RequestException:
            logger.exception("CoinGecko API request failed (attempt %d)", attempt + 1)
            if attempt < 2:
                time.sleep(2 ** attempt)

    if data is None:
        logger.error("CoinGecko API: all attempts failed")
        return []

    results: list[dict[str, Any]] = []
    for coin in coins:
        info = data.get(coin)
        if info is None:
            logger.warning("No CoinGecko data returned for %s", coin)
            continue

        price_usd = info.get("usd", 0.0)
        market_cap = info.get("usd_market_cap", 0.0)

        if coin in jpy_pegged:
            # JPY-pegged: check deviation from ¥1
            price_jpy = info.get("jpy", 0.0)
            jpyc_cfg = config.get("jpyc", {})
            target = jpyc_cfg.get("peg_target_jpy", 1.0)
            jpyc_threshold = jpyc_cfg.get("peg_threshold_pct", 1.0) / 100.0
            deviation = abs(price_jpy - target) / target if target else 0.0
            warning = deviation > jpyc_threshold
            display_info = {
                "coin": coin,
                "price_usd": price_usd,
                "price_jpy": price_jpy,
                "market_cap": market_cap,
                "peg_target": f"¥{target}",
                "peg_deviation": round(deviation, 6),
                "peg_warning": warning,
            }
        else:
            # USD-pegged: check deviation from $1
            deviation = abs(price_usd - 1.0)
            warning = deviation > threshold
            display_info = {
                "coin": coin,
                "price_usd": price_usd,
                "market_cap": market_cap,
                "peg_deviation": round(deviation, 6),
                "peg_warning": warning,
            }

        results.append(display_info)

        if warning:
            logger.warning(
                "Peg deviation alert: %s (deviation %.4f%%)",
                coin,
                deviation * 100,
            )

    return results
