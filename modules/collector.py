"""F1: Data Collection -- RSS feeds and CoinGecko price snapshots."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import feedparser
import requests

logger = logging.getLogger(__name__)

_COINGECKO_PRICE_URL = "https://api.coingecko.com/api/v3/simple/price"
_REQUEST_TIMEOUT = 15  # seconds


# ------------------------------------------------------------------
# F1-1  RSS Feed Collection
# ------------------------------------------------------------------


def _parse_feed(feed_cfg: dict[str, str], storage: Any | None) -> list[dict[str, Any]]:
    """Parse a single RSS feed and return new (non-duplicate) article dicts."""
    name = feed_cfg["name"]
    url = feed_cfg["url"]
    articles: list[dict[str, Any]] = []

    try:
        feed = feedparser.parse(url)
    except Exception:
        logger.exception("Failed to fetch RSS feed: %s (%s)", name, url)
        return articles

    if feed.bozo and not feed.entries:
        logger.warning("RSS feed returned no entries: %s (%s)", name, url)
        return articles

    for entry in feed.entries:
        link = entry.get("link", "")
        if not link:
            continue
        if storage and storage.is_duplicate(link):
            continue

        # Best-effort published timestamp
        published_at: str | None = None
        if ts := entry.get("published_parsed") or entry.get("updated_parsed"):
            try:
                published_at = datetime(*ts[:6], tzinfo=timezone.utc).isoformat()
            except Exception:
                pass

        # Summary / beginning of the article body
        raw_text = entry.get("summary") or entry.get("description") or ""

        articles.append(
            {
                "title": entry.get("title", "(no title)"),
                "url": link,
                "source": name,
                "published_at": published_at,
                "raw_text": raw_text,
            }
        )

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

    try:
        resp = requests.get(_COINGECKO_PRICE_URL, params=params, timeout=_REQUEST_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException:
        logger.exception("CoinGecko API request failed")
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
