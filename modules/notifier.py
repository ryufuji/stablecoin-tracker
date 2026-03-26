"""Slack notification module for stablecoin tracker."""

from __future__ import annotations

import json
import logging
import os

import requests

logger = logging.getLogger(__name__)


class SlackNotifier:
    """Send notifications to Slack via Incoming Webhook."""

    def __init__(self, webhook_url: str | None = None):
        self.webhook_url = webhook_url or os.getenv("SLACK_WEBHOOK_URL")
        if not self.webhook_url:
            logger.warning("SLACK_WEBHOOK_URL not set. Slack notifications disabled.")

    def _send(self, payload: dict) -> bool:
        if not self.webhook_url:
            return False
        try:
            resp = requests.post(
                self.webhook_url,
                json=payload,
                timeout=10,
            )
            resp.raise_for_status()
            return True
        except requests.RequestException:
            logger.exception("Slack notification failed")
            return False

    def notify_new_articles(self, articles: list[dict], prices: list[dict] | None = None):
        """Send a summary of newly collected articles to Slack."""
        if not articles:
            return

        # Count by source
        source_counts = {}
        for a in articles:
            src = a.get("source", "Unknown")
            source_counts[src] = source_counts.get(src, 0) + 1

        source_lines = "\n".join(f"  • {src}: {cnt}件" for src, cnt in source_counts.items())

        # High importance articles
        important = [a for a in articles if (a.get("importance") or 0) >= 4]
        important_section = ""
        if important:
            lines = []
            for a in important[:5]:
                imp = a.get("importance", "?")
                title = a.get("title", "")
                summary = a.get("summary_ja", "")
                url = a.get("url", "")
                lines.append(f"  :star: *[{imp}]* <{url}|{title}>\n    {summary}")
            important_section = "\n\n:rotating_light: *重要記事:*\n" + "\n".join(lines)

        # Price section
        price_section = ""
        if prices:
            peg_alerts = [p for p in prices if p.get("peg_warning")]
            if peg_alerts:
                plines = [
                    f"  :warning: *{p['coin']}*: ${p['price_usd']:.4f} (乖離 {p['peg_deviation']:.4f}%)"
                    for p in peg_alerts
                ]
                price_section = "\n\n:chart_with_downwards_trend: *ペッグ乖離アラート:*\n" + "\n".join(plines)

        text = (
            f":newspaper: *Stablecoin Tracker 更新*\n\n"
            f"新規記事: *{len(articles)}件*\n{source_lines}"
            f"{important_section}"
            f"{price_section}"
        )

        self._send({"text": text})

    def notify_peg_alert(self, coin: str, price: float, deviation: float):
        """Send an urgent peg deviation alert."""
        text = (
            f":rotating_light: *ペッグ乖離アラート*\n\n"
            f"コイン: *{coin}*\n"
            f"価格: ${price:.4f}\n"
            f"乖離: {deviation:.4f}%\n"
            f"閾値を超えています。確認してください。"
        )
        self._send({"text": text})

    def notify_weekly_report(self, report_path: str, article_count: int):
        """Notify that a weekly report has been generated."""
        text = (
            f":bar_chart: *週次レポート生成完了*\n\n"
            f"分析記事数: {article_count}件\n"
            f"ファイル: `{report_path}`\n"
            f"ダッシュボードで確認してください。"
        )
        self._send({"text": text})
