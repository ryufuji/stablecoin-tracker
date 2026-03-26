#!/usr/bin/env python3
"""ステーブルコイン情報収集・分析システム エントリーポイント"""

import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

import yaml
from dotenv import load_dotenv

load_dotenv()

# ログ設定
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)
log_file = LOG_DIR / f"{datetime.now().strftime('%Y-%m-%d')}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


def load_config(path: str = "config.yaml") -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def export_to_obsidian(storage, config) -> int:
    """importance >= threshold の記事をMarkdownファイルとしてエクスポート"""
    obsidian_cfg = config.get("obsidian", {})
    min_importance = obsidian_cfg.get("export_min_importance", 4)
    export_dir = obsidian_cfg.get("export_dir", "exports")
    os.makedirs(export_dir, exist_ok=True)

    articles = storage.get_articles(days=1, min_importance=min_importance)
    exported = 0

    for article in articles:
        title = article.get("title", "no_title")
        safe_title = "".join(c if c.isalnum() or c in " _-" else "_" for c in title)[:30]
        pub_date = (article.get("published_at") or article.get("created_at") or "")[:10]
        filename = f"{pub_date}_{safe_title}.md"
        filepath = os.path.join(export_dir, filename)

        if os.path.exists(filepath):
            continue

        import json as _json

        projects_raw = article.get("projects", "[]")
        if isinstance(projects_raw, str):
            try:
                projects_list = _json.loads(projects_raw)
            except Exception:
                projects_list = []
        else:
            projects_list = projects_raw or []

        content = f"""---
title: "{article.get('title', '')}"
source: "{article.get('source', '')}"
published: "{pub_date}"
category: {article.get('category', '')}
projects: {projects_list}
importance: {article.get('importance', '')}
url: "{article.get('url', '')}"
---

## 要約

{article.get('summary_ja', '（要約なし）')}

## リンク

[元記事を読む]({article.get('url', '')})
"""
        try:
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(content)
            exported += 1
        except Exception as e:
            logger.error(f"Obsidianエクスポートエラー: {filename}: {e}")

    return exported


def cmd_collect(args, config):
    """記事を収集し、AI処理を行う"""
    from modules.collector import collect_articles, fetch_prices
    from modules.storage import Storage

    if not args.dry_run:
        storage = Storage()
    else:
        storage = None

    # RSS記事収集
    logger.info("記事の収集を開始します...")
    articles = collect_articles(config, storage)
    logger.info(f"{len(articles)} 件の新規記事を取得しました")

    # CoinGecko価格取得
    prices = fetch_prices(config)
    if prices:
        coingecko_config = config.get("coingecko", {})
        threshold = coingecko_config.get("peg_threshold", 0.005)
        for p in prices:
            if abs(p["price_usd"] - 1.0) > threshold:
                logger.warning(f"ペッグ乖離検出: {p['coin']} = ${p['price_usd']:.4f}")

    # AI処理
    if not args.no_ai and articles:
        from modules.processor import Processor

        processor = Processor(config)
        logger.info("AI処理を開始します...")
        articles = processor.process_articles(articles)

    # DB保存
    if not args.dry_run:
        saved = 0
        for article in articles:
            try:
                storage.save_article(article)
                if article.get("summary_ja"):
                    storage.update_ai_fields(
                        article["url"],
                        article.get("summary_ja"),
                        article.get("category"),
                        article.get("projects"),
                        article.get("importance"),
                    )
                saved += 1
            except Exception as e:
                logger.error(f"保存エラー: {article.get('title', '?')}: {e}")
        logger.info(f"{saved} 件の記事をDBに保存しました")

        # Obsidianエクスポート
        exported = export_to_obsidian(storage, config)
        if exported:
            logger.info(f"{exported} 件の記事をObsidianにエクスポートしました")

        # Slack通知
        from modules.notifier import SlackNotifier

        notifier = SlackNotifier()
        if notifier.webhook_url:
            notifier.notify_new_articles(articles, prices)
            logger.info("Slack通知を送信しました")
    else:
        # dry-run: 取得内容を表示
        from rich.console import Console
        from rich.table import Table

        console = Console()
        table = Table(title="取得記事一覧（dry-run）")
        table.add_column("ソース", style="cyan")
        table.add_column("タイトル", style="white")
        table.add_column("公開日", style="green")

        for a in articles:
            table.add_row(a.get("source", ""), a.get("title", ""), a.get("published_at", ""))

        console.print(table)

        if prices:
            price_table = Table(title="ステーブルコイン価格")
            price_table.add_column("コイン", style="cyan")
            price_table.add_column("価格 (USD)", style="white")
            price_table.add_column("時価総額", style="green")
            price_table.add_column("ペッグ", style="red")

            threshold = config.get("coingecko", {}).get("peg_threshold", 0.005)
            for p in prices:
                deviation = abs(p["price_usd"] - 1.0)
                peg_status = "⚠ 乖離" if deviation > threshold else "OK"
                price_table.add_row(
                    p["coin"],
                    f"${p['price_usd']:.4f}",
                    f"${p.get('market_cap', 0):,.0f}",
                    peg_status,
                )
            console.print(price_table)


def cmd_report(args, config):
    """レポートを表示する"""
    from modules.reporter import generate_weekly_report, show_daily_report
    from modules.storage import Storage

    storage = Storage()

    if args.daily:
        show_daily_report(storage)
    elif args.weekly:
        generate_weekly_report(storage, config)
    else:
        logger.error("--daily または --weekly を指定してください")


def cmd_search(args, config):
    """記事を検索する"""
    from modules.reporter import search_articles
    from modules.storage import Storage

    storage = Storage()
    search_articles(
        storage,
        keyword=args.keyword,
        category=args.category,
        project=args.project,
        days=args.days,
    )


def main():
    parser = argparse.ArgumentParser(description="ステーブルコイン情報収集・分析システム")
    subparsers = parser.add_subparsers(dest="command", help="コマンド")

    # collect
    collect_parser = subparsers.add_parser("collect", help="記事を収集する")
    collect_parser.add_argument("--dry-run", action="store_true", help="DBへの保存なしで取得内容だけ確認")
    collect_parser.add_argument("--no-ai", action="store_true", help="AI処理をスキップして収集のみ")

    # report
    report_parser = subparsers.add_parser("report", help="レポートを表示する")
    report_parser.add_argument("--daily", action="store_true", help="日次ダッシュボード")
    report_parser.add_argument("--weekly", action="store_true", help="週次まとめ生成")

    # search
    search_parser = subparsers.add_parser("search", help="記事を検索する")
    search_parser.add_argument("keyword", help="検索キーワード")
    search_parser.add_argument("--category", help="カテゴリフィルタ")
    search_parser.add_argument("--project", help="プロジェクトフィルタ")
    search_parser.add_argument("--days", type=int, default=30, help="検索対象日数（デフォルト: 30）")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    config = load_config()

    if args.command == "collect":
        cmd_collect(args, config)
    elif args.command == "report":
        cmd_report(args, config)
    elif args.command == "search":
        cmd_search(args, config)


if __name__ == "__main__":
    main()
