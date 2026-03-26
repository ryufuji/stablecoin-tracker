#!/usr/bin/env python3
"""既存記事にAI処理を一括適用するスクリプト"""

import json
import logging
import sqlite3

import yaml
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def main():
    with open("config.yaml", "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    from modules.processor import Processor
    from modules.storage import Storage

    storage = Storage()
    processor = Processor(config)

    # 未処理記事を取得
    conn = sqlite3.connect(storage.db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM articles WHERE summary_ja IS NULL").fetchall()
    conn.close()

    articles = [dict(r) for r in rows]
    logger.info(f"{len(articles)} 件の未処理記事があります")

    processed = 0
    for i, article in enumerate(articles, 1):
        logger.info(f"[{i}/{len(articles)}] {article['title'][:50]}...")
        result = processor.process_article(article)
        if result:
            storage.update_ai_fields(
                article["url"],
                result["summary_ja"],
                result["category"],
                result["projects"],
                result["importance"],
            )
            processed += 1
        else:
            logger.warning(f"  スキップ: AI処理失敗")

    logger.info(f"完了: {processed}/{len(articles)} 件を処理しました")


if __name__ == "__main__":
    main()
