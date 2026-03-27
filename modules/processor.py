"""F2: AI Processing Module - Analyze stablecoin articles using Claude API."""

from __future__ import annotations

import json
import logging
import os

from anthropic import Anthropic
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """あなたはステーブルコインの専門アナリストです。以下の記事を分析し、指定されたJSON形式で回答してください。

重要: ステーブルコインとの関連性を重視して分析してください。

出力形式:
{
  "summary_ja": "日本語で3行の要約（各行は簡潔に。ステーブルコインとの関連を明確に）",
  "category": "規制・法令 | 技術 | 市場動向 | DeFi | 事件・リスク | ユースケース | 戦略・提携 | 導入事例 のいずれか1つ",
  "projects": ["言及されたステーブルコインプロジェクト名のリスト (例: USDT, USDC, DAI, JPYC等)"],
  "importance": 1から5の整数（1=低い重要度, 5=非常に重要。ステーブルコイン業界への影響度で判断）
}

カテゴリの判断基準:
- 規制・法令: 法律、規制案、コンプライアンス、ライセンス、政府方針
- 技術: プロトコル更新、スマートコントラクト、ブリッジ、チェーン対応
- 市場動向: 時価総額、取引量、価格変動、市場シェア、競争
- DeFi: レンディング、DEX、イールド、流動性プール、担保
- 事件・リスク: デペッグ、ハッキング、破綻、訴訟、準備金問題
- ユースケース: 決済、送金、国際取引、給与支払い、トークン化預金
- 戦略・提携: 企業提携、新規参入、事業拡大、パートナーシップ
- 導入事例: 銀行・企業の具体的な導入、プロダクトローンチ

JSONのみを返してください。説明は不要です。"""

VALID_CATEGORIES = {"規制・法令", "技術", "市場動向", "DeFi", "事件・リスク", "ユースケース", "戦略・提携", "導入事例"}


class Processor:
    """Processes articles through Claude API for analysis."""

    def __init__(self, config: dict):
        ai_config = config.get("ai", {})
        self.model = ai_config.get("model", "claude-haiku-4-5-20251001")
        self.max_tokens_per_article = ai_config.get("max_tokens_per_article", 1000)
        self.client = Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

    def process_article(self, article_dict: dict) -> dict | None:
        """Analyze a single article with Claude and return structured fields.

        Args:
            article_dict: Dict with at least 'title' and 'raw_text' keys.

        Returns:
            Dict with summary_ja, category, projects, importance or None on failure.
        """
        title = article_dict.get("title", "")
        raw_text = article_dict.get("raw_text", "")

        # Truncate raw_text to configured max length
        truncated_text = raw_text[: self.max_tokens_per_article]

        user_message = f"タイトル: {title}\n\n本文:\n{truncated_text}"

        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=1024,
                system=SYSTEM_PROMPT,
                messages=[{"role": "user", "content": user_message}],
            )

            content = response.content[0].text.strip()

            # Strip markdown code fences if present
            if content.startswith("```"):
                lines = content.split("\n")
                # Remove first line (```json or ```) and last line (```)
                lines = lines[1:]
                if lines and lines[-1].strip() == "```":
                    lines = lines[:-1]
                content = "\n".join(lines).strip()

            result = json.loads(content)

            # Validate expected keys
            required_keys = {"summary_ja", "category", "projects", "importance"}
            if not required_keys.issubset(result.keys()):
                missing = required_keys - result.keys()
                logger.error("API response missing keys: %s", missing)
                return None

            # Clamp importance to 1-5
            result["importance"] = max(1, min(5, int(result["importance"])))

            # Validate category
            if result["category"] not in VALID_CATEGORIES:
                logger.warning(
                    "Unexpected category '%s', keeping as-is", result["category"]
                )

            return result

        except json.JSONDecodeError as e:
            logger.error("Failed to parse JSON from API response: %s", e)
            return None
        except Exception as e:
            logger.error("API call failed for article '%s': %s", title, e)
            return None

    def process_articles(self, articles: list[dict]) -> list[dict]:
        """Batch-process articles, adding AI fields to each.

        Args:
            articles: List of article dicts with title/raw_text.

        Returns:
            Same list with summary_ja, category, projects, importance added.
            Articles that fail processing retain their original fields only.
        """
        results = []
        for article in articles:
            title = article.get("title", "(no title)")
            logger.info("Processing: %s", title)

            analysis = self.process_article(article)
            enriched = dict(article)
            if analysis:
                enriched.update(analysis)
            else:
                logger.warning("Skipping AI enrichment for: %s", title)

            results.append(enriched)

        logger.info(
            "Processed %d/%d articles successfully",
            sum(1 for a in results if "summary_ja" in a),
            len(articles),
        )
        return results
