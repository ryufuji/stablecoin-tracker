#!/usr/bin/env python3
"""Stablecoin Tracker - Flask Web UI"""

import json
import logging
import os
import sqlite3
import threading
import time

from flask import Flask, jsonify, render_template, request

from dotenv import load_dotenv

load_dotenv()

from modules.collector import collect_articles, fetch_prices
from modules.storage import Storage

app = Flask(__name__)
logger = logging.getLogger(__name__)

logging.basicConfig(level=logging.INFO)

CONFIG_PATH = "config.yaml"
_collect_lock = threading.Lock()
_scheduler_started = False
_COLLECT_INTERVAL = int(os.getenv("COLLECT_INTERVAL", "3600"))  # default 1 hour


def _load_config():
    import yaml

    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _get_storage():
    return Storage()


def _run_collection():
    """Collect articles once, saving new ones to DB."""
    with _collect_lock:
        try:
            storage = _get_storage()
            config = _load_config()
            articles = collect_articles(config, storage)
            saved = 0
            for article in articles:
                try:
                    storage.save_article(article)
                    saved += 1
                except Exception:
                    pass
            logger.info(f"Collection done: {saved} new articles saved (from {len(articles)} fetched)")
        except Exception as e:
            logger.error(f"Collection failed: {e}")


def _collection_loop():
    """Background loop: collect immediately, then every COLLECT_INTERVAL seconds."""
    logger.info("Starting auto-collection loop (interval=%ds)", _COLLECT_INTERVAL)
    _run_collection()
    while True:
        time.sleep(_COLLECT_INTERVAL)
        logger.info("Periodic collection triggered")
        _run_collection()


@app.before_request
def ensure_data():
    """Start the background collection scheduler on first request."""
    global _scheduler_started
    if not _scheduler_started:
        _scheduler_started = True
        t = threading.Thread(target=_collection_loop, daemon=True)
        t.start()


@app.route("/")
def dashboard():
    config = _load_config()
    storage = _get_storage()

    source_counts = storage.get_article_count_by_source(days=30)
    category_counts = storage.get_category_counts(days=30)
    important_articles = storage.get_articles(days=30, min_importance=5)[:5]
    recent_articles = storage.get_articles(days=30)[:10]

    # Parse projects JSON for display
    for a in important_articles + recent_articles:
        _parse_projects(a)

    # Fetch prices
    prices = fetch_prices(config)

    return render_template(
        "dashboard.html",
        prices=prices,
        source_counts=source_counts,
        category_counts=category_counts,
        important_articles=important_articles,
        recent_articles=recent_articles,
    )


@app.route("/articles")
def articles():
    storage = _get_storage()

    category = request.args.get("category", "")
    days = request.args.get("days", "7", type=str)
    min_importance = request.args.get("min_importance", "", type=str)

    kwargs = {}
    if days and days != "all":
        kwargs["days"] = int(days)
    else:
        kwargs["days"] = 3650  # ~10 years = "all"
    if category:
        kwargs["category"] = category
    if min_importance:
        kwargs["min_importance"] = int(min_importance)

    article_list = storage.get_articles(**kwargs)
    for a in article_list:
        _parse_projects(a)

    return render_template("articles.html", articles=article_list)


@app.route("/search")
def search():
    storage = _get_storage()

    keyword = request.args.get("keyword", "").strip()
    category = request.args.get("category", "")
    project = request.args.get("project", "").strip()
    days = request.args.get("days", "30", type=str)

    results = []
    if keyword:
        kwargs = {"keyword": keyword}
        if days and days != "all":
            kwargs["days"] = int(days)
        else:
            kwargs["days"] = 3650
        if category:
            kwargs["category"] = category
        if project:
            kwargs["project"] = project

        results = storage.get_articles(**kwargs)
        for a in results:
            _parse_projects(a)

    return render_template("search.html", results=results, keyword=keyword)


@app.route("/api/easy-explain-knowledge", methods=["POST"])
def easy_explain_knowledge():
    """ナレッジベースの内容を初心者向けに説明するAPI"""
    import anthropic

    data = request.get_json()
    title = data.get("title", "")
    content = data.get("content", "")[:2000]

    try:
        client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
        config = _load_config()
        model = config.get("ai", {}).get("model", "claude-haiku-4-5-20251001")

        prompt = (
            "以下のステーブルコインに関する解説文を、暗号資産や金融の知識がない初心者でも理解できるように、"
            "やさしい日本語で説明し直してください。\n\n"
            "ルール:\n"
            "- 専門用語は使わず、日常の言葉で説明する\n"
            "- 難しい概念は身近な例え（お買い物、銀行、ポイントカードなど）を使って説明する\n"
            "- 小学校高学年でも理解できるレベルを目指す\n"
            "- 箇条書きや短い段落で読みやすくする\n"
            "- 元の内容の重要なポイントは漏らさない\n\n"
            f"タイトル: {title}\n\n"
            f"内容:\n{content}"
        )

        message = client.messages.create(
            model=model,
            max_tokens=1024,
            messages=[{"role": "user", "content": prompt}],
        )
        return jsonify({"easy_content": message.content[0].text})

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/easy-explain/<int:article_id>")
def easy_explain(article_id):
    """記事の初心者向けかんたん説明を生成するAPI"""
    storage = _get_storage()
    try:
        conn = sqlite3.connect(storage.db_path)
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM articles WHERE id = ?", (article_id,)).fetchone()
        conn.close()
        if row is None:
            return jsonify({"error": "記事が見つかりません"}), 404

        article = dict(row)
        title = article.get("title", "")
        summary = article.get("summary_ja", "")

        import anthropic

        client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
        config = _load_config()
        model = config.get("ai", {}).get("model", "claude-haiku-4-5-20251001")

        prompt = (
            "以下のニュース記事の要約を、暗号資産や金融の知識がない初心者でも理解できるように、"
            "やさしい日本語で説明し直してください。\n\n"
            "ルール:\n"
            "- 専門用語は使わず、日常の言葉で説明する\n"
            "- 難しい概念は身近な例えを使って説明する\n"
            "- 「つまり何が起きたのか」「なぜ大事なのか」を明確にする\n"
            "- 3〜5文程度で簡潔にまとめる\n\n"
            f"記事タイトル: {title}\n"
            f"要約: {summary}"
        )

        message = client.messages.create(
            model=model,
            max_tokens=512,
            messages=[{"role": "user", "content": prompt}],
        )
        easy_summary = message.content[0].text
        return jsonify({"easy_summary": easy_summary})

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/article/<int:article_id>")
def article_detail(article_id):
    storage = _get_storage()
    try:
        conn = sqlite3.connect(storage.db_path)
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM articles WHERE id = ?", (article_id,)).fetchone()
        conn.close()
        if row is None:
            return "記事が見つかりません", 404
        article = dict(row)
        _parse_projects(article)
        return render_template("article_detail.html", article=article)
    except Exception:
        return "エラーが発生しました", 500


def _load_knowledge():
    """Load knowledge base YAML data."""
    import yaml

    kb_path = "data/knowledge.yaml"
    try:
        with open(kb_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        return {"sections": []}


def _flatten_chapters(sections):
    """Return ordered list of (section_id, chapter) tuples for prev/next nav."""
    flat = []
    for section in sections:
        for chapter in section.get("chapters", []):
            flat.append((section["id"], section, chapter))
    return flat


@app.route("/knowledge")
def knowledge():
    data = _load_knowledge()
    return render_template("knowledge.html", sections=data.get("sections", []))


@app.route("/knowledge/<section_id>/<chapter_id>")
def knowledge_detail(section_id, chapter_id):
    data = _load_knowledge()
    sections = data.get("sections", [])
    flat = _flatten_chapters(sections)

    # Find current chapter
    current_idx = None
    current_section = None
    current_chapter = None
    for i, (sid, sec, ch) in enumerate(flat):
        if sid == section_id and ch["id"] == chapter_id:
            current_idx = i
            current_section = sec
            current_chapter = ch
            break

    if current_chapter is None:
        return "ページが見つかりません", 404

    # Prev/next navigation
    prev_chapter = None
    prev_section_id = None
    next_chapter = None
    next_section_id = None
    if current_idx > 0:
        prev_section_id, _, prev_chapter = flat[current_idx - 1]
    if current_idx < len(flat) - 1:
        next_section_id, _, next_chapter = flat[current_idx + 1]

    return render_template(
        "knowledge_detail.html",
        section=current_section,
        chapter=current_chapter,
        prev_chapter=prev_chapter,
        prev_section_id=prev_section_id,
        next_chapter=next_chapter,
        next_section_id=next_section_id,
    )


def _parse_projects(article):
    """Parse projects JSON string into a list for template use."""
    projects = article.get("projects")
    if isinstance(projects, str):
        try:
            article["projects_list"] = json.loads(projects)
        except (json.JSONDecodeError, TypeError):
            article["projects_list"] = []
    elif isinstance(projects, list):
        article["projects_list"] = projects
    else:
        article["projects_list"] = []


@app.route("/twitter")
def twitter():
    """X/Twitter watchlist page."""
    config = _load_config()
    accounts = config.get("twitter_accounts", [])
    selected_category = request.args.get("category", "")

    # Get unique categories
    categories = sorted(set(a.get("category", "") for a in accounts if a.get("category")))

    # Filter by category if selected
    if selected_category:
        accounts = [a for a in accounts if a.get("category") == selected_category]

    # Attach any cached posts from the database
    storage = _get_storage()
    for account in accounts:
        handle = account.get("handle", "")
        try:
            conn = sqlite3.connect(storage.db_path)
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT title, url, summary_ja, published_at FROM articles "
                "WHERE source = ? ORDER BY published_at DESC LIMIT 3",
                (f"X/@{handle}",),
            ).fetchall()
            conn.close()
            account["recent_posts"] = [
                {
                    "text": dict(r)["title"],
                    "url": dict(r)["url"],
                    "summary_ja": dict(r).get("summary_ja", ""),
                    "date": (dict(r).get("published_at") or "")[:10],
                }
                for r in rows
            ]
        except Exception:
            account["recent_posts"] = []

    return render_template(
        "twitter.html",
        accounts=accounts,
        categories=categories,
        selected_category=selected_category,
    )


if __name__ == "__main__":
    debug = os.getenv("FLASK_DEBUG", "true").lower() == "true"
    port = int(os.getenv("PORT", 5001))
    app.run(debug=debug, host="0.0.0.0", port=port)
