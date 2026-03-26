"""F4: Report Output -- Daily dashboard, weekly summary, and search."""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

logger = logging.getLogger(__name__)
console = Console()

# ======================================================================
# F4-1  Daily Dashboard
# ======================================================================


def show_daily_report(
    storage: Any,
    prices: dict[str, dict[str, Any]] | None = None,
) -> None:
    """Print a coloured daily dashboard to the terminal.

    Parameters
    ----------
    storage:
        A ``Storage`` instance (see *modules/storage.py*).
    prices:
        Optional mapping ``{coin_symbol: {"price": float, "peg": float}}``
        used to flag peg deviations.  When *None* the peg section is
        skipped.
    """
    console.rule("[bold cyan]Daily Stablecoin Report[/bold cyan]")
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    console.print(f"[dim]Generated: {today}[/dim]\n")

    # ---- 1. Article count by source (previous day) -------------------
    source_counts = storage.get_article_count_by_source(days=1)
    if source_counts:
        tbl_src = Table(title="Articles by Source (past 24 h)", show_lines=True)
        tbl_src.add_column("Source", style="bold")
        tbl_src.add_column("Count", justify="right", style="green")
        for src, cnt in sorted(source_counts.items(), key=lambda x: -x[1]):
            tbl_src.add_row(src, str(cnt))
        total = sum(source_counts.values())
        tbl_src.add_row("[bold]TOTAL[/bold]", f"[bold]{total}[/bold]")
        console.print(tbl_src)
    else:
        console.print("[yellow]No articles collected in the past 24 hours.[/yellow]")
    console.print()

    # ---- 2. Category breakdown ---------------------------------------
    cat_counts = storage.get_category_counts(days=1)
    if cat_counts:
        tbl_cat = Table(title="Category Breakdown", show_lines=True)
        tbl_cat.add_column("Category", style="bold")
        tbl_cat.add_column("Count", justify="right", style="magenta")
        for cat, cnt in sorted(cat_counts.items(), key=lambda x: -x[1]):
            tbl_cat.add_row(cat, str(cnt))
        console.print(tbl_cat)
    else:
        console.print("[yellow]No categorised articles found.[/yellow]")
    console.print()

    # ---- 3. High-importance articles (importance == 5) ----------------
    important = storage.get_articles(days=1, min_importance=5)
    if important:
        console.print(
            Panel(
                f"[bold red]High-Importance Articles: {len(important)}[/bold red]",
                expand=False,
            )
        )
        for art in important:
            title = art.get("title", "(no title)")
            summary = art.get("summary_ja") or "(no summary)"
            console.print(f"  [bold]{title}[/bold]")
            console.print(f"    {summary}")
            console.print()
    else:
        console.print("[dim]No importance-5 articles today.[/dim]")
    console.print()

    # ---- 4. Peg deviation flags (optional) ---------------------------
    if prices:
        _PEG_THRESHOLD = 0.01  # 1 %
        tbl_peg = Table(title="Peg Deviation Check", show_lines=True)
        tbl_peg.add_column("Coin", style="bold")
        tbl_peg.add_column("Price", justify="right")
        tbl_peg.add_column("Peg", justify="right")
        tbl_peg.add_column("Deviation", justify="right")
        tbl_peg.add_column("Status")

        for coin, info in sorted(prices.items()):
            price = info.get("price", 0.0)
            peg = info.get("peg", 1.0)
            if peg == 0:
                continue
            dev = abs(price - peg) / peg
            dev_pct = f"{dev * 100:.3f}%"
            if dev >= _PEG_THRESHOLD:
                status = Text("DEPEGGED", style="bold red")
            else:
                status = Text("OK", style="green")
            tbl_peg.add_row(coin, f"{price:.4f}", f"{peg:.4f}", dev_pct, status)

        console.print(tbl_peg)
    console.print()
    console.rule("[bold cyan]End of Daily Report[/bold cyan]")


# ======================================================================
# F4-2  Weekly Summary
# ======================================================================


def generate_weekly_report(storage: Any, config: dict[str, Any]) -> str | None:
    """Collect the last 7 days of articles and ask Claude to produce a
    Japanese weekly trend summary.  The result is saved as a Markdown
    file under ``exports/weekly/``.

    Parameters
    ----------
    storage:
        A ``Storage`` instance.
    config:
        Project configuration dict.  Expected keys:

        * ``ai.model`` -- Anthropic model name (e.g. ``"claude-sonnet-4-20250514"``).

    Returns
    -------
    str | None
        The path to the saved Markdown file, or *None* on failure.
    """
    try:
        import anthropic
    except ImportError:
        logger.error("anthropic package is not installed -- cannot generate weekly report")
        return None

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        logger.error("ANTHROPIC_API_KEY not set in environment")
        return None

    articles = storage.get_articles(days=7)
    if not articles:
        logger.warning("No articles found for the past 7 days; skipping weekly report")
        return None

    # Build article digest for the prompt
    digest_lines: list[str] = []
    for art in articles:
        title = art.get("title", "")
        summary = art.get("summary_ja") or ""
        source = art.get("source", "")
        category = art.get("category") or ""
        importance = art.get("importance") or ""
        digest_lines.append(
            f"- [{source}] (importance:{importance}, category:{category}) {title}\n  {summary}"
        )
    digest = "\n".join(digest_lines)

    prompt = (
        "あなたはステーブルコイン市場の専門アナリストです。\n"
        "以下は過去7日間に収集されたステーブルコイン関連ニュース記事の一覧です。\n"
        "各記事のタイトル・要約・カテゴリ・重要度を踏まえ、\n"
        "今週のステーブルコイン市場のトレンドを日本語で要約してください。\n\n"
        "要約には以下を含めてください：\n"
        "1. 今週の主要トピック（3〜5件）\n"
        "2. 規制・法律に関する動き\n"
        "3. 技術・プロダクトの進展\n"
        "4. 市場動向とリスク要因\n"
        "5. 来週の注目ポイント\n\n"
        f"---\n記事一覧（{len(articles)}件）:\n{digest}\n---"
    )

    model = config.get("ai", {}).get("model", "claude-sonnet-4-20250514")

    try:
        client = anthropic.Anthropic(api_key=api_key)
        message = client.messages.create(
            model=model,
            max_tokens=4096,
            messages=[{"role": "user", "content": prompt}],
        )
        summary_text = message.content[0].text
    except Exception:
        logger.exception("Claude API call failed during weekly report generation")
        return None

    # Determine ISO week label and output path
    now = datetime.now(timezone.utc)
    iso_year, iso_week, _ = now.isocalendar()
    filename = f"{iso_year}-W{iso_week:02d}.md"
    out_dir = os.path.join("exports", "weekly")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, filename)

    header = (
        f"# Weekly Stablecoin Report {iso_year}-W{iso_week:02d}\n\n"
        f"Generated: {now.strftime('%Y-%m-%d %H:%M:%S UTC')}\n"
        f"Articles analysed: {len(articles)}\n\n---\n\n"
    )

    try:
        with open(out_path, "w", encoding="utf-8") as fh:
            fh.write(header)
            fh.write(summary_text)
            fh.write("\n")
        logger.info("Weekly report saved to %s", out_path)
        console.print(f"[green]Weekly report saved:[/green] {out_path}")
        return out_path
    except OSError:
        logger.exception("Failed to write weekly report to %s", out_path)
        return None


# ======================================================================
# F4-3  Search
# ======================================================================


def search_articles(
    storage: Any,
    keyword: str,
    category: str | None = None,
    project: str | None = None,
    days: int = 30,
) -> list[dict[str, Any]]:
    """Search stored articles and display results with rich formatting.

    Parameters
    ----------
    storage:
        A ``Storage`` instance.
    keyword:
        Free-text search term matched against title and raw_text.
    category:
        Optional category filter.
    project:
        Optional project name filter (substring match on the JSON
        ``projects`` column).
    days:
        Look-back window in days (default 30).

    Returns
    -------
    list[dict]
        The matching article dicts (also printed to terminal).
    """
    results = storage.get_articles(
        days=days,
        category=category,
        project=project,
        keyword=keyword,
    )

    console.rule(f"[bold cyan]Search Results for '{keyword}'[/bold cyan]")

    if not results:
        console.print("[yellow]No articles matched the query.[/yellow]")
        return results

    console.print(f"[dim]Found {len(results)} article(s)[/dim]\n")

    tbl = Table(show_lines=True, expand=True)
    tbl.add_column("#", justify="right", style="dim", width=4)
    tbl.add_column("Date", style="cyan", width=12)
    tbl.add_column("Source", style="green", width=14)
    tbl.add_column("Imp", justify="center", width=4)
    tbl.add_column("Title", style="bold", ratio=2)
    tbl.add_column("Summary", ratio=3)

    for idx, art in enumerate(results, 1):
        pub = art.get("published_at") or art.get("created_at") or ""
        if pub and len(pub) >= 10:
            pub = pub[:10]
        importance = art.get("importance")
        imp_str = str(importance) if importance is not None else "-"
        if importance is not None and importance >= 4:
            imp_str = f"[red]{imp_str}[/red]"

        summary = art.get("summary_ja") or ""
        if len(summary) > 120:
            summary = summary[:117] + "..."

        tbl.add_row(
            str(idx),
            pub,
            art.get("source", ""),
            imp_str,
            art.get("title", ""),
            summary,
        )

    console.print(tbl)
    return results
