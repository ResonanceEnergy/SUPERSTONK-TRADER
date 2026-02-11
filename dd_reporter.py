import os
import json
import sqlite3
import argparse
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse

DB_DEFAULT = "superstonk_dd.sqlite"


def utc_now():
    return datetime.now(timezone.utc)


def iso(dt: datetime):
    return dt.astimezone(timezone.utc).isoformat()


def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def connect_db(path: str):
    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row
    return con


def domain_of(url: str) -> str:
    try:
        p = urlparse(url)
        host = (p.netloc or "").lower().replace("www.", "")
        return host if host else "(unknown)"
    except Exception:
        return "(unknown)"


def fetch_all(con, sql, params=()):
    cur = con.execute(sql, params)
    return [dict(r) for r in cur.fetchall()]


def fetch_one(con, sql, params=()):
    cur = con.execute(sql, params)
    row = cur.fetchone()
    return dict(row) if row else {}


def count_domains(rows):
    counts = {}
    for r in rows:
        d = domain_of(r["url"])
        counts[d] = counts.get(d, 0) + 1
    items = [{"domain": k, "count": v} for k, v in counts.items()]
    items.sort(key=lambda x: x["count"], reverse=True)
    return items


def compute_deltas(a, b):
    keys = set(a.keys()) | set(b.keys())
    out = []
    for k in keys:
        out.append({"key": k, "this": int(a.get(k, 0)), "last": int(b.get(k, 0)), "delta": int(a.get(k, 0)) - int(b.get(k, 0))})
    out.sort(key=lambda x: (abs(x["delta"]), x["this"]), reverse=True)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=DB_DEFAULT)
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--out", default="reports")
    ap.add_argument("--top-posts", type=int, default=25)
    ap.add_argument("--top-domains", type=int, default=30)
    ap.add_argument("--diff", action="store_true")
    ap.add_argument("--week-len", type=int, default=7)
    args = ap.parse_args()

    ensure_dir(args.out)
    con = connect_db(args.db)

    now = utc_now()
    start = now - timedelta(days=args.days)
    start_iso = iso(start)
    now_iso = iso(now)

    totals = fetch_one(con, "SELECT COUNT(*) AS posts_total FROM posts")
    totals_links = fetch_one(con, "SELECT COUNT(*) AS links_total FROM links")

    posts_window = fetch_one(con, "SELECT COUNT(*) AS posts_window FROM posts WHERE created_iso >= ?", (start_iso,))

    flairs = fetch_all(con, """
        SELECT COALESCE(link_flair_text,'(none)') AS flair, COUNT(*) AS c
        FROM posts
        WHERE created_iso >= ?
        GROUP BY COALESCE(link_flair_text,'(none)')
        ORDER BY c DESC
    """, (start_iso,))

    top_posts = fetch_all(con, """
        SELECT id, created_iso, title, score, num_comments, permalink, link_flair_text
        FROM posts
        WHERE created_iso >= ? AND created_iso < ?
        ORDER BY score DESC
        LIMIT ?
    """, (start_iso, now_iso, args.top_posts))

    domain_rows_window = fetch_all(con, """
        SELECT l.url AS url
        FROM links l
        JOIN posts p ON p.id = l.post_id
        WHERE p.created_iso >= ? AND p.created_iso < ?
    """, (start_iso, now_iso))

    top_domains_window = count_domains(domain_rows_window)[: args.top_domains]

    diff = None
    if args.diff:
        wl = max(1, args.week_len)
        this_start = now - timedelta(days=wl)
        last_start = now - timedelta(days=2*wl)
        last_end = now - timedelta(days=wl)

        this_start_iso = iso(this_start)
        last_start_iso = iso(last_start)
        last_end_iso = iso(last_end)

        this_flairs = fetch_all(con, """
            SELECT COALESCE(link_flair_text,'(none)') AS flair, COUNT(*) AS c
            FROM posts
            WHERE created_iso >= ? AND created_iso < ?
            GROUP BY COALESCE(link_flair_text,'(none)')
        """, (this_start_iso, now_iso))
        last_flairs = fetch_all(con, """
            SELECT COALESCE(link_flair_text,'(none)') AS flair, COUNT(*) AS c
            FROM posts
            WHERE created_iso >= ? AND created_iso < ?
            GROUP BY COALESCE(link_flair_text,'(none)')
        """, (last_start_iso, last_end_iso))

        this_map = {r['flair']: r['c'] for r in this_flairs}
        last_map = {r['flair']: r['c'] for r in last_flairs}
        flair_deltas = compute_deltas(this_map, last_map)

        this_dom_rows = fetch_all(con, """
            SELECT l.url AS url
            FROM links l
            JOIN posts p ON p.id = l.post_id
            WHERE p.created_iso >= ? AND p.created_iso < ?
        """, (this_start_iso, now_iso))

        last_dom_rows = fetch_all(con, """
            SELECT l.url AS url
            FROM links l
            JOIN posts p ON p.id = l.post_id
            WHERE p.created_iso >= ? AND p.created_iso < ?
        """, (last_start_iso, last_end_iso))

        this_dom = {d['domain']: d['count'] for d in count_domains(this_dom_rows)}
        last_dom = {d['domain']: d['count'] for d in count_domains(last_dom_rows)}
        dom_deltas = compute_deltas(this_dom, last_dom)[: max(50, args.top_domains)]

        diff = {
            'week_len_days': wl,
            'this_week': {'start': this_start_iso, 'end': now_iso},
            'last_week': {'start': last_start_iso, 'end': last_end_iso},
            'flair_deltas': flair_deltas,
            'domain_deltas': dom_deltas,
        }

    stamp = now.strftime('%Y%m%d_%H%M%S')
    suffix = '_diff' if args.diff else ''
    md_path = os.path.join(args.out, f'dd_report_{stamp}{suffix}.md')
    json_path = os.path.join(args.out, f'dd_report_{stamp}{suffix}.json')

    report = {
        'generated_at_utc': iso(now),
        'window_days': args.days,
        'window_start_utc': start_iso,
        'totals': {**totals, **totals_links, **posts_window},
        'flair_breakdown_window': flairs,
        'top_posts_by_score_window': top_posts,
        'top_domains_window': top_domains_window,
        'diff': diff,
    }

    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    # basic markdown
    flair_lines = '\n'.join([f"- `{r['flair']}` — **{r['c']}**" for r in flairs]) if flairs else '_(none)_' 
    domain_lines = '\n'.join([f"- `{d['domain']}` — **{d['count']}**" for d in top_domains_window]) if top_domains_window else '_(none)_' 

    md = f"""# Superstonk DD Library Report

Generated (UTC): `{iso(now)}`  
Window: last **{args.days}** days (since `{start_iso}`)

## Totals
- Posts (all time): **{report['totals'].get('posts_total', 0)}**
- Links (all time): **{report['totals'].get('links_total', 0)}**
- Posts in window: **{report['totals'].get('posts_window', 0)}**

## Flair breakdown
{flair_lines}

## Top cited domains (window)
{domain_lines}
"""

    if diff:
        md += "\n## Week-over-week Diff\n"
        md += f"This week: `{diff['this_week']['start']} → {diff['this_week']['end']}`\n"
        md += f"Last week: `{diff['last_week']['start']} → {diff['last_week']['end']}`\n\n"
        md += "### Flair deltas (this - last)\n"
        md += '\n'.join([f"- `{x['key']}` Δ **{x['delta']}** (this {x['this']}, last {x['last']})" for x in diff['flair_deltas'][:20]])
        md += "\n\n### Domain deltas (this - last)\n"
        md += '\n'.join([f"- `{x['key']}` Δ **{x['delta']}**" for x in diff['domain_deltas'][:30]])

    md += f"\n\nOutputs:\n- {md_path}\n- {json_path}\n"

    with open(md_path, 'w', encoding='utf-8') as f:
        f.write(md)

    print('Report written:')
    print(' -', md_path)
    print(' -', json_path)

    con.close()


if __name__ == '__main__':
    main()
