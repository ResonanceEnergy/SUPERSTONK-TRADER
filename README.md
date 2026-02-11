# Superstonk DD Autopilot — One‑Drop (Windows + VS Code + SQLite)

This project builds an automated **Due Diligence (DD) library** from **r/Superstonk**.

## What it does
- Ingests DD‑flair submissions (📚 Due Diligence + 📚 Possible DD).
- Auto‑discovers *library/directory/compilation* hub posts.
- Crawls hub posts and extracts links from **BEST comments only** (limited top slice).
- Stores everything in **SQLite** with a resumable crawl queue.
- Logs errors + progress (rotating log file + heartbeat logs).
- Runs safely on a schedule with a **time budget** (default: 20 minutes).
- Generates weekly reports (+ optional week‑over‑week diff) from the SQLite DB.

> Safety: This is a research/archive pipeline. It does **not** execute trades.

---

## Quick Start (Windows)

### 1) Open in VS Code
- File → Open Folder → `superstonk_dd_autopilot_one_drop/`

### 2) Create a virtual environment
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 3) Create Reddit API credentials
Create a Reddit **script** app at:
- https://www.reddit.com/prefs/apps

Copy `client_id` and `client_secret`.

### 4) Configure `.env`
Copy `.env.example` to `.env` and fill in values.

```powershell
copy .env.example .env
notepad .env
```

### 5) Run ingestion once (20‑minute budget)
```powershell
python dd_library_autopilot.py --max-minutes 20
```

Outputs:
- SQLite DB: `superstonk_dd.sqlite`
- Logs: `logs/dd_autopilot.log`

---

## Scheduling (every 6 hours, 24/7, max 20 minutes)

### Install scheduled tasks (PowerShell)
```powershell
cd tools
.\install_tasks.ps1
```

This creates:
- **Superstonk DD Autopilot (6h-20m)** — runs every 6 hours, starting 1 minute from install time
- **Superstonk DD Hub Refresh (Monthly-20m)** — optional hub refresh on the 1st of each month

### Remove scheduled tasks
```powershell
cd tools
.\uninstall_tasks.ps1
```

---

## Weekly Report

### Generate weekly report (last 7 days)
```powershell
python dd_reporter.py --days 7
```

### Generate weekly report + diff (this week vs last week)
```powershell
python dd_reporter.py --days 7 --diff
```

Outputs written to `reports/`:
- `dd_report_<timestamp>.md`
- `dd_report_<timestamp>.json`

---

## Export project to ZIP (no secrets)

```powershell
python export_project_zip.py
```

Creates ZIP in `exports/` and always excludes:
- `.env` (secrets)
- `.venv/`

---

## VS Code Tasks
Open **Terminal → Run Task…**
- Install (pip)
- Run Autopilot (20 min)
- Run Hub Refresh (20 min)
- Generate Weekly Report + Diff (7d)
- Export Project ZIP
