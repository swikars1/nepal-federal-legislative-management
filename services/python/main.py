#!/usr/bin/env python3
"""
Nepal Federal Legislative Scraper - Main Controller

Controls scraping for bills + committees, then cleans and imports to DB.

Usage:
    python main.py                                  # run once now
    python main.py --schedule                       # run on fixed schedule
    python main.py --schedule --run-now             # run now, then schedule
    python main.py --schedule --timezone Asia/Kathmandu
"""

import argparse
import asyncio
import inspect
import json
import logging
import os
import re
import subprocess
import sys
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

import psycopg2
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from dotenv import load_dotenv


# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# Load environment variables
load_dotenv()


# Paths
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "data" / "output"
SCRAPER_DIR = BASE_DIR / "scraper"
REPO_ROOT = BASE_DIR.parent

# Create output directories
DATA_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# Schedule configuration
SCHEDULE_HOURS = "6,10,13,16,20"
SCHEDULE_MINUTE = 0
DEFAULT_TIMEZONE = os.getenv("SCRAPER_TIMEZONE", "Asia/Kathmandu")
OUTPUT_MAX_FILES = int(os.getenv("SCRAPER_OUTPUT_MAX_FILES", "3"))


# =====================================================================
# SCRAPER IMPORTS
# =====================================================================

def import_bills_scraper():
    """Import bills scraper module."""
    sys.path.insert(0, str(SCRAPER_DIR / "bills"))
    try:
        import scrape_bills

        return scrape_bills
    except ImportError as e:
        log.error(f"Failed to import bills scraper: {e}")
        return None


def import_bills_cleaner():
    """Import bills cleaner/normalizer module."""
    sys.path.insert(0, str(SCRAPER_DIR / "bills"))
    try:
        import clean_and_insert_bills

        return clean_and_insert_bills
    except ImportError:
        return None


def import_committees_scraper():
    """Import committees scraper module."""
    sys.path.insert(0, str(SCRAPER_DIR / "committees"))
    try:
        import scrape_committees

        return scrape_committees
    except ImportError as e:
        log.error(f"Failed to import committees scraper: {e}")
        return None


def import_committees_cleaner():
    """Import committees cleaner module."""
    sys.path.insert(0, str(SCRAPER_DIR / "committees"))
    try:
        import clean_and_insert

        return clean_and_insert
    except ImportError:
        return None


# =====================================================================
# HELPERS
# =====================================================================

def normalize_result(scraper_name: str, result: Any) -> Optional[Dict[str, Any]]:
    """Normalize raw list results into summary dict."""
    if result is None:
        return None

    if isinstance(result, dict):
        return result

    if isinstance(result, list):
        if scraper_name == "bills":
            return {
                "success": True,
                "total_bills": len(result),
                "hor_count": sum(1 for b in result if b.get("type") == "HoR"),
                "na_count": sum(1 for b in result if b.get("type") == "NA"),
            }
        if scraper_name == "committees":
            return {
                "success": True,
                "total_committees": len(result),
                "hor_count": sum(1 for c in result if c.get("house") == "HoR"),
                "na_count": sum(1 for c in result if c.get("house") == "NA"),
            }

    return {"success": False, "error": f"Unexpected result type for {scraper_name}"}


def get_database_url() -> Optional[str]:
    """Read database URL from env. Supports DATABASE_URL and DATABASEURL."""
    return os.getenv("DATABASE_URL") or os.getenv("DATABASEURL")


def parse_upserted_count(output: str) -> int:
    """
    Parse 'Upserted N ...' from Bun importer output.
    Example: '✓ Upserted 123 bills.'
    """
    matches = re.findall(r"upserted\s+(\d+)", output or "", flags=re.IGNORECASE)
    return int(matches[-1]) if matches else 0


def cleanup_output_directory(max_files: int = OUTPUT_MAX_FILES) -> Dict[str, Any]:
    """
    Keep only the newest `max_files` files in services/python/data/output.
    Older files are removed.
    """
    try:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

        if max_files < 1:
            return {
                "success": False,
                "error": "max_files must be >= 1",
                "max_files": max_files,
            }

        files = [p for p in OUTPUT_DIR.iterdir() if p.is_file()]
        before_count = len(files)

        if before_count <= max_files:
            return {
                "success": True,
                "max_files": max_files,
                "before_count": before_count,
                "after_count": before_count,
                "removed_files": [],
            }

        # oldest first
        files.sort(key=lambda p: (p.stat().st_mtime, p.name))
        remove_count = before_count - max_files
        to_remove = files[:remove_count]

        removed_files: List[str] = []
        failed_files: List[str] = []
        for path in to_remove:
            try:
                path.unlink()
                removed_files.append(path.name)
            except Exception:
                failed_files.append(path.name)

        after_count = before_count - len(removed_files)
        success = len(failed_files) == 0

        if removed_files:
            log.info(
                "Output cleanup: removed %d old file(s), kept latest %d",
                len(removed_files),
                max_files,
            )

        return {
            "success": success,
            "max_files": max_files,
            "before_count": before_count,
            "after_count": after_count,
            "removed_files": removed_files,
            "failed_files": failed_files,
            "error": None if success else "Some files could not be deleted",
        }
    except Exception as exc:
        return {"success": False, "error": str(exc), "max_files": max_files}


def run_bun_script(script_name: str) -> Dict[str, Any]:
    """Run bun script and return structured result."""
    try:
        completed = subprocess.run(
            ["bun", "run", script_name],
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        return {
            "success": False,
            "error": "bun executable not found",
            "script": script_name,
            "upserted": 0,
        }
    except Exception as exc:
        return {
            "success": False,
            "error": str(exc),
            "script": script_name,
            "upserted": 0,
        }

    stdout = (completed.stdout or "").strip()
    stderr = (completed.stderr or "").strip()
    upserted = parse_upserted_count(stdout)

    if completed.returncode != 0:
        return {
            "success": False,
            "error": stderr or stdout or f"{script_name} failed",
            "script": script_name,
            "upserted": 0,
            "stdout": stdout,
            "stderr": stderr,
        }

    return {
        "success": True,
        "script": script_name,
        "upserted": upserted,
        "stdout": stdout,
        "stderr": stderr,
    }


def run_db_imports() -> Dict[str, Any]:
    """Import cleaned JSON into DB via Bun scripts."""
    bills_import = run_bun_script("db:import-bills")
    committees_import = run_bun_script("db:import-committees")

    errors: List[str] = []
    if not bills_import.get("success"):
        errors.append(f"db:import-bills: {bills_import.get('error', 'Unknown error')}")
    if not committees_import.get("success"):
        errors.append(
            f"db:import-committees: {committees_import.get('error', 'Unknown error')}"
        )

    return {
        "success": not errors,
        "bills_upserted": bills_import.get("upserted", 0),
        "committees_upserted": committees_import.get("upserted", 0),
        "bills_import": bills_import,
        "committees_import": committees_import,
        "error": "; ".join(errors) if errors else None,
    }


def collect_errors(results: Dict[str, Any]) -> List[str]:
    """Collect step errors into a string list for scrape_logs.errors."""
    errors: List[str] = []
    for step_name, result in results.items():
        if isinstance(result, dict) and not result.get("success", True):
            error_msg = result.get("error", "Unknown error")
            errors.append(f"{step_name}: {error_msg}")
    return errors


def determine_overall_status(results: Dict[str, Any]) -> str:
    """Return one of success | partial | failed."""
    statuses: List[bool] = []
    for result in results.values():
        if isinstance(result, dict) and "success" in result:
            statuses.append(bool(result.get("success")))

    if not statuses:
        return "failed"
    if all(statuses):
        return "success"
    if any(statuses):
        return "partial"
    return "failed"


# =====================================================================
# SCRAPE LOGS (DB)
# =====================================================================

def create_scrape_log() -> Optional[str]:
    """
    Insert run-start row into scrape_logs and return log id.
    Uses DATABASE_URL / DATABASEURL from .env.
    """
    db_url = get_database_url()
    if not db_url:
        log.warning("DATABASE_URL not found; scrape_logs will not be written.")
        return None

    log_id = str(uuid.uuid4())
    try:
        with psycopg2.connect(db_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO scrape_logs (id, started_at, status)
                    VALUES (%s, NOW(), %s)
                    """,
                    (log_id, "partial"),
                )
        return log_id
    except Exception as exc:
        log.error("Failed to create scrape_log row: %s", exc, exc_info=True)
        return None


def update_scrape_log(log_id: Optional[str], results: Dict[str, Any]) -> None:
    """Update scrape_logs row on completion."""
    if not log_id:
        return

    db_url = get_database_url()
    if not db_url:
        return

    bills_result = normalize_result("bills", results.get("bills")) or {}
    db_import = results.get("db_import") if isinstance(results.get("db_import"), dict) else {}

    bills_found = int(bills_result.get("total_bills", 0) or 0)
    # We currently know upserted count, not exact new-vs-updated split.
    bills_updated = int(db_import.get("bills_upserted", 0) or 0)
    bills_new = 0

    errors = collect_errors(results)
    status = determine_overall_status(results)
    errors_json = json.dumps(errors, ensure_ascii=False) if errors else None

    try:
        with psycopg2.connect(db_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE scrape_logs
                    SET finished_at = NOW(),
                        bills_found = %s,
                        bills_updated = %s,
                        bills_new = %s,
                        errors = %s,
                        status = %s
                    WHERE id = %s
                    """,
                    (
                        bills_found,
                        bills_updated,
                        bills_new,
                        errors_json,
                        status,
                        log_id,
                    ),
                )
    except Exception as exc:
        log.error("Failed to update scrape_log row: %s", exc, exc_info=True)


# =====================================================================
# MAIN AUTOMATED WORKFLOW
# =====================================================================

async def run_all() -> Dict[str, Any]:
    """
    Run all tasks automatically:
    [1] scrape bills
    [2] scrape committees
    [3] clean both
    [4] import cleaned data to DB
    """
    log.info("=" * 60)
    log.info("AUTO-RUN MODE: scrape -> clean -> import -> log")
    log.info("=" * 60)

    log_id = create_scrape_log()
    results: Dict[str, Any] = {}

    try:
        # [1] Scrape bills
        log.info("\n[1/4] Scraping bills...")
        scrape_bills = import_bills_scraper()
        if scrape_bills:
            try:
                bills_result = await scrape_bills.scrape_all()
                results["bills"] = normalize_result("bills", bills_result)
            except Exception as exc:
                log.error("Bills scraping failed: %s", exc, exc_info=True)
                results["bills"] = {"success": False, "error": str(exc)}
        else:
            log.warning("Bills scraper module not available, skipping...")
            results["bills"] = None

        # [2] Scrape committees
        log.info("\n[2/4] Scraping committees...")
        scrape_committees = import_committees_scraper()
        if scrape_committees:
            try:
                committees_result = await scrape_committees.scrape_all_committees()
                results["committees"] = normalize_result(
                    "committees", committees_result
                )
            except Exception as exc:
                log.error("Committee scraping failed: %s", exc, exc_info=True)
                results["committees"] = {"success": False, "error": str(exc)}
        else:
            log.warning("Committees scraper module not available, skipping...")
            results["committees"] = None

        # [3] Clean both
        log.info("\n[3/4] Cleaning bills and committees...")
        bills_cleaner = import_bills_cleaner()
        if bills_cleaner:
            try:
                bills_clean_result = bills_cleaner.main()
                if inspect.isawaitable(bills_clean_result):
                    bills_clean_result = await bills_clean_result
                results["bills_clean"] = bills_clean_result
            except Exception as exc:
                log.error("Bills cleaner failed: %s", exc, exc_info=True)
                results["bills_clean"] = {"success": False, "error": str(exc)}
        else:
            log.warning("Bills cleaner module not available, skipping...")
            results["bills_clean"] = None

        committees_cleaner = import_committees_cleaner()
        if committees_cleaner:
            try:
                committees_clean_result = committees_cleaner.main()
                if inspect.isawaitable(committees_clean_result):
                    committees_clean_result = await committees_clean_result
                results["committees_clean"] = committees_clean_result
            except Exception as exc:
                log.error("Committees cleaner failed: %s", exc, exc_info=True)
                results["committees_clean"] = {"success": False, "error": str(exc)}
        else:
            log.warning("Committees cleaner module not available, skipping...")
            results["committees_clean"] = None

        # [4] Import cleaned JSON to DB
        log.info("\n[4/4] Importing cleaned data to database...")
        results["db_import"] = run_db_imports()

        return results

    except Exception as exc:
        log.error("Pipeline crashed: %s", exc, exc_info=True)
        results["pipeline"] = {"success": False, "error": str(exc)}
        return results
    finally:
        try:
            results["output_cleanup"] = cleanup_output_directory()
            print_report(results)
        finally:
            update_scrape_log(log_id, results)


# =====================================================================
# REPORTING
# =====================================================================

def save_report(results: Dict[str, Any]) -> None:
    """Save JSON report for each run."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    report_file = OUTPUT_DIR / f"run_report_{timestamp}.json"

    with open(report_file, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2, default=str)

    log.info("Saved report: %s", report_file)


def print_report(results: Dict[str, Any]) -> None:
    """Print a formatted report of run results."""
    log.info("\n" + "=" * 60)
    log.info("SCRAPING REPORT")
    log.info("=" * 60)

    for step_name, raw_result in results.items():
        result = normalize_result(step_name, raw_result)
        if result is None:
            continue

        log.info("\n%s:", step_name.upper())
        if result.get("success"):
            log.info("  Status: Success")
            if "duration_seconds" in result:
                log.info("  Duration: %.2fs", result["duration_seconds"])
            if "total_bills" in result:
                log.info("  Total bills: %s", result["total_bills"])
                if result.get("hor_count", 0) > 0:
                    log.info("    HoR: %s", result["hor_count"])
                if result.get("na_count", 0) > 0:
                    log.info("    NA:  %s", result["na_count"])
            if "total_committees" in result:
                log.info("  Total committees: %s", result["total_committees"])
                if result.get("hor_count", 0) > 0:
                    log.info("    HoR: %s", result["hor_count"])
                if result.get("na_count", 0) > 0:
                    log.info("    NA:  %s", result["na_count"])
            if "bills_upserted" in result or "committees_upserted" in result:
                log.info("  Bills upserted: %s", result.get("bills_upserted", 0))
                log.info(
                    "  Committees upserted: %s",
                    result.get("committees_upserted", 0),
                )
            if "removed_files" in result:
                log.info("  Removed files: %d", len(result.get("removed_files", [])))
                if result.get("failed_files"):
                    log.info(
                        "  Failed deletes: %d",
                        len(result.get("failed_files", [])),
                    )
            if "output" in result:
                log.info("  Output: %s", result["output"])
        else:
            log.info("  Status: Failed")
            log.info("  Error: %s", result.get("error", "Unknown error"))

    log.info("\n" + "=" * 60 + "\n")
    save_report(results)
    log.info("=" * 60 + "\n")


# =====================================================================
# SCHEDULER
# =====================================================================

def run_all_sync() -> None:
    """Sync wrapper for scheduler jobs."""
    asyncio.run(run_all())


def start_scheduler(timezone_name: str, run_now: bool = False) -> None:
    """Start APScheduler with fixed cron times."""
    try:
        timezone = ZoneInfo(timezone_name)
    except Exception:
        log.error("Invalid timezone: %s", timezone_name)
        raise

    if run_now:
        log.info("Running immediate job before scheduler start...")
        run_all_sync()

    scheduler = BlockingScheduler(timezone=timezone)
    trigger = CronTrigger(
        hour=SCHEDULE_HOURS,
        minute=SCHEDULE_MINUTE,
        timezone=timezone,
    )
    scheduler.add_job(
        run_all_sync,
        trigger=trigger,
        id="nepal_legislative_scraper",
        replace_existing=True,
        coalesce=True,
        max_instances=1,
        misfire_grace_time=3600,
    )

    log.info("=" * 60)
    log.info(
        "Scheduler started (%s): daily at %s:00",
        timezone_name,
        SCHEDULE_HOURS,
    )
    log.info("=" * 60)

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        log.info("Scheduler stopped.")


# =====================================================================
# MAIN ENTRY POINT
# =====================================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Nepal Federal Legislative Scraper Controller"
    )
    parser.add_argument(
        "--schedule",
        action="store_true",
        help="Run continuously on fixed cron times",
    )
    parser.add_argument(
        "--run-now",
        action="store_true",
        help="With --schedule: run one job immediately before waiting for cron",
    )
    parser.add_argument(
        "--timezone",
        default=DEFAULT_TIMEZONE,
        help=f"Scheduler timezone (default: {DEFAULT_TIMEZONE})",
    )

    args = parser.parse_args()

    if args.schedule:
        start_scheduler(timezone_name=args.timezone, run_now=args.run_now)
        return

    if args.run_now:
        log.info("--run-now has no effect without --schedule; running once now.")
    asyncio.run(run_all())


if __name__ == "__main__":
    main()
