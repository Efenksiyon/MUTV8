"""
Module 1: The Data Hoarder
Polls the Polymarket Gamma API every 5 minutes and stores market snapshots
in a Supabase `market_history` table.
"""

import json
import logging
import os
import time
from datetime import datetime, timezone

import requests
import schedule
from dotenv import load_dotenv
from supabase import create_client, Client

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
load_dotenv()

SUPABASE_URL: str = os.environ["SUPABASE_URL"]
SUPABASE_KEY: str = os.environ["SUPABASE_KEY"]

GAMMA_EVENTS_URL = "https://gamma-api.polymarket.com/events"
TARGET_CATEGORIES = {"sports", "politics"}   # lower-cased for comparison
TOP_N_ACTIVE = 50                            # fallback if no category match
FETCH_LIMIT = 200                            # max events pulled per request
TABLE_NAME = "market_history"

# ---------------------------------------------------------------------------
# Supabase client (initialised once at import time)
# ---------------------------------------------------------------------------
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _event_tags(event: dict) -> set[str]:
    """Return the set of lowercase tag slugs for an event."""
    return {t.get("slug", "").lower() for t in (event.get("tags") or [])}


def _parse_market(event: dict) -> list[dict]:
    """
    outcomes and outcomePrices arrive as JSON strings, e.g. '["Yes","No"]'.
    We only keep binary Yes/No markets and map index 0 → Yes, index 1 → No.
    """
    rows: list[dict] = []
    now = datetime.now(timezone.utc).isoformat()
    # Use first matching tag label as category (fallback to empty string)
    tag_labels = [t.get("label", "") for t in (event.get("tags") or [])]
    category = tag_labels[0] if tag_labels else ""
    event_id = str(event.get("id", ""))

    markets: list[dict] = event.get("markets", [])
    for market in markets:
        # outcomes is a JSON string: '["Yes", "No"]'
        try:
            outcomes = json.loads(market.get("outcomes") or "[]")
            prices   = json.loads(market.get("outcomePrices") or "[]")
        except (json.JSONDecodeError, TypeError):
            continue

        titles = [o.strip().lower() for o in outcomes]
        if "yes" not in titles or "no" not in titles:
            continue  # skip non-binary markets

        try:
            yes_idx  = titles.index("yes")
            no_idx   = titles.index("no")
            yes_price = float(prices[yes_idx]) if yes_idx < len(prices) else None
            no_price  = float(prices[no_idx])  if no_idx  < len(prices) else None
        except (ValueError, IndexError, TypeError):
            yes_price = no_price = None

        volume = None
        try:
            volume = float(market.get("volumeNum") or market.get("volume") or 0)
        except (TypeError, ValueError):
            pass

        rows.append(
            {
                "timestamp": now,
                "market_id": str(market.get("id", event_id)),
                "market_question": (
                    market.get("question") or event.get("title") or ""
                ).strip(),
                "category": category,
                "yes_price": yes_price,
                "no_price": no_price,
                "volume": volume,
            }
        )
    return rows


# ---------------------------------------------------------------------------
# Core job
# ---------------------------------------------------------------------------

def fetch_and_store() -> None:
    """Fetch active markets from Polymarket and upsert a snapshot to Supabase."""
    log.info("--- Fetch cycle starting ---")

    # 1. Pull events from Gamma API
    try:
        params = {
            "active": "true",
            "closed": "false",
            "limit": FETCH_LIMIT,
        }
        resp = requests.get(GAMMA_EVENTS_URL, params=params, timeout=20)
        resp.raise_for_status()
        events: list[dict] = resp.json()
    except requests.RequestException as exc:
        log.error("Failed to fetch from Gamma API: %s", exc)
        return

    log.info("Fetched %d events from Gamma API.", len(events))

    # 2. Filter: prefer Sports/Politics (matched via tag slugs); fall back to top-N
    filtered = [
        e for e in events
        if _event_tags(e) & TARGET_CATEGORIES
    ]

    if filtered:
        log.info(
            "Filtered to %d events in target categories: %s",
            len(filtered),
            TARGET_CATEGORIES,
        )
        working_set = filtered
    else:
        log.warning(
            "No events found in %s — falling back to top-%d by volume.",
            TARGET_CATEGORIES,
            TOP_N_ACTIVE,
        )
        working_set = events[:TOP_N_ACTIVE]

    # 3. Parse into row dicts
    rows: list[dict] = []
    for event in working_set:
        rows.extend(_parse_market(event))

    if not rows:
        log.warning("No binary Yes/No markets found in this fetch cycle.")
        return

    log.info("Parsed %d market rows ready for insertion.", len(rows))

    # 4. Insert into Supabase
    try:
        result = supabase.table(TABLE_NAME).insert(rows).execute()
        inserted = len(result.data) if result.data else 0
        log.info("Successfully inserted %d rows into '%s'.", inserted, TABLE_NAME)
    except Exception as exc:
        log.error("Supabase insertion failed: %s", exc)


# ---------------------------------------------------------------------------
# Scheduler — runs every 5 minutes
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    log.info("Data Hoarder starting up. First fetch in 0 s, then every 5 minutes.")

    fetch_and_store()                              # run immediately on start-up
    schedule.every(5).minutes.do(fetch_and_store)

    while True:
        try:
            schedule.run_pending()
        except Exception as exc:
            log.error("Unexpected error in scheduler loop: %s", exc, exc_info=True)
        time.sleep(10)
