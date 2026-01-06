"""
Synthetic app event generator for product analytics events.

Writes JSONL files to:
  data/raw/events/event_date=YYYY-MM-DD/events.jsonl

We are seperating per day which is why we split by event_date

Each line is one JSON event matching the event schema contract (v1).
"""

from __future__ import annotations

import json
import random
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List


# ----------------------------
# Config
# ----------------------------

SCHEMA_VERSION = 1
ALLOWED_PLATFORMS = ["web", "ios", "android"]
FEATURES = ["search", "profile", "settings", "checkout", "notifications", "recommendations", "notes"]
ACTIONS = ["view", "click", "create", "update", "delete", "refresh"]
FEATURE_ACTIONS = { #so they are mapped correctly 
    "search": ["view", "click"],
    "profile": ["view", "update"],
    "settings": ["view", "update"],
    "checkout": ["view", "click", "refresh"],
    "notifications": ["view", "refresh", "click", "delete"],
    "recommendations": ["view", "click"],
    "notes": ["create", "update", "delete", "view"],
}

RAW_BASE = Path("data/raw/events")


@dataclass(frozen=True)
class GeneratorConfig:
    start_date: str = "2026-01-01"   # YYYY-MM-DD
    num_days: int = 7
    num_users: int = 300

    # Behavior knobs
    signup_rate: float = 0.12        # fraction of users who sign up on day 1
    daily_active_rate: float = 0.35  # chance a user is active on a given day
    sessions_per_active_user: tuple[int, int] = (1, 3)   # min, max
    feature_events_per_session: tuple[int, int] = (1, 8) # min, max
    purchase_rate_per_session: float = 0.06              # chance purchase happens in a session
    #intentional messy data to simulate real world data quality scenarios: 
    bad_row_rate: float = 0.01        # 1% intentionally messy rows
    duplicate_rate: float = 0.01      # 1% duplicated events (simulates retries)


# ----------------------------
# Helpers
# ----------------------------

def iso_utc(dt: datetime) -> str:
    """Return ISO 8601 UTC string with Z suffix."""
    dt_utc = dt.astimezone(timezone.utc)
    return dt_utc.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_date_utc(date_str: str) -> datetime:
    """Parse YYYY-MM-DD as UTC midnight datetime."""
    y, m, d = (int(x) for x in date_str.split("-"))
    return datetime(y, m, d, tzinfo=timezone.utc)


def random_time_within_day(day_start: datetime) -> datetime:
    """Return a random time within the UTC day."""
    seconds = random.randint(0, 24 * 60 * 60 - 1)
    return day_start + timedelta(seconds=seconds)


def new_event_id() -> str:
    return str(uuid.uuid4())


def make_session_id(user_id: int, session_index: int, day_start: datetime) -> str:
    # Stable-ish + readable
    day = day_start.date().isoformat()
    return f"sess-{user_id}-{day}-{session_index}"


def write_jsonl(path: Path, events: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f: #overrites with every run, not appends 
        for e in events:
            f.write(json.dumps(e, separators=(",", ":"), ensure_ascii=False) + "\n")


def maybe_corrupt_event(e: Dict[str, Any], bad_row_rate: float) -> Dict[str, Any]:
    if random.random() >= bad_row_rate:
        return e

    choice = random.choice(
        ["missing_user", "bad_time", "bad_platform", "case_event", "blank_event_id"]
    )

    if choice == "missing_user":
        e["user_id"] = None
    elif choice == "bad_time":
        e["event_time"] = "2026-13-99T99:99:99Z"
    elif choice == "bad_platform":
        e["platform"] = " WEB "
    elif choice == "case_event":
        e["event_name"] = str(e["event_name"]).upper()
    elif choice == "blank_event_id":
        e["event_id"] = ""

    return e



# ----------------------------
# Event builders (core pattern)
# ----------------------------

def build_event(
    *,
    event_name: str,
    event_time: datetime,
    user_id: int,
    session_id: str,
    platform: str,
    properties: Dict[str, Any],
) -> Dict[str, Any]:
    return {
        "event_id": new_event_id(),
        "event_name": event_name,
        "event_time": iso_utc(event_time),
        "user_id": user_id,
        "session_id": session_id,
        "platform": platform,
        "schema_version": SCHEMA_VERSION,
        "properties": properties,
    }


def signup_event(event_time: datetime, user_id: int, session_id: str, platform: str) -> Dict[str, Any]:
    return build_event(
        event_name="signup",
        event_time=event_time,
        user_id=user_id,
        session_id=session_id,
        platform=platform,
        properties={"method": random.choice(["email", "google", "apple"])},
    )


def login_event(event_time: datetime, user_id: int, session_id: str, platform: str) -> Dict[str, Any]:
    return build_event(
        event_name="login",
        event_time=event_time,
        user_id=user_id,
        session_id=session_id,
        platform=platform,
        properties={"success": True},
    )


def feature_use_event(event_time: datetime, user_id: int, session_id: str, platform: str) -> Dict[str, Any]:
    feature = random.choice(FEATURES)
    actions = FEATURE_ACTIONS.get(feature, ACTIONS)
    action = random.choice(actions)
    props: Dict[str, Any] = {
        "feature_name": feature,
        "action": action,
        "duration_ms": random.randint(200, 12000),
    }

    # Optional realism: some features have objects
    if feature in {"notes", "profile"}:
        props["object_type"] = "note" if feature == "notes" else "profile_item"
        props["object_id"] = f"{props['object_type']}_{random.randint(1000, 9999)}"

    return build_event(
        event_name="feature_use",
        event_time=event_time,
        user_id=user_id,
        session_id=session_id,
        platform=platform,
        properties=props,
    )


def purchase_event(event_time: datetime, user_id: int, session_id: str, platform: str) -> Dict[str, Any]:
    amount = random.choice([9.99, 19.99, 29.99, 49.99]) #random prices 
    return build_event(
        event_name="purchase",
        event_time=event_time,
        user_id=user_id,
        session_id=session_id,
        platform=platform,
        properties={
            "amount": amount,
            "currency": "USD",
            "plan": random.choice(["basic", "pro", "team"]),
        },
    )


def logout_event(event_time: datetime, user_id: int, session_id: str, platform: str) -> Dict[str, Any]:
    return build_event(
        event_name="logout",
        event_time=event_time,
        user_id=user_id,
        session_id=session_id,
        platform=platform,
        properties={},
    )


# ----------------------------
# Simulation logic
# ----------------------------

def simulate_session(day_start: datetime, user_id: int, session_id: str, platform: str, cfg: GeneratorConfig) -> List[Dict[str, Any]]:
    """Generate a sequence of events for one session."""
    events: List[Dict[str, Any]] = []

    # choose a session start time within the day
    t = random_time_within_day(day_start)

    # login
    events.append(login_event(t, user_id, session_id, platform))
    t += timedelta(seconds=random.randint(5, 60))

    # feature uses
    n_feature = random.randint(*cfg.feature_events_per_session)
    for _ in range(n_feature):
        events.append(feature_use_event(t, user_id, session_id, platform))
        t += timedelta(seconds=random.randint(5, 180))

    # optional purchase
    if random.random() < cfg.purchase_rate_per_session:
        events.append(purchase_event(t, user_id, session_id, platform))
        t += timedelta(seconds=random.randint(5, 60))

    # logout
    events.append(logout_event(t, user_id, session_id, platform))
    return events


def generate_events_for_day(day_start: datetime, cfg: GeneratorConfig, signed_up_users: set[int]) -> List[Dict[str, Any]]:
    """Generate all events for a single day."""
    events: List[Dict[str, Any]] = []
    platform_weights = [0.6, 0.25, 0.15]  # web heavier

    for user_id in range(1, cfg.num_users + 1):
        is_active = random.random() < cfg.daily_active_rate
        if not is_active:
            continue

        platform = random.choices(ALLOWED_PLATFORMS, weights=platform_weights, k=1)[0]
        sessions_today = random.randint(*cfg.sessions_per_active_user)

        for s in range(1, sessions_today + 1):
            session_id = make_session_id(user_id, s, day_start)

            # if user has never signed up, maybe do it (only once)
            if user_id not in signed_up_users and random.random() < cfg.signup_rate:
                # signup time near start of first session
                signup_time = random_time_within_day(day_start)
                events.append(signup_event(signup_time, user_id, session_id, platform))
                signed_up_users.add(user_id)

            events.extend(simulate_session(day_start, user_id, session_id, platform, cfg))

    # Sort by event_time (helps downstream sanity)
    events.sort(key=lambda e: e["event_time"])
    return events


def main() -> None:
    # Make results reproducible
    random.seed(42)

    cfg = GeneratorConfig(
        start_date="2026-01-01",
        num_days=7,
        num_users=300,
    )

    start = parse_date_utc(cfg.start_date)
    signed_up_users: set[int] = set()

    for i in range(cfg.num_days):
        day_start = start + timedelta(days=i)
        day_str = day_start.date().isoformat()

        day_events = generate_events_for_day(day_start, cfg, signed_up_users)

        # Introduce a small amount of messy data
        day_events = [maybe_corrupt_event(e, cfg.bad_row_rate) for e in day_events]

        # Simulate ingestion retries (duplicate event_ids)
        if day_events:
            n_dupes = int(len(day_events) * cfg.duplicate_rate)
            for _ in range(n_dupes):
                day_events.append(random.choice(day_events))

        # Re-sort after adding duplicates
        day_events.sort(key=lambda e: e["event_time"])

        out_path = RAW_BASE / f"event_date={day_str}" / "events.jsonl" #split by day and stored as a jsonl
        write_jsonl(out_path, day_events)

        print(f"[OK] {day_str}: wrote {len(day_events):,} events -> {out_path}")

    print("\n Complete! All new even data logged in raw zone")


if __name__ == "__main__":
    main()

