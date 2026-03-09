"""
consolidate_fieldlogs.py

Merges fieldlog JSON files found in WORKING_DIR into a single consolidated JSON file.
The consolidated file is a flat list of sessions. Context (operator, equipment, location)
is embedded within each session in the source files.

De-duplication is at the session level: a session is added only if it contains
at least one flight whose (drone_id + flight.endTime) key is not already present
in the consolidated file.

Each run:
  - Archives the existing consolidated.json to consolidated_archive/ (timestamped)
  - Scans WORKING_DIR for any new JSON source files
  - Ingests sessions containing new flights
  - Writes a fresh consolidated.json
  - Moves all source files to processed/

Usage:
    python consolidate_fieldlogs.py
"""

import json
import sys
import os
import shutil
from datetime import datetime
from pathlib import Path

# ── Configuration ────────────────────────────────────────────────────────────
WORKING_DIR = r"C:\Users\charles.isles\Desktop\Test Consolidate"  # Set this to your folder, e.g. r"C:\Users\Charlie\fieldlogs"
# ─────────────────────────────────────────────────────────────────────────────

CONSOLIDATED_FILE = os.path.join(WORKING_DIR, "consolidated.json")
PROCESSED_DIR = os.path.join(WORKING_DIR, "processed")
ARCHIVE_DIR = os.path.join(WORKING_DIR, "consolidated_archive")


def ensure_dirs():
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    os.makedirs(ARCHIVE_DIR, exist_ok=True)


def load_consolidated(path):
    """Load the consolidated file, or return an empty list if it doesn't exist."""
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return []


def save_consolidated(path, sessions):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(sessions, f, indent=2)


def archive_consolidated(path):
    if not os.path.exists(path):
        return
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    archive_path = os.path.join(ARCHIVE_DIR, f"consolidated_{timestamp}.json")
    shutil.copy2(path, archive_path)
    print(f"Archived existing consolidated file to {archive_path}")


def build_existing_keys(sessions):
    """Build a set of drone_id+endTime keys from all flights in the consolidated list."""
    keys = set()
    for session in sessions:
        drone_id = session.get("equipment", {}).get("drone_id", "")
        for flight in session.get("flights", []):
            end_time = flight.get("endTime", "")
            if drone_id and end_time:
                keys.add(f"{drone_id}|{end_time}")
    return keys


def session_has_new_flights(session, existing_keys):
    """Return True if any flight in this session is not already in the consolidated file."""
    drone_id = session.get("equipment", {}).get("drone_id", "")
    for flight in session.get("flights", []):
        end_time = flight.get("endTime", "")
        if f"{drone_id}|{end_time}" not in existing_keys:
            return True
    return False


def ingest_file(source_path, sessions, existing_keys):
    """Read a source file and add any sessions containing new flights."""
    with open(source_path, "r", encoding="utf-8") as f:
        source = json.load(f)

    new_count = 0
    for session in source.get("sessions", []):
        if session_has_new_flights(session, existing_keys):
            sessions.append(session)
            # Update keys so later files don't re-add the same flights
            drone_id = session.get("equipment", {}).get("drone_id", "")
            for flight in session.get("flights", []):
                end_time = flight.get("endTime", "")
                if drone_id and end_time:
                    existing_keys.add(f"{drone_id}|{end_time}")
            new_count += 1

    return new_count


def move_to_processed(source_path):
    filename = os.path.basename(source_path)
    dest = os.path.join(PROCESSED_DIR, filename)
    if os.path.exists(dest):
        stem, ext = os.path.splitext(filename)
        timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        dest = os.path.join(PROCESSED_DIR, f"{stem}_{timestamp}{ext}")
    shutil.move(source_path, dest)


def main():
    consolidated_resolved = str(Path(CONSOLIDATED_FILE).resolve())

    source_files = [
        str(f) for f in Path(WORKING_DIR).glob("*.json")
        if str(f.resolve()) != consolidated_resolved
    ]

    if not source_files:
        print("No source JSON files found in WORKING_DIR. Nothing to do.")
        sys.exit(0)

    ensure_dirs()
    archive_consolidated(CONSOLIDATED_FILE)

    sessions = load_consolidated(CONSOLIDATED_FILE)
    existing_keys = build_existing_keys(sessions)

    print(f"Starting with {len(existing_keys)} existing flights\n")

    total_new_sessions = 0
    for source_path in sorted(source_files):
        new_sessions = ingest_file(source_path, sessions, existing_keys)
        print(f"  {source_path}: {new_sessions} new session(s) added")
        move_to_processed(source_path)
        total_new_sessions += new_sessions

    save_consolidated(CONSOLIDATED_FILE, sessions)
    print(f"\n{total_new_sessions} new session(s) added. consolidated.json updated.")


if __name__ == "__main__":
    main()