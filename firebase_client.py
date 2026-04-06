import json
import os
from datetime import datetime, timezone
from typing import Any

import firebase_admin
from firebase_admin import credentials, firestore



BASE_DIR = os.path.dirname(os.path.abspath(__file__))

FIREBASE_CREDENTIALS_PATH = os.path.join(BASE_DIR, "firebase_credentials.json")
NORMALIZED_JSON_PATH = os.path.join(BASE_DIR, "normalized_output.json")

LISTINGS_COLLECTION = "listings"

# If a listing was not seen in this run, mark it inactive.
MARK_MISSING_LISTINGS_INACTIVE = True

# If expiresAt is missing, keep listing active unless unseen in later runs.
DEFAULT_STATUS_IF_NO_EXPIRY = "active"

def init_firestore():
    if not os.path.exists(FIREBASE_CREDENTIALS_PATH):
        raise FileNotFoundError(
            f"Firebase credentials not found at: {FIREBASE_CREDENTIALS_PATH}"
        )

    if not firebase_admin._apps:
        cred = credentials.Certificate(FIREBASE_CREDENTIALS_PATH)
        firebase_admin.initialize_app(cred)

    return firestore.client()



def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat()


def parse_datetime_safe(value: Any) -> datetime | None:
    if value is None:
        return None

    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value).strip()
        if not text:
            return None

        if text.endswith("Z"):
            text = text[:-1] + "+00:00"

        try:
            dt = datetime.fromisoformat(text)
        except ValueError:
            return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(timezone.utc)


def is_expired(expires_at: Any) -> bool:
    dt = parse_datetime_safe(expires_at)
    if dt is None:
        return False
    return dt < utc_now()


def clean_string(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def choose_primary_listing(existing: dict[str, Any] | None, candidate: dict[str, Any]) -> dict[str, Any]:
    """
    Decide which listing should be primary inside a duplicate group.

    Preference order:
    1. active over inactive
    2. has address/lat/lng over missing
    3. lower price
    4. newer scrapedAt/createdAt
    """
    if existing is None:
        return candidate

    def score(item: dict[str, Any]) -> tuple:
        active_score = 1 if item.get("status") == "active" else 0
        complete_loc_score = 1 if item.get("address") and item.get("latitude") is not None and item.get("longitude") is not None else 0

        price = item.get("price")
        if isinstance(price, (int, float)):
            price_score = -float(price)  # lower is better
        else:
            price_score = float("-inf")

        scraped_at = parse_datetime_safe(item.get("scrapedAt")) or parse_datetime_safe(item.get("createdAt"))
        timestamp_score = scraped_at.timestamp() if scraped_at else float("-inf")

        return (active_score, complete_loc_score, price_score, timestamp_score)

    return candidate if score(candidate) > score(existing) else existing


def load_normalized_data() -> list[dict[str, Any]]:
    if not os.path.exists(NORMALIZED_JSON_PATH):
        raise FileNotFoundError(
            f"Normalized JSON not found at: {NORMALIZED_JSON_PATH}"
        )

    with open(NORMALIZED_JSON_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError("normalized_output.json must contain a JSON list.")

    cleaned: list[dict[str, Any]] = []
    for item in data:
        if isinstance(item, dict):
            cleaned.append(item)

    return cleaned


def assign_primary_flags(listings: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    For each duplicateGroupId, choose one listing as primary.
    Listings without duplicateGroupId are primary by default.
    """
    best_by_group: dict[str, dict[str, Any]] = {}

    for listing in listings:
        group_id = clean_string(listing.get("duplicateGroupId"))
        if not group_id:
            continue
        best_by_group[group_id] = choose_primary_listing(best_by_group.get(group_id), listing)

    for listing in listings:
        group_id = clean_string(listing.get("duplicateGroupId"))
        if not group_id:
            listing["isPrimary"] = True
            listing["primaryListingId"] = listing.get("id")
        else:
            primary = best_by_group[group_id]
            is_primary = primary.get("id") == listing.get("id")
            listing["isPrimary"] = is_primary
            listing["primaryListingId"] = primary.get("id")

    return listings


def prepare_listing_for_firestore(listing: dict[str, Any], run_started_at: str) -> dict[str, Any]:
    item = dict(listing)

    listing_id = clean_string(item.get("id"))
    if not listing_id:
        raise ValueError("Listing is missing id.")

    item["id"] = listing_id
    item["sourceType"] = clean_string(item.get("sourceType")) or "unknown"
    item["universityId"] = clean_string(item.get("universityId")) or "purdue"
    item["listingCategory"] = clean_string(item.get("listingCategory")) or "sublease"

    # status from expiresAt
    expires_at = item.get("expiresAt")
    if expires_at:
        item["status"] = "inactive" if is_expired(expires_at) else "active"
    else:
        item["status"] = DEFAULT_STATUS_IF_NO_EXPIRY

    # ingestion metadata
    item["lastSeenAt"] = run_started_at
    item["updatedAt"] = run_started_at

    if not item.get("createdAt"):
        item["createdAt"] = run_started_at

    # ensure bool fields exist
    item["isPrimary"] = bool(item.get("isPrimary", False))
    item["isComplete"] = bool(item.get("isComplete", False))

    # normalize lists
    if not isinstance(item.get("amenities"), list):
        item["amenities"] = []
    if not isinstance(item.get("utilities"), list):
        item["utilities"] = []

    # optional grouping helper
    item["duplicateGroupId"] = clean_string(item.get("duplicateGroupId"))
    item["primaryListingId"] = clean_string(item.get("primaryListingId")) or listing_id

    return item


def upsert_listings(db: firestore.Client, listings: list[dict[str, Any]], run_started_at: str) -> set[str]:
    """
    Upserts all listings into Firestore.
    Returns the set of listing ids seen in this run.
    """
    seen_ids: set[str] = set()

    batch = db.batch()
    writes_in_batch = 0
    max_batch_size = 400  

    for raw_listing in listings:
        item = prepare_listing_for_firestore(raw_listing, run_started_at)
        listing_id = item["id"]
        seen_ids.add(listing_id)

        doc_ref = db.collection(LISTINGS_COLLECTION).document(listing_id)
        batch.set(doc_ref, item, merge=True)
        writes_in_batch += 1

        if writes_in_batch >= max_batch_size:
            batch.commit()
            batch = db.batch()
            writes_in_batch = 0

    if writes_in_batch > 0:
        batch.commit()

    return seen_ids


def mark_unseen_listings_inactive(db: firestore.Client, seen_ids: set[str], run_started_at: str) -> int:
    """
    Marks listings inactive if they were not present in this run.
    Useful when a source listing disappears but has no expiresAt.
    """
    count = 0
    docs = db.collection(LISTINGS_COLLECTION).stream()

    batch = db.batch()
    writes_in_batch = 0
    max_batch_size = 400

    for doc in docs:
        data = doc.to_dict() or {}
        listing_id = data.get("id") or doc.id

        if listing_id in seen_ids:
            continue

        # already inactive, no need to rewrite
        if data.get("status") == "inactive":
            continue

        batch.set(
            doc.reference,
            {
                "status": "inactive",
                "updatedAt": run_started_at,
                "inactiveReason": "not_seen_in_latest_ingestion",
            },
            merge=True,
        )
        writes_in_batch += 1
        count += 1

        if writes_in_batch >= max_batch_size:
            batch.commit()
            batch = db.batch()
            writes_in_batch = 0

    if writes_in_batch > 0:
        batch.commit()

    return count



def upsert_duplicate_group_summaries(db: firestore.Client, listings: list[dict[str, Any]], run_started_at: str) -> None:
    """
    Writes a lightweight summary collection for duplicate groups.
    This is optional but useful for querying grouped listings later.
    """
    groups: dict[str, list[dict[str, Any]]] = {}

    for item in listings:
        group_id = clean_string(item.get("duplicateGroupId"))
        if not group_id:
            continue
        groups.setdefault(group_id, []).append(item)

    if not groups:
        return

    batch = db.batch()
    writes_in_batch = 0
    max_batch_size = 400

    for group_id, items in groups.items():
        primary = None
        for item in items:
            if item.get("isPrimary"):
                primary = item
                break
        if primary is None:
            primary = items[0]

        summary = {
            "duplicateGroupId": group_id,
            "primaryListingId": primary.get("id"),
            "listingIds": [x.get("id") for x in items if x.get("id")],
            "count": len(items),
            "sourceTypes": sorted({x.get("sourceType") for x in items if x.get("sourceType")}),
            "updatedAt": run_started_at,
            "address": primary.get("address"),
            "latitude": primary.get("latitude"),
            "longitude": primary.get("longitude"),
            "status": primary.get("status"),
        }

        doc_ref = db.collection("duplicate_groups").document(group_id)
        batch.set(doc_ref, summary, merge=True)
        writes_in_batch += 1

        if writes_in_batch >= max_batch_size:
            batch.commit()
            batch = db.batch()
            writes_in_batch = 0

    if writes_in_batch > 0:
        batch.commit()



def main():
    run_started_at = utc_now_iso()

    print("Loading normalized data...")
    listings = load_normalized_data()
    print(f"Loaded {len(listings)} listings.")

    print("Assigning primary listings inside duplicate groups...")
    listings = assign_primary_flags(listings)

    print("Connecting to Firestore...")
    db = init_firestore()

    print("Uploading listings...")
    seen_ids = upsert_listings(db, listings, run_started_at)
    print(f"Upserted {len(seen_ids)} listings into '{LISTINGS_COLLECTION}'.")

    print("Writing duplicate group summaries...")
    upsert_duplicate_group_summaries(db, listings, run_started_at)
    print("Duplicate group summaries updated.")

    if MARK_MISSING_LISTINGS_INACTIVE:
        print("Marking unseen old listings inactive...")
        num_marked = mark_unseen_listings_inactive(db, seen_ids, run_started_at)
        print(f"Marked {num_marked} unseen listings inactive.")

    print("Done.")


if __name__ == "__main__":
    main()