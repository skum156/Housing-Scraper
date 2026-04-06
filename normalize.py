import re
import hashlib
from typing import Any
from datetime import datetime
from dateutil import parser as date_parser



def md5_text(text: str) -> str:
    return hashlib.md5(text.encode("utf-8")).hexdigest()



def compute_duplicate_group_id(listing):
    addr = (listing.get("address") or "").lower().strip()
    lat = listing.get("latitude")
    lng = listing.get("longitude")

    # fallback if no location
    if lat is None or lng is None:
        key = addr
    else:
        # round coordinates to group nearby listings
        key = f"{addr}|{round(lat, 3)}|{round(lng, 3)}"

    return md5_text(key)



def normalize_listing(listing: dict[str, Any]) -> dict[str, Any]:
    listing = dict(listing)

    listing = normalize_strings(listing)
    listing = normalize_source_fields(listing)
    listing = normalize_price(listing)
    listing = normalize_beds_baths(listing)
    listing = normalize_dates(listing)
    listing = normalize_gender(listing)

    listing = enrich_from_description(listing)

    listing = normalize_amenities(listing)
    listing = normalize_utilities(listing)
    listing = normalize_location(listing)

    listing["duplicateGroupId"] = compute_duplicate_group_id(listing)

    listing = normalize_flags(listing)
    listing = ensure_required_fields(listing)

    return listing



def clean_string(value: Any) -> str | None:
    if value is None:
        return None
    value = str(value)
    value = re.sub(r"\s+", " ", value).strip()
    return value or None


def to_iso(date_value: Any) -> str | None:
    if not date_value:
        return None
    try:
        return date_parser.parse(str(date_value)).isoformat()
    except:
        return None


def parse_int_like(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return int(value)
    nums = re.findall(r"\d+", str(value))
    return int(nums[0]) if nums else None


def parse_float_like(value: Any):
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    match = re.search(r"\d+(?:\.\d+)?", str(value))
    return float(match.group()) if match else None



def normalize_strings(listing):
    for field in ["id", "universityId", "sourceType", "listingCategory", "title", "description", "address"]:
        listing[field] = clean_string(listing.get(field))
    return listing


def normalize_source_fields(listing):
    listing["sourceType"] = (listing.get("sourceType") or "").lower() or "unknown"
    listing["universityId"] = (listing.get("universityId") or "purdue").lower()
    listing["listingCategory"] = "sublease"
    listing["status"] = "active"
    return listing



def normalize_price(listing):
    listing["price"] = parse_int_like(listing.get("price"))
    return listing



def normalize_beds_baths(listing):
    listing["bedsCount"] = parse_int_like(listing.get("bedsCount"))
    listing["bathsCount"] = parse_float_like(listing.get("bathsCount"))
    return listing



def normalize_dates(listing):
    for field in ["leaseStart", "leaseEnd", "expiresAt", "createdAt", "scrapedAt"]:
        listing[field] = to_iso(listing.get(field))

    now = datetime.utcnow().isoformat()
    listing["createdAt"] = listing.get("createdAt") or now
    listing["scrapedAt"] = listing.get("scrapedAt") or now

    return listing


def normalize_gender(listing):
    text = (listing.get("genderRestriction") or listing.get("description") or "").lower()

    if "female" in text:
        listing["genderRestriction"] = "female"
    elif "male" in text:
        listing["genderRestriction"] = "male"
    else:
        listing["genderRestriction"] = None

    return listing

def enrich_from_description(listing):
    desc = (listing.get("description") or "").lower()

    amenities = set(listing.get("amenities") or [])
    utilities = set(listing.get("utilities") or [])

    if "furnished" in desc:
        amenities.add("furnished")

    if any(x in desc for x in ["washer", "dryer", "laundry"]):
        amenities.add("laundry")

    if "parking" in desc:
        amenities.add("parking")

    if "gym" in desc:
        amenities.add("gym")

    if "pool" in desc:
        amenities.add("pool")

    if "balcony" in desc:
        amenities.add("balcony")

    if "kitchen" in desc:
        amenities.add("kitchen")

    if "bus" in desc or "shuttle" in desc:
        amenities.add("transport")

    if "pet" in desc:
        if "not" in desc:
            amenities.add("pets not allowed")
        else:
            amenities.add("pets allowed")

    if any(x in desc for x in ["wifi", "wi-fi", "internet"]):
        utilities.add("wifi")

    if "water" in desc:
        utilities.add("water")

    if "electricity" in desc:
        utilities.add("electricity")

    if "gas" in desc:
        utilities.add("gas")

    if "utilities included" in desc:
        utilities.add("utilities included")

    if "plus utilities" in desc or "utilities not included" in desc:
        utilities.add("utilities not included")

    listing["amenities"] = list(amenities)
    listing["utilities"] = list(utilities)

    return listing


AMENITY_MAP = {
    "in unit laundry": "laundry",
    "fully furnished": "furnished",
    "free parking": "parking",
    "private bathroom": "private bath",
    "full kitchen": "kitchen",
}


def normalize_amenities(listing):
    cleaned = []
    for a in listing.get("amenities") or []:
        key = clean_string(a)
        if not key:
            continue
        key = key.lower()
        cleaned.append(AMENITY_MAP.get(key, key))

    listing["amenities"] = sorted(set(cleaned))
    return listing


def normalize_utilities(listing):
    cleaned = []
    for u in listing.get("utilities") or []:
        key = clean_string(u)
        if not key:
            continue
        key = key.lower()
        if key not in {"utility", "utilities"}:
            cleaned.append(key)

    listing["utilities"] = sorted(set(cleaned))
    return listing



def normalize_location(listing):
    try:
        listing["latitude"] = float(listing.get("latitude")) if listing.get("latitude") else None
    except:
        listing["latitude"] = None

    try:
        listing["longitude"] = float(listing.get("longitude")) if listing.get("longitude") else None
    except:
        listing["longitude"] = None

    return listing


def normalize_flags(listing):
    listing["negotiable"] = True if listing.get("negotiable") else False
    listing["isComplete"] = bool(listing.get("price") and listing.get("address"))
    return listing



def ensure_required_fields(listing):
    fields = [
        "id","universityId","sourceType","listingCategory",
        "title","description","price","bedsCount","bathsCount",
        "address","latitude","longitude","leaseStart","leaseEnd",
        "negotiable","genderRestriction","utilities","amenities",
        "status","expiresAt","duplicateGroupId","createdAt","scrapedAt"
    ]

    for f in fields:
        listing.setdefault(f, None)

    if not isinstance(listing["utilities"], list):
        listing["utilities"] = []
    if not isinstance(listing["amenities"], list):
        listing["amenities"] = []

    return listing