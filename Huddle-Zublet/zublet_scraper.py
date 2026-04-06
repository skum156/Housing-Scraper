import hashlib
import json
import time
import requests

# ======================
# CONFIG
# ======================

UNIVERSITY_ID = "purdue"
SOURCE_TYPE = "zublet"

BASE_URL = "https://zublet-production.up.railway.app/listings/location/purdue"


# ======================
# UTILS
# ======================

def md5_text(text: str) -> str:
    return hashlib.md5(text.encode("utf-8")).hexdigest()


def safe_json_response(resp: requests.Response):
    try:
        return resp.json()
    except Exception:
        return None


# ======================
# EXTRACTION (NEW 🔥)
# ======================

def extract_amenities_and_utilities(item):
    amenities = set()
    utilities = set()

    desc = (item.get("description") or "").lower()

    # -------- API fields --------
    if item.get("furnished"):
        amenities.add("furnished")

    if item.get("laundry"):
        amenities.add("laundry")

    if item.get("parkingType"):
        amenities.add("parking")

    if item.get("petsAllowed") is True:
        amenities.add("pets allowed")
    elif item.get("petsAllowed") is False:
        amenities.add("pets not allowed")

    # -------- TEXT extraction --------
    if "wifi" in desc or "wi-fi" in desc or "internet" in desc:
        amenities.add("wifi")
        utilities.add("wifi")

    if "gym" in desc:
        amenities.add("gym")

    if "pool" in desc:
        amenities.add("pool")

    if "parking" in desc:
        amenities.add("parking")

    if "laundry" in desc or "washer" in desc or "dryer" in desc:
        amenities.add("laundry")

    if "furnished" in desc:
        amenities.add("furnished")

    if "utilities included" in desc or "all utilities" in desc:
        utilities.add("utilities included")

    if "water" in desc:
        utilities.add("water")

    if "electricity" in desc:
        utilities.add("electricity")

    if "gas" in desc:
        utilities.add("gas")

    return list(amenities), list(utilities)


# ======================
# CORE API FETCH
# ======================

def fetch_zublet_api():
    all_listings = []

    offset = 0
    limit = 50

    while True:
        params = {
            "limit": limit,
            "offset": offset,
            "sort": "RECENT"
        }

        try:
            resp = requests.get(
                BASE_URL,
                params=params,
                headers={
                    "User-Agent": "Mozilla/5.0",
                    "Accept": "application/json"
                },
                timeout=10
            )

            data = safe_json_response(resp)

            if not data or "listings" not in data:
                print("Invalid response")
                break

            listings = data["listings"]

            if not listings:
                print("No more listings")
                break

            for item in listings:
                amenities, utilities = extract_amenities_and_utilities(item)

                duplicate_group_id = md5_text(
                    f"{item.get('id')}|{item.get('startDate')}|{item.get('endDate')}"
                )

                all_listings.append({
                    "id": item.get("id"),

                    "universityId": UNIVERSITY_ID,
                    "sourceType": SOURCE_TYPE,

                    # treat everything as sublease for consistency
                    "listingCategory": "sublease",

                    "title": f"{item.get('address')} | {item.get('totalRooms')} Bedroom"
                             if item.get("address") else "Zublet Listing",

                    "description": item.get("description"),

                    "price": item.get("monthlyPrice"),

                    "bedsCount": item.get("totalRooms"),
                    "bathsCount": item.get("numBathrooms"),

                    # ✅ correct geo
                    "address": item.get("address"),
                    "latitude": item.get("latitude"),
                    "longitude": item.get("longitude"),

                    "leaseStart": item.get("startDate"),
                    "leaseEnd": item.get("endDate"),

                    "negotiable": None,

                    "genderRestriction": item.get("preferredTenantGender"),

                    # 🔥 now populated
                    "utilities": utilities,
                    "amenities": amenities,

                    "status": "active" if item.get("active") else "inactive",

                    "expiresAt": item.get("endDate"),

                    "duplicateGroupId": duplicate_group_id,
                })

            print(f"Fetched {len(listings)} listings (offset={offset})")

            offset += limit
            time.sleep(0.2)

        except Exception as e:
            print(f"API ERROR: {e}")
            break

    return all_listings


# ======================
# MAIN
# ======================

if __name__ == "__main__":
    print("Running Zublet API ingestion...")

    listings = fetch_zublet_api()

    with open("zublet_final.json", "w", encoding="utf-8") as f:
        json.dump(listings, f, indent=2, ensure_ascii=False)

    print(f"Saved {len(listings)} listings to zublet_final.json")