import json
import re
import datetime
import uuid

INPUT_FILE = "zillow_data.json"
OUTPUT_FILE = "cleaned_listings.json"


def parse_price(price_string):
    # convert price into numbers
    if not price_string:
        return None

    price_string = str(price_string)
    cleaned = re.sub(r"[^\d.]", "", price_string)

    try:
        return float(cleaned)
    except:
        return None
    
def get_title(listing, address):
    building_name = listing.get("buildingName")

    if building_name and building_name.strip():
        return f"{building_name} | {address}"

    return address # no building name, just put address
    
def get_link(listing):
    url = listing.get("detailUrl")

    if not url:
        return None

    # sometimes only has /apartments/jflaksjflsajf
    if url.startswith("/"):
        return f"https://www.zillow.com{url}"

    return url


def extract_photo_keys(listing):
    # take keys from zillow json
    keys = []

    photos = listing.get("carouselPhotosComposable", {}).get("photoData", [])

    for photo in photos:
        key = photo.get("photoKey")
        if key:
            keys.append(key)

    return keys


def create_json():
    with open(INPUT_FILE, "r") as f:
        data = json.load(f)

    listings = data if isinstance(data, list) else data.get("results", [])

    output = []

    now = datetime.datetime.utcnow().isoformat()

    for listing in listings:
        # if we ever want the link, it is just
        # https://www.zillow.com/homedetails/{base_id}_zpid/
        base_id = listing.get("id") or str(uuid.uuid4())
        address = listing.get("address")
        title = get_title(listing, address)

        lat = None
        lon = None

        building_name = listing.get("")

        if listing.get("latLong"): # 2 parts
            lat = listing["latLong"].get("latitude")
            lon = listing["latLong"].get("longitude")

        photo_keys = extract_photo_keys(listing)

        units = listing.get("units")

        if not units:
            units = [{
                "price": listing.get("price"),
                "beds": listing.get("beds"),
                "baths": listing.get("baths")
            }]

        for index, unit in enumerate(units):

            price = parse_price(unit.get("price"))

            beds = None
            try:
                beds = float(unit.get("beds")) if unit.get("beds") else None
            except:
                beds = None

            baths = None
            try:
                baths = float(unit.get("baths")) if unit.get("baths") else None
            except:
                baths = None


            listing_obj = {

                "id": f"{base_id}_{index}",
                "universityId": "purdue",
                "sourceType": "zillow",
                "listingCategory": "official", # ask
                "title": title,
                "description": "", # intentionally blank for now, could use beds bath count
                "price": price,
                "bedsCount": beds,
                "bathsCount": baths,
                "address": address,
                "latitude": lat,
                "longitude": lon,
                "leaseStart": None, # lease start sometimes given, work on this
                "leaseEnd": None,
                "Negotiable": None,
                "genderRestriction": None,
                "utilities": [], # maybe fill later
                "amenities": [], # maybe fill later
                "status": "active",
                "expiresAt": None,
                "duplicateGroupId": None, # used later i think
                "createdAt": None, # later
                "scrapedAt": now, # timestamp of current time
                "photos": photo_keys, # https://photos.zillowstatic.com/fp/{photoKey}-p_e.jpg for the actual photo
                "link": get_link(listing)
            }

            output.append(listing_obj)

    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2)

    # print(f"{OUTPUT_FILE}")

if __name__ == "__main__":
    create_json()