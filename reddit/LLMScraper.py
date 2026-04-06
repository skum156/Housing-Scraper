import os
import json
import time
import logging
import hashlib
import re

from datetime import datetime, date, timedelta
from pathlib import Path

import praw
import requests
from tqdm import tqdm
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv(Path(__file__).resolve().parent / ".env")

OPENAI_KEY = os.getenv("OPENAI_API_KEY")
GOOGLE_KEY = os.getenv("GOOGLE_KEY")

if not OPENAI_KEY:
    raise ValueError("OPENAI_API_KEY missing")

for key in ["REDDIT_CLIENT_ID", "REDDIT_CLIENT_SECRET", "REDDIT_USER_AGENT"]:
    if not os.getenv(key):
        raise ValueError(f"Missing {key}")

client = OpenAI(api_key=OPENAI_KEY)

reddit = praw.Reddit(
    client_id=os.getenv("REDDIT_CLIENT_ID"),
    client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
    user_agent=os.getenv("REDDIT_USER_AGENT"),
)

logging.basicConfig(
    filename="llm_pipeline.log",
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)




SUBREDDIT = "PurdueHousing"
LIMIT = 3
OUTPUT_FILE = "llm_posts.json"
MODEL = "gpt-4o-mini"




def get_semester_start(today=None):

    today = today or date.today()
    year = today.year

    spring = date(year, 1, 1)
    summer = date(year, 5, 15)
    fall = date(year, 8, 1)

    if today >= fall:
        return fall
    elif today >= summer:
        return summer
    return spring


semester_start = get_semester_start()

semester_start_ts = datetime.combine(
    semester_start,
    datetime.min.time()
).timestamp()


CACHE = {}

def get_coordinates_from_google(query):

    if not query:
        return None, None, None

    if query in CACHE:
        return CACHE[query]

    try:

        params = {
            "input": f"{query} West Lafayette",
            "inputtype": "textquery",
            "fields": "geometry,formatted_address",
            "key": GOOGLE_KEY,
        }

        resp = requests.get(
            "https://maps.googleapis.com/maps/api/place/findplacefromtext/json",
            params=params,
            timeout=10,
        )

        data = resp.json()

        if data.get("status") != "OK":
            return None, None, None

        candidate = data["candidates"][0]

        lat = candidate["geometry"]["location"]["lat"]
        lon = candidate["geometry"]["location"]["lng"]
        address = candidate["formatted_address"]

        CACHE[query] = (lat, lon, address)

        return lat, lon, address

    except Exception as e:

        logging.warning(f"Google API error: {e}")

        return None, None, None




KNOWN_APARTMENTS = [
    "chauncey square",
    "rise on chauncey",
    "verve",
    "hub",
    "fuse",
    "aspire",
    "lark",
    "alight",
    "redpoint",
    "station 21",
    "campus edge",
    "village west",
    "beau jardin",
]


def derive_location_query(title, body):

    text = f"{title} {body}".lower()

    for place in KNOWN_APARTMENTS:
        if place in text:
            return place

    if "purdue" in text:
        return "Purdue University"

    return title




SYSTEM_PROMPT = """
Extract structured data from this housing listing.

Return JSON:

{
"price": int or null,
"bedsCount": float or null,
"bathsCount": float or null,
"gender": "male" | "female" | null,
"isNegotiable": true | false | null,
"allUtilities": [],
"allAmenities": [],
"sublettingTrue": true | false | null
}
"""


def extract_with_gpt(title, body):

    try:

        prompt = f"TITLE:\n{title}\n\nBODY:\n{body}"

        response = client.chat.completions.create(
            model=MODEL,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": prompt},
            ],
            temperature=0.2,
        )

        raw = response.choices[0].message.content.strip()

        return json.loads(raw)

    except Exception as e:

        logging.error(f"LLM extraction error: {e}")

        return {}




def build_canonical_listing(post, llm):

    now = datetime.utcnow()

    price = llm.get("price")
    beds = llm.get("bedsCount")
    baths = llm.get("bathsCount")

    text = f"{post['title']} {post['selftext']}".lower()

    if beds is None and "studio" in text:
        beds = 1

    if beds is None:
        match = re.search(r'(\d+)\s*(bed|bedroom)', text)
        if match:
            beds = int(match.group(1))

    query = derive_location_query(post["title"], post["selftext"])

    lat, lon, address = get_coordinates_from_google(query)

    utilities = llm.get("allUtilities") or []
    amenities = llm.get("allAmenities") or []

    expires_at = now + timedelta(days=14)

    address_norm = (address or "").lower().strip()

    dedup_key = f"{address_norm}-{price}-{beds}"

    duplicate_group_id = hashlib.md5(
        dedup_key.encode()
    ).hexdigest()

    listing = {

        "id": post["id"],
        "universityId": "purdue",
        "sourceType": "reddit",
        "listingCategory": "sublease" if llm.get("sublettingTrue") else "official",

        "title": post["title"],
        "description": post["selftext"],

        "price": price,
        "bedsCount": beds,
        "bathsCount": baths,

        "address": address,
        "latitude": lat,
        "longitude": lon,

        "leaseStart": None,
        "leaseEnd": None,

        "negotiable": llm.get("isNegotiable"),
        "genderRestriction": llm.get("gender"),

        "utilities": utilities,
        "amenities": amenities,

        "status": "active",
        "expiresAt": expires_at.isoformat(),

        "duplicateGroupId": duplicate_group_id,

        "createdAt": post["created_utc"],
        "scrapedAt": post["scraped_at"],
    }

    return listing




def extract_posts_with_llm(subreddit_name, limit):

    subreddit = reddit.subreddit(subreddit_name)

    listings = []

    for post in tqdm(subreddit.new(limit=limit)):

        try:

            if post.created_utc < semester_start_ts:
                continue

            base = {

                "id": post.id,
                "title": post.title or "",
                "selftext": post.selftext or "",
                "created_utc": datetime.utcfromtimestamp(
                    post.created_utc
                ).isoformat(),
                "scraped_at": datetime.utcnow().isoformat(),
            }

            llm_fields = extract_with_gpt(
                base["title"],
                base["selftext"]
            )

            listing = build_canonical_listing(
                base,
                llm_fields
            )

            listings.append(listing)

            time.sleep(0.5)

        except Exception as e:

            logging.warning(f"Skipping post {post.id}: {e}")

    return listings




def save_posts(posts, path):

    with open(path, "w", encoding="utf-8") as f:

        json.dump(posts, f, indent=2, ensure_ascii=False)

    print(f"\nSaved {len(posts)} listings to {path}")


if __name__ == "__main__":

    posts = extract_posts_with_llm(
        SUBREDDIT,
        LIMIT
    )


    save_posts(posts, OUTPUT_FILE)
