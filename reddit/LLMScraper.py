

import os
import json
import time
import logging
from datetime import datetime, date
from dotenv import load_dotenv
import praw
import requests
from tqdm import tqdm
from openai import OpenAI

load_dotenv(dotenv_path=".env")

logging.basicConfig(
    filename="llm_pipeline.log",
    filemode="a",
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO,
)

OPENAI_KEY = os.getenv("OPENAI_API_KEY")
GOOGLE_KEY = os.getenv("GOOGLE_MAPS_API_KEY") or "YOUR_RESTRICTED_GOOGLE_API_KEY"  # Replace safely

if not OPENAI_KEY:
    raise ValueError("OPENAI_API_KEY not found.")

for key in ["REDDIT_CLIENT_ID", "REDDIT_CLIENT_SECRET", "REDDIT_USER_AGENT"]:
    if not os.getenv(key):
        raise ValueError(f"Missing {key} in .env")

client = OpenAI(api_key=OPENAI_KEY)
reddit = praw.Reddit(
    client_id=os.getenv("REDDIT_CLIENT_ID"),
    client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
    user_agent=os.getenv("REDDIT_USER_AGENT"),
)

SUBREDDIT = "PurdueHousing"
LIMIT = 300
OUTPUT_FILE = "llm_posts.json"
MODEL = "gpt-4o-mini"

def get_semester_start(today: date = None) -> date:
    today = today or date.today()
    year = today.year
    spring_start, summer_start, fall_start = date(year, 1, 1), date(year, 5, 15), date(year, 8, 1)
    if today >= fall_start:
        return fall_start
    elif today >= summer_start:
        return summer_start
    return spring_start

semester_start = get_semester_start()
semester_start_ts = datetime.combine(semester_start, datetime.min.time()).timestamp()

CACHE = {}

def get_coordinates_from_google(place_name: str):
    if not place_name:
        return None, None, None, None, None
    if place_name in CACHE:
        return CACHE[place_name]

    try:
        query = f"{place_name}, West Lafayette, IN"
        url = (
            f"https://maps.googleapis.com/maps/api/place/findplacefromtext/json"
            f"?input={query}&inputtype=textquery"
            f"&fields=name,geometry,place_id,formatted_address,website"
            f"&key={GOOGLE_KEY}"
        )
        resp = requests.get(url, timeout=8)
        data = resp.json()

        if data.get("candidates"):
            c = data["candidates"][0]
            lat = c["geometry"]["location"]["lat"]
            lon = c["geometry"]["location"]["lng"]
            pid = c.get("place_id")
            address = c.get("formatted_address")
            website = c.get("website")

            maps_link = f"https://www.google.com/maps/place/?q=place_id:{pid}" if pid else None
            CACHE[place_name] = (lat, lon, pid, address, maps_link or website)
            return lat, lon, pid, address, maps_link or website
        else:
            return None, None, None, None, None
    except Exception as e:
        logging.warning(f"Google API error for {place_name}: {e}")
        return None, None, None, None, None

SYSTEM_PROMPT = """
You are a structured data extraction assistant for Reddit housing listings.

Given TITLE, BODY, and URL, output a JSON object with these exact fields:

{
  "price": int or null,
  "price_confidence": float 0-1,
  "bedsCount": float or null,
  "bathsCount": float or null,
  "gender": "male" | "female" | null,
  "isNegotiable": true | false | null,
  "includesUtilities": true | false | null,
  "includesAmenities": true | false | null,
  "allUtilities": [list of mentioned utilities like water, gas, internet, electricity, trash],
  "allAmenities": [list of amenities like gym, parking, washer, dryer, pool, pet friendly],
  "sublettingTrue": true | false | null,
  "myHousingName": string or null (from: Rise on Chauncey, Hub West Lafayette, Fuse, Aspire, Lark, Alight, Redpoint, Yugo River Market, Station 21, Campus Edge, Village West, Beau Jardin, University Crossing, Wabash Landing, Evergreen Campus Rentals, Grant Street, Chauncey Square, The Cottages on Lindberg),
  "latitude": float or null,
  "longitude": float or null,
  "placeID": string or null,
  "placeAddress": string or null,
  "links": [list of up to 3 relevant URLs like official site, Google Maps link, leasing page, or social media if mentioned]
}

Guidelines:
- Do not assume true/false; use null when unclear.
- Use reasoning, not regex.
- Return valid JSON only.
"""

def extract_with_gpt(title: str, body: str, url: str) -> dict:
    text_input = f"TITLE:\n{title}\n\nBODY:\n{body}\n\nURL:\n{url}"
    try:
        response = client.chat.completions.create(
            model=MODEL,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": text_input},
            ],
            temperature=0.2,
        )
        raw = response.choices[0].message.content.strip()
        data = json.loads(raw)

        for k in ["isNegotiable", "includesUtilities", "includesAmenities", "sublettingTrue"]:
            if isinstance(data.get(k), str):
                val = data[k].lower().strip()
                if val in ["true", "yes", "included"]:
                    data[k] = True
                elif val in ["false", "no", "not included"]:
                    data[k] = False
                else:
                    data[k] = None

        hname = data.get("myHousingName")
        lat, lon, pid, addr, maps_link = get_coordinates_from_google(hname)
        data["latitude"], data["longitude"] = lat, lon
        data["placeID"], data["placeAddress"] = pid, addr
        data["links"] = data.get("links", [])
        if maps_link and maps_link not in data["links"]:
            data["links"].append(maps_link)

        return data

    except json.JSONDecodeError:
        logging.error("Invalid JSON from GPT")
        return {"error": "Invalid JSON returned", "raw": raw}
    except Exception as e:
        logging.error(f"LLM extraction error: {e}")
        return {"error": str(e)}

def extract_posts_with_llm(subreddit_name: str, limit: int):
    subreddit = reddit.subreddit(subreddit_name)
    posts = []
    for post in tqdm(subreddit.new(limit=limit)):
        try:
            if post.created_utc < semester_start_ts:
                continue

            base = {
                "id": post.id,
                "title": post.title or "",
                "author": str(post.author),
                "created_utc": datetime.utcfromtimestamp(post.created_utc).isoformat(),
                "permalink": f"https://reddit.com{post.permalink}",
                "flair": post.link_flair_text,
                "selftext": post.selftext or "",
                "score": post.score,
                "num_comments": post.num_comments,
                "url": post.url or "",
                "scraped_at": datetime.utcnow().isoformat(),
            }

            llm_fields = extract_with_gpt(base["title"], base["selftext"], base["url"])
            base["llm_fields"] = llm_fields
            posts.append(base)

            time.sleep(0.5)  # gentle rate limit
        except Exception as e:
            logging.warning(f"Skipping post {getattr(post, 'id', '?')} due to error: {e}")
            continue
    return posts

def save_posts(posts, path):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(posts, f, indent=2, ensure_ascii=False)
    logging.info(f"Saved {len(posts)} posts to {path}")

if __name__ == "__main__":
    try:
        posts = extract_posts_with_llm(SUBREDDIT, LIMIT)
        save_posts(posts, OUTPUT_FILE)
    except Exception as e:
        logging.exception("Pipeline failed")
