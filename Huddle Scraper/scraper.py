import os
import json
import time
import logging
from typing import List, Tuple, Optional, Dict
from datetime import datetime, date, timedelta
from dotenv import load_dotenv
import praw
from tqdm import tqdm
import re



load_dotenv()
logging.basicConfig(
    filename="pipeline.log",
    filemode="a",
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO
)


reddit = praw.Reddit(
    client_id=os.getenv("REDDIT_CLIENT_ID"),
    client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
    user_agent=os.getenv("REDDIT_USER_AGENT"),
)

try:
    me = reddit.user.me()
    print(f"Authenticated as: {me or 'Read-only mode'}")
    logging.info(f"Authenticated successfully as {me}")
except Exception as e:
    logging.exception(" Authentication failed")
    raise e


SUBREDDIT = "PurdueHousing"

LIMIT = 300
OUTPUT_FILE = "raw_posts.json"

#Semester Aware filtering
def get_semester_start(today: date = None) -> date:
    
    today = today or date.today()
    year = today.year
    spring_start = date(year, 1, 1)
    summer_start = date(year, 5, 15)
    fall_start = date(year, 8, 1)

    if today >= fall_start:
        return fall_start
    elif today >= summer_start:
        return summer_start
    else:
        return spring_start


semester_start = get_semester_start()
semester_start_ts = datetime.combine(semester_start, datetime.min.time()).timestamp()
print(f"📅 Semester filtering active: keeping posts after {semester_start.isoformat()}")

#Price Extraction 
MONTH_HINT_RE = re.compile(
    r"\b(per\s*month|/mo|/month|monthly|rent|lease|sublease|utilities|mo)\b", re.I
)
INCENTIVE_RE = re.compile(
    r"\b(credit|bonus|rebate|discount|cover|contribute|subsid(?:y|ies)|"
    r"pay\s+for|i\s+will\s+pay|one\s+month\s+free|free\s+month)\b", re.I,
)

PRICE_PATTERNS = [
    re.compile(r'\$(\d{2,5})(?:\.\d{1,2})?', re.I),
    re.compile(r'(?<!\d)(\d{3,5})\s*\$', re.I),
    re.compile(r'(?<!\d)(\d{3,5})\s*(?:/|per)\s*(?:mo|month)\b', re.I),
    re.compile(r'(?<!\d)(\d{3,5})\s*(?:per\s*month|monthly|rent)\b', re.I),
    re.compile(r'(\d+(?:\.\d+)?)\s*[kK]\b', re.I),
]

def _normalize(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\u00a0", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()

def _safe_float(s: str) -> Optional[float]:
    try:
        return float(s.replace(",", ""))
    except Exception:
        return None

def _collect_candidates_field(text: str) -> List[Tuple[float, int, int]]:
    text = _normalize(text)
    if not text:
        return []
        
    

    cands = []
    for pat in PRICE_PATTERNS:
        for m in pat.finditer(text):
            val = _safe_float(m.group(1))
            if val is None:
                continue
            if pat is PRICE_PATTERNS[-1]:
                val *= 1000.0
            start = m.start()
            win = text[max(0, start - 40): start + 60]

            # Ignore years or incentive-like small numbers
            if 2020 <= val <= 2035 and not MONTH_HINT_RE.search(win):
                continue
            if val < 300 and INCENTIVE_RE.search(win):
                continue

            ctx = sum([
                bool(MONTH_HINT_RE.search(win)),
                "$" in m.group(0),
                bool(re.search(r"\brent\s+(is|at|:)\b", win, re.I))
            ])
            if 200 <= val <= 6000:
                cands.append((val, start, ctx))
    return cands

def _collect_candidates_all(title: str, body: str, url: str):
    results = []
    for src, txt in (("title", title or ""), ("body", body or ""), ("url", url or "")):
        for v, i, c in _collect_candidates_field(txt):
            results.append((v, src, i, c))
    return results

def _score_and_pick(cands):
    if not cands:
        return None, 0.0, {"reason": "no_candidates"}
    source_rank = {"title": 0, "body": 1, "url": 2}
    cands_sorted = sorted(cands, key=lambda x: (-x[3], source_rank.get(x[1], 9), x[2], x[0]))
    distinct_vals = sorted({int(round(v)) for (v, *_rest) in cands})
    spread = max(distinct_vals) - min(distinct_vals) if distinct_vals else 0
    ambiguous = len(distinct_vals) >= 2 and spread >= 200
    best_v, best_src, best_idx, best_ctx = cands_sorted[0]
    price = int(round(best_v))
    conf = 0.95 if best_ctx >= 2 and best_src == "title" else 0.9 if best_ctx >= 2 else 0.7 if best_ctx == 1 else 0.5
    if ambiguous:
        conf = min(conf, 0.55)
    return price, {"source": "regex", "confidence": conf, "distinct_values": distinct_vals, "ambiguous": ambiguous}

#Property Extraction
PROPERTY_ALIASES = {
    "rise on chauncey": "Rise on Chauncey",
    "hub": "Hub West Lafayette",
    "fuse": "Fuse West Lafayette",
    "aspire": "Aspire West Lafayette",
    "lark": "Lark West Lafayette",
    "lark townhomes": "Lark Townhomes",
    "alight": "Alight West Lafayette",
    "redpoint": "Redpoint West Lafayette",
    "yugo river market": "Yugo River Market",
    "river market": "Yugo River Market",
    "station 21": "Station 21",
    "campus edge": "Campus Edge at Pierce",
    "village west": "Village West",
    "beau jardin": "Beau Jardin",
    "university crossing": "University Crossing",
    "wabash landing": "Wabash Landing",
    "evergreen campus rentals": "Evergreen Campus Rentals",
    "grant street": "Grant Street",
    "chauncey square": "Chauncey Square",
    "the cottages": "The Cottages on Lindberg",
    "cottages on lindberg": "The Cottages on Lindberg",
}

_ALIAS_ALTS = sorted(PROPERTY_ALIASES.keys(), key=len, reverse=True)
ALIASES_RE = re.compile(r"\b(" + "|".join(map(re.escape, _ALIAS_ALTS)) + r")\b", re.I)

def extract_my_housing_name(title: str, body: str) -> Optional[str]:
    text_full = f"{title} {body}"
    for m in ALIASES_RE.finditer(text_full.lower()):
        key = m.group(1).lower()
        if key in PROPERTY_ALIASES:
            return PROPERTY_ALIASES[key]
    return None

#Additional Fields Extraction
BEDS_RE = re.compile(r"(\d+(?:\.\d+)?)\s*[Bb](?:\s*[xX]\s*(\d+(?:\.\d+)?)[Bb])?", re.I)
BEDWORD_RE = re.compile(r"(\d+)\s*(?:bed|bedroom)s?", re.I)
BATH_RE = re.compile(r"(\d+(?:\.\d+)?)\s*(?:bath|ba)\b", re.I)
GENDER_RE = re.compile(r"\b(female|male|women|men|girl|boy|lad(y|ies)|gentlemen)\b", re.I)
NEGOTIABLE_RE = re.compile(r"\b(negotiable|flexible|price\s*flexible)\b", re.I)
UTILITIES_RE = re.compile(r"utilities\s*(included|covered|paid|included in rent)", re.I)
AMENITIES_RE = re.compile(r"\b(gym|pool|fitness|clubhouse|laundry|parking|washer|dryer)\b", re.I)
SUBLET_RE = re.compile(r"\b(sublease|sublet|lease\s*takeover|lease\s*transfer)\b", re.I)

def extract_extras(title: str, body: str) -> Dict:
    text = f"{title} {body}".lower()
    extras = {
        "bedsCount": None, "bathsCount": None, "gender": None,
        "isNegotiable": False, "includesUtilities": False,
        "includesAmenities": False, "sublettingTrue": False,
        "myHousingName": extract_my_housing_name(title, body)
    }

    if m := BEDS_RE.search(text):
        extras["bedsCount"] = float(m.group(1))
        if m.group(2): extras["bathsCount"] = float(m.group(2))
    elif m := BEDWORD_RE.search(text):
        extras["bedsCount"] = float(m.group(1))
    if extras["bathsCount"] is None and (m := BATH_RE.search(text)):
        extras["bathsCount"] = float(m.group(1))

    if m := GENDER_RE.search(text):
        g = m.group(1).lower()
        extras["gender"] = "female" if any(x in g for x in ("f", "w", "girl", "lady")) else "male"

    extras["isNegotiable"] = bool(NEGOTIABLE_RE.search(text))
    extras["includesUtilities"] = bool(UTILITIES_RE.search(text))
    extras["includesAmenities"] = bool(AMENITIES_RE.search(text))
    extras["sublettingTrue"] = bool(SUBLET_RE.search(text))

    return extras

#Extraction pipeline
def extract_posts(subreddit_name: str, limit: int):
    subreddit = reddit.subreddit(subreddit_name)
    posts = []
    print(f" Fetching up to {limit} posts from r/{subreddit_name} ...")

    for post in tqdm(subreddit.new(limit=limit)):
        try:
            if post.created_utc < semester_start_ts:
                continue  # skip old posts

            data = {
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

            price, meta = _score_and_pick(_collect_candidates_all(
                data["title"], data["selftext"], data["url"]
            ))
            data["price"], data["price_meta"] = price, meta
            data.update(extract_extras(data["title"], data["selftext"]))
            posts.append(data)
        except Exception as e:
            logging.warning(f"Skipping post {getattr(post, 'id', '?')} due to error: {e}")
            continue

   
    return posts

#Saving Results
def save_posts(posts, output_file):
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(posts, f, indent=2, ensure_ascii=False)
    print(f"Saved {len(posts)} posts to {output_file}")
    logging.info(f"Saved {len(posts)} posts to {output_file}")

#Running pipeline
if __name__ == "__main__":
    start = time.time()
    try:
        posts = extract_posts(SUBREDDIT, LIMIT)
        save_posts(posts, OUTPUT_FILE)
        print(f"Done. Took {time.time()-start:.1f}s")
    except Exception as e:
        logging.exception("Pipeline failed")