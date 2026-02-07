import json
import re
import logging
from datetime import datetime

RAW_FILE = "purdue_housing.json"
CLEANED_FILE = "cleaned_posts.json"

logging.basicConfig(
    filename="pipeline.log",
    filemode="a",
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO
    
)



def clean_text(text: str) -> str:
    if not text:
        return ""
    text = re.sub(r"[\u2022\t\n\r]+", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()

def extract_images_from_media_metadata(media_metadata):
    if not isinstance(media_metadata, dict):
        return []
    urls = []
    for img in media_metadata.values():
        try:
            url = img.get("s", {}).get("u") or ""
            if not url:
                previews = img.get("p") or []
                if previews:
                    url = previews[-1].get("u", "")  # largest preview
            if url:
                urls.append(url.replace("&amp;", "&"))
        except Exception:
            continue
    seen, out = set(), []
    for u in urls:
        if u and u not in seen:
            seen.add(u)
            out.append(u)
    return out

def extract_price(text):

    if not text:
        return None
    m = re.search(r"\$?\s?(\d{3,5})(?:\s?[/]?(?:month|mo|per|/)?)*", text.lower())
    return int(m.group(1)) if m else None

def extract_bed_bath(text):
    t = (text or "").lower()
    bed_match = re.search(r"(\d+)\s*(bed|br|bedroom)s?", t)
    bath_match = re.search(r"(\d+)\s*(bath|ba|bathroom)s?", t)
    beds = int(bed_match.group(1)) if bed_match else None
    baths = int(bath_match.group(1)) if bath_match else None
    return beds, baths

def infer_room_type(text):
    t = (text or "").lower()
    if "private room" in t or "private bed" in t:
        return "private"
    if "shared room" in t or "shared bed" in t or "shared bedroom" in t:
        return "shared"
    return None

def infer_gender_pref(text):
    t = (text or "").lower()
    if "female" in t and "male" not in t:
        return "female"
    if "male" in t and "female" not in t:
        return "male"
    return None

def convert_timestamp(utc_value):
    try:
        return datetime.utcfromtimestamp(float(utc_value)).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def reddit_to_canonical(child):

    try:
        d = child.get("data", {})

        title = clean_text(
            d.get("title", "")
        )
        body = clean_text(
            d.get("selftext", "") or d.get("body", "") or d.get("text", "") or d.get("description", "")
        )
        merged = f"{title} {body}".strip()

        images = []
        images = extract_images_from_media_metadata(d.get("media_metadata"))
        if not images:
            url = d.get("url")
            if isinstance(url, str) and url.lower().endswith((".jpg", ".jpeg", ".png", ".webp")):
                images = [url]
        if not images and isinstance(d.get("images"), list):
            images = d["images"]

        created_at = convert_timestamp(d.get("created_utc"))
        if not created_at and isinstance(d.get("created"), str):
            created_at = d["created"]  # already formatted in your flat file

        permalink = d.get("permalink")
        if permalink:
            permalink = f"https://reddit.com{permalink}"
        elif isinstance(d.get("url"), str) and d["url"].startswith("https://reddit.com/"):
            permalink = d["url"]

        price = extract_price(merged)
        if price is None and d.get("price"):
            try:
                price = int(str(d["price"]).strip())
            except Exception:
                pass

        canonical = {
            "source": "reddit",
            "source_id": d.get("id"),
            "title": title or None,
            "description": body or None,
            "price_monthly": price,
            "place_name": d.get("place_name") or None,  # flat may not have this
            "place_id": d.get("place_id") or None,
            "address": d.get("address") or None,
            "lat": d.get("lat") or d.get("latitude") or None,
            "lng": d.get("lng") or d.get("longitude") or None,
            "beds": None,
            "baths": None,
            "room_type": infer_room_type(merged),
            "gender_pref": infer_gender_pref(merged),
            "lease_start": d.get("lease_start") or None,
            "lease_end": d.get("lease_end") or None,
            "utilities_included": d.get("utilities_included") if isinstance(d.get("utilities_included"), bool) else None,
            "negotiable": d.get("negotiable") if isinstance(d.get("negotiable"), bool) else None,
            "furnished": d.get("furnished") if isinstance(d.get("furnished"), bool) else None,
            "amenities": d.get("amenities") if isinstance(d.get("amenities"), list) else [],
            "images": images,
            "permalink": permalink,
            "author": d.get("author"),
            "created_at": created_at,
        }

        beds_text, baths_text = extract_bed_bath(merged)
        canonical["beds"] = canonical["beds"] or beds_text
        canonical["baths"] = canonical["baths"] or baths_text

        if not canonical["source_id"] or not (canonical["title"] or canonical["description"]):
            raise ValueError("Missing essential field(s)")

        return canonical

    except Exception as e:
        logging.warning(f"Skipped reddit post: {e}")
        return None

# --------- I/O ---------

def load_raw_posts(file_path):
    """Load either a Reddit listing JSON (with data.children) or a flat list of posts."""
    with open(file_path, "r", encoding="utf-8") as f:
        blob = json.load(f)

    # Case 1: Reddit listing structure
    if isinstance(blob, dict) and "data" in blob and isinstance(blob["data"].get("children"), list):
        return blob["data"]["children"]

    # Case 2: Already-cleaned list of posts (flat)
    if isinstance(blob, list) and blob and isinstance(blob[0], dict):
        # Wrap each in {"data": {...}} so the rest of pipeline works
        return [{"data": item} for item in blob]

    raise ValueError("Unrecognized JSON structure: expected reddit Listing or list of posts.")

def save_cleaned_posts(posts, path):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(posts, f, ensure_ascii=False, indent=2)
    logging.info(f"Saved {len(posts)} canonical posts to {path}")
    print(f"✅ Saved {len(posts)} canonical posts to {path}")

def main():
    print("📦 Loading raw reddit posts...")
    raw_children = load_raw_posts(RAW_FILE)

    print("🧹 Converting to CANONICAL format...")
    canonical = []
    seen = set()

    for child in raw_children:
        c = reddit_to_canonical(child)
        if not c:
            continue
        key = ("reddit", c["source_id"])
        if key in seen:
            logging.info(f"Duplicate reddit id skipped: {key}")
            continue
        seen.add(key)
        canonical.append(c)

    print(f"✅ Valid canonical posts: {len(canonical)} / {len(raw_children)}")
    save_cleaned_posts(canonical, CLEANED_FILE)
    print("🎉 Done.")
    
if __name__ == "__main__":
    main()
