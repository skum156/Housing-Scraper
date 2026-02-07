
import json

def extract_images(post_data):
    images = []
    media_metadata = post_data.get("media_metadata")
    gallery_data = post_data.get("gallery_data")

    if gallery_data and media_metadata:
        for item in gallery_data["items"]:
            media_id = item["media_id"]
            if media_id in media_metadata:
                url = media_metadata[media_id]["s"]["u"]
                images.append(url.replace("&amp;", "&"))
    elif post_data.get("url") and post_data["url"].endswith((".jpg", ".png")):
        images.append(post_data["url"])
    return images

with open("purdue_housing.json", "r", encoding="utf-8") as f:
    raw_data = json.load(f)

cleaned_posts = []

for listing in raw_data:
    for child in listing["data"]["children"]:
        d = child["data"]

        post = {
            "id": d.get("id"),
            "title": d.get("title"),
            "description": d.get("selftext"),
            "author": d.get("author"),
            "created_utc": d.get("created_utc"),
            "permalink": f"https://www.reddit.com{d.get('permalink')}",
            "score": d.get("score"),
            "images": extract_images(d)
        }

        cleaned_posts.append(post)

with open("purdue_housing.json", "w", encoding="utf-8") as f:
    json.dump(cleaned_posts, f, ensure_ascii=False, indent=2)
