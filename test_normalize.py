import json
import os
from normalize import normalize_listing

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

zublet_path = os.path.join(BASE_DIR, "Huddle-Zublet", "zublet_final.json")
reddit_path = os.path.join(BASE_DIR, "reddit", "llm_posts.json")

with open(zublet_path) as f:
    zublet_data = json.load(f)

with open(reddit_path) as f:
    reddit_data = json.load(f)

normalized = []

for listing in zublet_data + reddit_data:
    normalized.append(normalize_listing(listing))

with open("normalized_output.json", "w") as f:
    json.dump(normalized, f, indent=2)

print("Done. Created normalized_output.json")