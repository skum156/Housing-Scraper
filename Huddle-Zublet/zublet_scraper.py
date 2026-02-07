from playwright.sync_api import sync_playwright
import json
import time

def scrape_zublet():
    listings = []
    seen = set()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        # Go to Zublet homepage
        page.goto("https://zublet.com", timeout=60000)

        # Wait for listings to load
        page.wait_for_timeout(5000)

        # Scroll to ensure all cards load
        for _ in range(5):
            page.mouse.wheel(0, 2000)
            time.sleep(1)

        # Grab all potential listing cards
        cards = page.locator("a, div").all()

        for card in cards:
            try:
                text = card.inner_text().strip()

                # Skip non-listing blocks
                if "$" not in text:
                    continue

                lines = [l.strip() for l in text.split("\n") if l.strip()]

                if len(lines) < 2:
                    continue

                # Basic parsing
                price = None
                title = None
                location = None

                for i, line in enumerate(lines):
                    if "$" in line:
                        price = line
                        if i >= 1:
                            location = lines[i-1]
                        if i >= 2:
                            title = lines[i-2]
                        break

                if not price or not title:
                    continue

                key = (title, location, price)

                if key in seen:
                    continue

                seen.add(key)

                # Try to get link if exists
                url = ""
                try:
                    url = card.get_attribute("href") or ""
                except:
                    pass

                listings.append({
                    "title": title,
                    "location": location or "",
                    "price": price,
                    "url": url,
                    "source": "zublet"
                })

            except:
                continue

        browser.close()

    return listings


# Run scraper
data = scrape_zublet()

with open("zublet_data.json", "w") as f:
    json.dump(data, f, indent=2)

print(f"Saved {len(data)} listings")
