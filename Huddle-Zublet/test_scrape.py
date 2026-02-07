print("Script is running")

import requests
from bs4 import BeautifulSoup

url = "https://zublet.com"
headers = {"User-Agent": "Mozilla/5.0"}

response = requests.get(url, headers=headers)

print("Status code:", response.status_code)
print("HTML length:", len(response.text))

soup = BeautifulSoup(response.text, "html.parser")

if soup.title:
    print("Page title:", soup.title.text)
else:
    print("No title found")

