import os
import requests
from bs4 import BeautifulSoup
from supabase import create_client
from datetime import datetime, timedelta

# Connect to Supabase
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase = create_client(url, key)

def scrape_bake():
    # 30-Day Window
    today = datetime.now().date()
    future_limit = today + timedelta(days=30)
    
    # List of your 100 Curated Places (Example shown)
    places = [
        {"name": "The Tech Interactive", "url": "https://www.thetech.org/events", "zip": "95113"},
        {"name": "Palo Alto Junior Museum", "url": "https://www.paloaltozoo.org", "zip": "94303"}
    ]

    for place in places:
        # 1. Fetch the website
        res = requests.get(place['url'])
        soup = BeautifulSoup(res.text, 'html.parser')
        
        # 2. Logic to find "Special" keywords
        keywords = ["Workshop", "Holiday", "Festival", "Special", "Free Day"]
        page_text = soup.get_text()
        
        if any(word in page_text for word in keywords):
            # 3. Save to Supabase
            data = {
                "title": f"Special Activity at {place['name']}",
                "event_date": today.isoformat(),
                "zip_code": place['zip'],
                "description": "New activity discovered by BAKE scraper."
            }
            supabase.table("events").upsert(data).execute()

if __name__ == "__main__":
    scrape_bake()
