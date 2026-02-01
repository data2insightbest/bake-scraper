import os
import requests
from bs4 import BeautifulSoup
from supabase import create_client
from datetime import datetime, timedelta

# 1. Setup Supabase
URL = os.environ.get("SUPABASE_URL")
KEY = os.environ.get("SUPABASE_KEY")
supabase = create_client(URL, KEY)

def run_bake_scraper():
    places = supabase.table("places").select("*").execute().data
    if not places:
        print("❌ No places found in Supabase.")
        return

    print(f"🚀 Starting BAKE Smart Scout for {len(places)} locations.")

    for place in places:
        name = place.get('name')
        target_url = place.get('url')
        zip_code = place.get('zip_code')
        cat_id = place.get('category_id')

        try:
            # "Human" Headers to bypass bot blockers
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept-Language': 'en-US,en;q=0.9',
            }
            
            res = requests.get(target_url, headers=headers, timeout=20)
            
            if res.status_code != 200:
                print(f"⚠️ {name} blocked us (Status: {res.status_code})")
                continue

            soup = BeautifulSoup(res.text, 'html.parser')
            
            # Expanded keyword list - removed case sensitivity
            keywords = [
                "workshop", "festival", "holiday", "storytime", "special", 
                "free day", "camp", "exhibit", "performance", "family", "kids"
            ]
            
            # Search in text AND specific HTML tags where titles usually live
            page_text = soup.get_text().lower()
            found_keywords = [word for word in keywords if word in page_text]

            if found_keywords:
                print(f"🎯 {name}: Found {len(found_keywords)} matches!")
                
                event_data = {
                    "place_id": place['id'],
                    "title": f"Activity at {name}",
                    "description": f"Keywords found: {', '.join(found_keywords[:3])}...",
                    "event_date": datetime.now().strftime("%Y-%m-%d"),
                    "zip_code": zip_code,
                    "category_id": cat_id,
                    "event_url": target_url
                }
                
                # Upsert uses 'place_id' and 'event_date' to avoid duplicates
                supabase.table("events").upsert(event_data).execute()
            else:
                # Debug: Print the first 100 characters to see what the scraper actually "sees"
                snippet = page_text[:100].replace('\n', ' ')
                print(f"   - {name}: No keywords in snippet: [{snippet}...]")

        except Exception as e:
            print(f"❌ Error at {name}: {e}")

if __name__ == "__main__":
    run_bake_scraper()
