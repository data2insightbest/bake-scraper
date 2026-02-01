import os
import requests
from bs4 import BeautifulSoup
from supabase import create_client
from datetime import datetime, timedelta

# 1. Setup Supabase
URL = os.environ.get("SUPABASE_URL")
KEY = os.environ.get("SUPABASE_KEY")
supabase = create_client(URL, KEY)

def get_search_window():
    """Determines how many days ahead to look based on the Hybrid Window logic."""
    today = datetime.now()
    
    # 3. Special Scout: 1st of the Month (90 Days)
    if today.day == 1:
        print("🔍 MODE: Special Scout (90-day window)")
        return 90
    
    # 2. Weekly Deep Dive: Monday (45 Days)
    if today.weekday() == 0: # 0 is Monday
        print("🔍 MODE: Weekly Deep Dive (45-day window)")
        return 45
    
    # 1. Daily Refresh: Standard (14 Days)
    print("🔍 MODE: Daily Refresh (14-day window)")
    return 14

def run_bake_scraper():
    # Fetch your 100 places
    places = supabase.table("places").select("*").execute().data
    if not places:
        print("❌ No places found in Supabase.")
        return

    days_ahead = get_search_window()
    horizon_date = (datetime.now() + timedelta(days=days_ahead)).date()

    print(f"🚀 Starting BAKE Scraper for {len(places)} locations.")
    print(f"📅 Looking for events between now and {horizon_date}")

    for place in places:
        try:
            # Note: Using your specific column names 'name' and 'url'
            name = place.get('name')
            target_url = place.get('url')
            zip_code = place.get('zip_code')
            cat_id = place.get('category_id')

            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
            res = requests.get(target_url, headers=headers, timeout=20)
            soup = BeautifulSoup(res.text, 'html.parser')
            
            # Keywords to identify family-friendly activities
            keywords = ["workshop", "festival", "holiday", "storytime", "special", "free day", "camp", "exhibit"]
            page_text = soup.get_text().lower()
            
            found_keywords = [word for word in keywords if word in page_text]

            if found_keywords:
                # Create the event record
                # Note: We 'upsert' so if the event already exists, it just updates
                event_data = {
                    "place_id": place['id'],
                    "title": f"Special Activity at {name}",
                    "description": f"Found mentions of: {', '.join(found_keywords)}",
                    "event_date": datetime.now().strftime("%Y-%m-%d"), # Ideally, we'd extract actual dates here
                    "zip_code": zip_code,
                    "category_id": cat_id,
                    "event_url": target_url
                }
                
                # Using upsert to prevent duplicates
                supabase.table("events").upsert(event_data).execute()
                print(f"✅ Saved event for {name}")
            else:
                print(f"   - No new events found for {name}")

        except Exception as e:
            print(f"⚠️ Error at {place.get('name', 'Unknown')}: {e}")

if __name__ == "__main__":
    run_bake_scraper()
