import os
import requests
from bs4 import BeautifulSoup
from supabase import create_client
from datetime import datetime, timedelta

# 1. Setup Supabase
# Ensure these match the Secret names in your GitHub Repository
URL = os.environ.get("SUPABASE_URL")
KEY = os.environ.get("SUPABASE_KEY")
supabase = create_client(URL, KEY)

def get_search_window():
    """Hybrid Window Logic: 14 days daily, 45 days Mondays, 90 days 1st of month."""
    today = datetime.now()
    if today.day == 1:
        return 90
    if today.weekday() == 0:  # Monday
        return 45
    return 14

def run_bake_scraper():
    # Fetch your 100 places - using the exact column names from your manual upload
    places_res = supabase.table("places").select("id, name, url, zip_code, category").execute()
    places = places_res.data
    
    if not places:
        print("❌ No places found in Supabase 'places' table.")
        return

    days_ahead = get_search_window()
    horizon_date = (datetime.now() + timedelta(days=days_ahead)).date()

    print(f"🚀 BAKE Scraper: Checking {len(places)} locations.")
    print(f"📅 Horizon: Looking for events until {horizon_date}")

    for place in places:
        name = place.get('name')
        target_url = place.get('url') # Matches your CSV column 'url'
        zip_code = place.get('zip_code')
        cat_name = place.get('category') # The text name like 'Science Museum'

        try:
            # Browser-like headers to prevent 'Access Denied' errors
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8'
            }
            
            res = requests.get(target_url, headers=headers, timeout=20)
            
            if res.status_code != 200:
                print(f"⚠️ {name} blocked us or is down (Status: {res.status_code})")
                continue

            soup = BeautifulSoup(res.text, 'html.parser')
            
            # Keywords to find family events
            keywords = ["workshop", "festival", "holiday", "storytime", "special", "free day", "camp", "exhibit", "kids", "family"]
            page_text = soup.get_text().lower()
            
            found_keywords = [word for word in keywords if word in page_text]

            if found_keywords:
                # Prepare data to match your 'events' table columns exactly
                event_data = {
                    "place_id": place['id'],
                    "title": f"Activity at {name}",
                    "description": f"Focus: {', '.join(found_keywords[:4])}",
                    "event_date": datetime.now().strftime("%Y-%m-%d"),
                    "zip_code": zip_code,
                    "category_name": cat_name, # Corrected column name
                    "event_url": target_url
                }
                
                # 'upsert' prevents duplicate entries for the same place on the same day
                supabase.table("events").upsert(event_data).execute()
                print(f"✅ Saved event for {name}")
            else:
                print(f"   - No keywords found for {name}")

        except Exception as e:
            print(f"❌ Error at {name}: {str(e)}")

if __name__ == "__main__":
    run_bake_scraper()
