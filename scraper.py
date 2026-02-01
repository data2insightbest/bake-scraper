import os
import requests
from bs4 import BeautifulSoup
from supabase import create_client
from datetime import datetime, timedelta

# 1. Setup
URL = os.environ.get("SUPABASE_URL")
KEY = os.environ.get("SUPABASE_KEY")
supabase = create_client(URL, KEY)

def calculate_specificity(text):
    score = 0
    text = text.lower()
    
    # Tier 1: Core Keywords (+10 each)
    keywords = ["workshop", "festival", "holiday", "storytime", "camp", "exhibit", "performance"]
    found_keywords = [w for w in keywords if w in text]
    score += (len(set(found_keywords)) * 10)
    
    # Tier 2: Urgency/Rarity Boosters (+20 each)
    # These words usually indicate a unique event rather than general admission
    boosters = ["one-day only", "limited spots", "registration required", "annual", "special guest", "grand opening"]
    for word in boosters:
        if word in text:
            score += 20
            
    return min(score, 100), found_keywords

def run_bake_scraper():
    places_res = supabase.table("places").select("id, name, url, zip_code, category").execute()
    places = places_res.data
    
    for place in places:
        try:
            res = requests.get(place['url'], timeout=20, headers={'User-Agent': 'Mozilla/5.0'})
            if res.status_code != 200: continue

            soup = BeautifulSoup(res.text, 'html.parser')
            page_text = soup.get_text()
            
            # Calculate Score
            score, found = calculate_specificity(page_text)

            # Only save if it's actually an event (Score > 0)
            if score > 0:
                event_data = {
                    "place_id": place['id'],
                    "title": f"Activity at {place['name']}",
                    "description": f"Found: {', '.join(found[:3])}",
                    "specificity_score": score, # NEW: Helps Replit rank events
                    "event_date": datetime.now().strftime("%Y-%m-%d"),
                    "zip_code": place.get('zip_code'),
                    "category_name": place.get('category'),
                    "event_url": place['url']
                }
                supabase.table("events").upsert(event_data).execute()
                print(f"✅ {place['name']} - Score: {score}")

        except Exception as e:
            print(f"❌ Error at {place['name']}: {e}")

if __name__ == "__main__":
    run_bake_scraper()
    
