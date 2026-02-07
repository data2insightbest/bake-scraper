import os
import json
import time
import requests
import urllib.parse
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from google import genai
from supabase import create_client

# 1. Setup
supabase = create_client(os.environ["VITE_SUPABASE_URL"], os.environ["VITE_SUPABASE_KEY"])
client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Referer': 'https://www.google.com/'
}

def cleanup_old_events():
    """Removes events that happened before yesterday."""
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    print(f"🧹 Maintenance: Deleting events before {yesterday}...")
    try:
        supabase.table("events").delete().lt("event_date", yesterday).execute()
        print("✅ Cleanup complete.")
    except Exception as e:
        print(f"⚠️ Cleanup error: {e}")

def run_bake_scraper():
    # Fetch museums
    places_res = supabase.table("places").select("*").execute()
    #places = places_res.data
    places = places_res.data[:10]
    print(f"🚀 BAKE Scraper 2.0: Extracting Full Event Data...")

    for i, place in enumerate(places):
        name = place['name']
        url = place.get('url')
        # Use existing zip or default if it's a generic search
        place_zip = place.get('zip_code') or "94118" 

        if not url:
            search_query = f"{name} kids events {place_zip}"
            url = f"https://www.google.com/search?q={urllib.parse.quote(search_query)}"
            print(f"🔎 Generic: {name} in {place_zip}")

        try:
            res = requests.get(url, headers=HEADERS, timeout=15)
            if res.status_code != 200: continue

            text_content = BeautifulSoup(res.text, 'html.parser').get_text()[:7500]

            # Detailed Prompt for Full Data Extraction
            prompt = f"""
            Find all kids events for {name} in zip code {place_zip}.
            Return a JSON list where each object has:
            - "title": Title of the event
            - "event_date": YYYY-MM-DD format
            - "price_text": Exact price (e.g. "$15", "Free", or "Varies")
            - "category_name": One of [Science, Art, Outdoor, Play, Animals]
            - "window_type": One of [Daily, Weekly, Special]
            - "zip_code": "{place_zip}"
            - "snippet": Short 1-sentence description
            """
            
            ai_res = client.models.generate_content(
                model='gemini-2.0-flash-lite',
                contents=f"{prompt}\n\nContent:\n{text_content}"
            )
            
            # Use JSON parsing with error handling
            try:
                events_list = json.loads(ai_res.text.strip('`json\n '))
            except:
                print(f"⚠️ Failed to parse AI JSON for {name}")
                continue

            if events_list:
                for event in events_list:
                    event['place_id'] = place['id']
                    # Bulk insert is more efficient
                    supabase.table("events").insert(event).execute()
                print(f"✅ [{i+1}] {name}: Added {len(events_list)} full events.")

            time.sleep(10) # Protect your API quota

        except Exception as e:
            print(f"❌ Error at {name}: {e}")

    cleanup_old_events()
    print("🏁 All done!")

if __name__ == "__main__":
    run_bake_scraper()
            
