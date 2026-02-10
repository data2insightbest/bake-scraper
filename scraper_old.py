import os
import json
import time
import requests
import urllib.parse
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from google import genai
from supabase import create_client

# 1. Setup Connections
supabase = create_client(os.environ["VITE_SUPABASE_URL"], os.environ["VITE_SUPABASE_KEY"])
client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Referer': 'https://www.google.com/',
}

def cleanup_old_events():
    """Wipes out any records older than yesterday to keep Supabase clean."""
    cutoff_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    print(f"🧹 Database Maintenance: Deleting events before {cutoff_date}...")
    try:
        supabase.table("events").delete().lt("event_date", cutoff_date).execute()
        print(f"✅ Cleanup successful.")
    except Exception as e:
        print(f"⚠️ Cleanup skipped: {e}")

def run_bake_scraper():
    # Fetch museums/places
    places_res = supabase.table("places").select("*").execute()
    places = places_res.data[:10] 
    
    print(f"🚀 BAKE Scraper 2.0: Extracting Full Event Data...")

    for i, place in enumerate(places):
        name = place['name']
        url = place.get('url')
        zip_code = place.get('zip_code', '94118')
        
        # If no URL, we search for the place + zip code
        if not url:
            search_query = f"{name} kids events {zip_code}"
            url = f"https://www.google.com/search?q={urllib.parse.quote(search_query)}"
            print(f"🔎 Generic: {name} in {zip_code}")

        try:
            response = requests.get(url, headers=HEADERS, timeout=15)
            if response.status_code != 200:
                print(f"    ⏩ Skipping {name}: Status {response.status_code}")
                continue

            soup = BeautifulSoup(response.text, 'html.parser')
            text_content = soup.get_text()[:7500] 

            prompt = f"""
            Extract kids events for {name} in {zip_code}.
            Return ONLY a JSON list of objects.
            Include:
            - "title": Name of event
            - "event_date": YYYY-MM-DD
            - "category_name": One of [Science, Art, Outdoor, Play, Animals]
            - "window_type": 'Daily', 'Weekly', or 'Special'
            - "price_text": Extract real price (e.g. "$12", "Free")
            - "snippet": 1 sentence description
            - "zip_code": "{zip_code}"
            """
            
            ai_res = client.models.generate_content(
                model='gemini-2.0-flash-lite',
                contents=f"{prompt}\n\nText:\n{text_content}"
            )
            
            # Clean AI response
            raw_json = ai_res.text.strip('`json\n ')
            events_list = json.loads(raw_json)

            if isinstance(events_list, list):
                saved_count = 0
                today = datetime.now().date()
                
                for event in events_list:
                    try:
                        event_dt = datetime.strptime(event['event_date'], '%Y-%m-%d').date()
                        
                        # Filtering Logic (14/45/90 day windows)
                        is_valid = False
                        w_type = event.get('window_type', 'Daily')
                        if w_type == 'Daily' and event_dt <= (today + timedelta(days=14)):
                            is_valid = True
                        elif w_type == 'Weekly' and event_dt <= (today + timedelta(days=45)):
                            is_valid = True
                        elif w_type == 'Special' and event_dt <= (today + timedelta(days=90)):
                            is_valid = True

                        if is_valid:
                            event['place_id'] = place['id']
                            supabase.table("events").insert(event).execute()
                            saved_count += 1
                    except Exception as inner_e:
                        continue
                
                print(f"✅ [{i+1}/{len(places)}] {name}: Saved {saved_count} events.")
            
            time.sleep(10) # Safety buffer

        except Exception as e:
            print(f"❌ Error at {name}: {e}")

    cleanup_old_events()
    print("🏁 Hybrid Scrape Cycle Complete!")

if __name__ == "__main__":
    run_bake_scraper()
            
