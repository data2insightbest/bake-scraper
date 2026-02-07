import os
import json
import time
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from google import genai  # UPDATED: New 2026 SDK
from supabase import create_client

# 1. Setup Connections
supabase = create_client(os.environ["VITE_SUPABASE_URL"], os.environ["VITE_SUPABASE_KEY"])
# New Client structure for 2026
client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
}

def cleanup_old_events():
    """Wipes out any records older than 2 days to keep Supabase under 500MB."""
    cutoff_date = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
    print(f"🧹 Database Maintenance: Deleting events before {cutoff_date}...")
    try:
        supabase.table("events").delete().lt("event_date", cutoff_date).execute()
        print(f"✅ Cleanup successful.")
    except Exception as e:
        print(f"⚠️ Cleanup skipped: {e}")

def run_bake_scraper():
    # Fetch museums
    places_res = supabase.table("places").select("*").execute()
    #places = places_res.data
    places = places_res.data[:10] 
    
    print(f"🚀 BAKE AI Scraper Started. Mode: Hybrid Window (14/45/90 Days).")

    for i, place in enumerate(places):
        name = place['name']
        url = place['url']
        zip_code = place.get('zip_code', '00000')
        
        try:
            # Step 1: Fetch Website
            response = requests.get(url, headers=HEADERS, timeout=15)
            if response.status_code != 200:
                print(f"    ⏩ Skipping {name}: Status {response.status_code}")
                continue

            # Step 2: Extract Text
            soup = BeautifulSoup(response.text, 'html.parser')
            text_content = soup.get_text()[:7000] # Slightly more context for better price extraction

            # Step 3: AI Hybrid Window Processing (UPDATED for Gemini 2.0)
            prompt = f"""
            Extract a list of upcoming kids events from this text for {name} in {zip_code}.
            Return ONLY a JSON list of objects.
            
            Each object MUST include:
            - "title": Name of event
            - "event_date": YYYY-MM-DD
            - "category_name": One of [Science, Art, Outdoor, Play, Animals]
            - "window_type": Categorize as 'Daily' (recurring/small), 'Weekly' (mid-size), or 'Special' (festivals/large exhibits)
            - "price_text": Extract the ACTUAL real price (e.g. "$12", "Free", "$5 kids/$10 adults"). Do not guess.
            - "snippet": 1 sentence description
            - "zip_code": "{zip_code}"
            """
            
            # Using the 2026 client.models syntax
            ai_res = client.models.generate_content(
                model='gemini-2.0-flash-lite',
                contents=f"{prompt}\n\nText:\n{text_content}"
            )
            
            # Clean up the AI response to get pure JSON
            raw_json = ai_res.text.strip('`json\n ')
            events_list = json.loads(raw_json)

            # Step 4: Hybrid Filtering & Saving
            today = datetime.now().date()
            saved_count = 0

            if isinstance(events_list, list):
                for event in events_list:
                    # Validate date string format
                    try:
                        event_dt = datetime.strptime(event['event_date'], '%Y-%m-%d').date()
                    except:
                        continue
                    
                    # Window logic:
                    is_valid = False
                    if event['window_type'] == 'Daily' and event_dt <= (today + timedelta(days=14)):
                        is_valid = True
                    elif event['window_type'] == 'Weekly' and event_dt <= (today + timedelta(days=45)):
                        is_valid = True
                    elif event['window_type'] == 'Special' and event_dt <= (today + timedelta(days=90)):
                        is_valid = True

                    if is_valid:
                        event['place_id'] = place['id']
                        # Ensure the zip code is exactly what was requested
                        event['zip_code'] = zip_code
                        supabase.table("events").insert(event).execute()
                        saved_count += 1
                
                print(f"✅ [{i+1}/{len(places)}] {name}: Saved {saved_count} window-matched events.")
            else:
                print(f"ℹ️ [{i+1}/{len(places)}] {name}: No list returned from AI.")

            # RPM SAFETY
            time.sleep(10) 

        except Exception as e:
            if "429" in str(e):
                print("🛑 RPM LIMIT HIT: Stopping.")
                break
            print(f"❌ Error at {name}: {e}")

    # Final Step: Maintenance
    cleanup_old_events()
    print("🏁 Hybrid Scrape Cycle Complete!")

if __name__ == "__main__":
    run_bake_scraper()
            
