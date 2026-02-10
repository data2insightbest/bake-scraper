import os
import time
import json
import requests
from datetime import datetime
import google.generativeai as genai
from supabase import create_client

# --- Setup ---
supabase = create_client(os.environ['VITE_SUPABASE_URL'], os.environ['VITE_SUPABASE_KEY'])
genai.configure(api_key=os.environ['GEMINI_API_KEY'])
model = genai.GenerativeModel('gemini-1.5-flash')

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Referer': 'https://www.google.com/',
    'Accept-Language': 'en-US,en;q=0.5'
}

def get_ai_extraction(html_content, venue):
    """Processes HTML with Gemini API using backoff for 429 errors."""
    prompt = f"""
    Extract upcoming kids events for {venue['name']} (Zip: {venue['zip_code']}).
    Return ONLY a valid JSON list of objects with these exact keys:
    - "title": Name of event
    - "event_date": YYYY-MM-DD
    - "category_name": One of [Science, Art, Outdoor, Play, Animals]
    - "window_type": One of ['Daily', 'Weekly', 'Special']
    - "price_text": Real price (e.g. "$12", "Free")
    - "snippet": 1 concise sentence description
    - "zip_code": "{venue['zip_code']}"
    """
    
    for attempt in range(3):
        try:
            # We slice the HTML to 30k chars to stay within context and avoid truncation errors
            response = model.generate_content([prompt, html_content[:30000]])
            # Clean JSON markdown if present
            clean_json = response.text.replace('```json', '').replace('```', '').strip()
            return json.loads(clean_json)
        except Exception as e:
            if "429" in str(e):
                delay = (attempt + 1) * 20 
                print(f"⚠️ 429 Limit hit. Sleeping {delay}s...")
                time.sleep(delay)
            else:
                print(f"❌ AI Error: {e}")
                return []
    return []

def cleanup_old_events():
    """Deletes events that happened before today."""
    today = datetime.now().strftime('%Y-%m-%d')
    print(f"🧹 Cleaning up events older than {today}...")
    supabase.table("events").delete().lt("event_date", today).execute()

def scrape_cycle():
    cleanup_old_events()
    
    # Get all places from your new table
    places = supabase.table("places").select("*").execute().data
    
    for venue in places:
        print(f"🔍 Scraping: {venue['name']}...")
        try:
            time.sleep(5) # Delay to respect the target website
            res = requests.get(venue['url'], headers=HEADERS, timeout=15)
            
            if res.status_code == 200:
                events = get_ai_extraction(res.text, venue)
                
                for event in events:
                    # Add the place_id to link back to the 'places' table
                    event['place_id'] = venue['id']
                    supabase.table("events").insert(event).execute()
                
                print(f"✅ Saved {len(events)} events for {venue['name']}.")
            else:
                print(f"⏩ Failed {venue['name']}: HTTP {res.status_code}")
                
            # Wait 10s between venues to avoid Gemini 429
            time.sleep(10)
            
        except Exception as e:
            print(f"❌ Error at {venue['name']}: {e}")

if __name__ == "__main__":
    scrape_cycle()
    print("🏁 Scrape Complete!")
    
