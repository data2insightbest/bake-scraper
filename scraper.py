import os
import time
import json
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from google import genai 
from supabase import create_client

# --- Setup ---
supabase = create_client(os.environ['VITE_SUPABASE_URL'], os.environ['VITE_SUPABASE_KEY'])

client = genai.Client(api_key=os.environ['GEMINI_API_KEY'])

# Expanded headers to bypass 403/405 blocks
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referer': 'https://www.google.com/',
    'DNT': '1',
}

def clean_html(raw_html):
    """Strips HTML noise to save tokens and focus the AI."""
    soup = BeautifulSoup(raw_html, 'html.parser')
    for element in soup(["script", "style", "footer", "nav", "header", "aside"]):
        element.decompose()
    return soup.get_text(separator=' ', strip=True)

def get_ai_extraction(cleaned_text, venue):
    """Uses Gemini 2.5 Flash-Lite (the current 2026 standard)."""
    # Safety check for empty text (prevents AI Error: char 0)
    if not cleaned_text or len(cleaned_text.strip()) < 100:
        return []

    prompt = f"""
    Find upcoming kids events for {venue['name']} (Zip: {venue['zip_code']}).
    Output a JSON list with:
    - "title": Event name
    - "event_date": YYYY-MM-DD
    - "category_name": [Science, Art, Outdoor, Play, Animals]
    - "window_type": ['Daily', 'Weekly', 'Special']
    - "price_text": e.g. "$15" or "Free"
    - "snippet": 1 sentence summary
    - "zip_code": "{venue['zip_code']}"
    """
    
    for attempt in range(3):
        try:
            response = client.models.generate_content(
                model='gemini-2.5-flash-lite',
                contents=[prompt, cleaned_text[:18000]] 
            )
            
            res_text = response.text.strip()
            # Handle markdown wrapping
            clean_json = res_text.replace('```json', '').replace('```', '').strip()
            return json.loads(clean_json)
            
        except Exception as e:
            if "429" in str(e):
                delay = (attempt + 1) * 20 
                print(f"⚠️ Rate limit. Backing off {delay}s...")
                time.sleep(delay)
            else:
                print(f"❌ AI Error: {e}")
                return []
    return []

def run_scraper():
    # Start a session to handle cookies/persist identity (Fixes 403)
    session = requests.Session()
    session.headers.update(HEADERS)

    today = datetime.now().strftime('%Y-%m-%d')
    print(f"🧹 Deleting events before {today}...")
    supabase.table("events").delete().lt("event_date", today).execute()
    
    places = supabase.table("places").select("*").execute().data[:10]
    
    for venue in places:
        print(f"🔄 Scraping {venue['name']}...")
        try:
            # Slower delay to appear more human
            time.sleep(8) 
            res = session.get(venue['url'], timeout=20)
            
            if res.status_code == 200:
                text = clean_html(res.text)
                events = get_ai_extraction(text, venue)
                
                for event in events:
                    # Note: Ensure events table place_id column is 'int8' not 'uuid'
                    event['place_id'] = int(venue['id']) 
                    supabase.table("events").insert(event).execute()
                    print(f"   ✨ Added: {event['title']}")
                print(f"✅ Finished {venue['name']}.")
            else:
                print(f"⏩ Skip {venue['name']}: HTTP {res.status_code}")
            
            time.sleep(10) 
            
        except Exception as e:
            print(f"❌ Failed {venue['name']}: {e}")

if __name__ == "__main__":
    run_scraper()
    
