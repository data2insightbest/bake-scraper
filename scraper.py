import os
import time
import json
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from google import genai  # Newer Unified SDK
from supabase import create_client

# --- Setup ---
supabase = create_client(os.environ['VITE_SUPABASE_URL'], os.environ['VITE_SUPABASE_KEY'])
client = genai.Client(api_key=os.environ['GEMINI_API_KEY'])

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Referer': 'https://www.google.com/'
}

def clean_html(raw_html):
    """Strips HTML junk to reduce token usage and noise."""
    soup = BeautifulSoup(raw_html, 'html.parser')
    for element in soup(["script", "style", "footer", "nav", "header", "aside"]):
        element.decompose()
    return soup.get_text(separator=' ', strip=True)

def get_ai_extraction(cleaned_text, venue):
    """Uses new google-genai client with exponential backoff."""
    prompt = f"""
    Find upcoming kids events for {venue['name']} (Zip: {venue['zip_code']}).
    Output a JSON list with these keys:
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
            # New SDK syntax: client.models.generate_content
            response = client.models.generate_content(
                model='gemini-1.5-flash',
                contents=[prompt, cleaned_text[:15000]]
            )
            
            # Extract text and clean potential markdown
            raw_text = response.text
            clean_json = raw_text.replace('```json', '').replace('```', '').strip()
            return json.loads(clean_json)
            
        except Exception as e:
            if "429" in str(e):
                delay = (attempt + 1) * 20 
                print(f"⚠️ Rate limit hit. Backing off for {delay}s...")
                time.sleep(delay)
            else:
                print(f"❌ AI Error: {e}")
                return []
    return []

def run_scraper():
    # 1. Clean old data
    today = datetime.now().strftime('%Y-%m-%d')
    supabase.table("events").delete().lt("event_date", today).execute()
    
    # 2. Process venues
    places_res = supabase.table("places").select("*").execute()
    places = places_res.data[:10]
    
    for venue in places:
        print(f"🔄 Scraping {venue['name']}...")
        try:
            time.sleep(5) # Respect website
            res = requests.get(venue['url'], headers=HEADERS, timeout=15)
            
            if res.status_code == 200:
                text = clean_html(res.text)
                events = get_ai_extraction(text, venue)
                
                for event in events:
                    event['place_id'] = venue['id'] # Link to place
                    supabase.table("events").insert(event).execute()
                    print(f"   ✨ Added: {event['title']}")
                
                print(f"✅ Finished {venue['name']}.")
            else:
                print(f"⏩ Skip {venue['name']}: HTTP {res.status_code}")
            
            # 12s pause keeps you under 5 requests per minute
            time.sleep(12) 
            
        except Exception as e:
            print(f"❌ Failed {venue['name']}: {e}")

if __name__ == "__main__":
    run_scraper()
    print("🏁 All venues processed.")
    
