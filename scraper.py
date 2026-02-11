import os
import time
import json
import re  # Added for date validation
from bs4 import BeautifulSoup
from datetime import datetime
from google import genai 
from supabase import create_client
from playwright.sync_api import sync_playwright

# --- Setup ---
supabase = create_client(os.environ['VITE_SUPABASE_URL'], os.environ['VITE_SUPABASE_KEY'])
client = genai.Client(api_key=os.environ['GEMINI_API_KEY'])

USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'

def clean_html(raw_html):
    soup = BeautifulSoup(raw_html, 'html.parser')
    for element in soup(["script", "style", "footer", "nav", "header", "aside", "svg"]):
        element.decompose()
    return soup.get_text(separator=' ', strip=True)

# --- NEW: Date Validator ---
def is_valid_date(date_str):
    """Returns True if string is exactly YYYY-MM-DD, False otherwise."""
    return bool(re.match(r'^\d{4}-\d{2}-\d{2}$', str(date_str)))

def get_ai_extraction(cleaned_text, venue):
    if not cleaned_text or len(cleaned_text.strip()) < 200:
        return []

    prompt = f"""
    Find upcoming kids events for {venue['name']} (Zip: {venue['zip_code']}).
    Output a JSON list with:
    - "title": Event name
    - "event_date": YYYY-MM-DD (If date is not found, exclude the event)
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
            clean_json = res_text.replace('```json', '').replace('```', '').strip()
            return json.loads(clean_json)
        except Exception as e:
            if "429" in str(e):
                time.sleep((attempt + 1) * 20)
            else:
                return []
    return []

def run_scraper():
    today = datetime.now().strftime('%Y-%m-%d')
    print(f"🧹 Deleting events before {today}...")
    supabase.table("events").delete().lt("event_date", today).execute()
    
    places = supabase.table("places").select("*").execute().data[:10]
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent=USER_AGENT)
        
        for venue in places:
            print(f"🔄 Scraping {venue['name']}...")
            page = context.new_page()
            try:
                page.goto(venue['url'], wait_until="domcontentloaded", timeout=45000)
                time.sleep(5)
                
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                time.sleep(3) 
                
                html_content = page.content()
                text = clean_html(html_content)
                
                events = get_ai_extraction(text, venue)
                
                for event in events:
                    # --- VALIDATION CHECK ---
                    # Only insert if the date is in the correct format
                    if is_valid_date(event.get('event_date')):
                        event['place_id'] = int(venue['id'])
                        event['place_name'] = venue['name'] 
                        
                        supabase.table("events").insert(event).execute()
                        print(f"   ✨ Added: {event['title']} at {venue['name']}")
                    else:
                        # This skips the "Not specified" errors silently
                        print(f"   ⏩ Skipped '{event.get('title')}' - invalid date format: {event.get('event_date')}")
                
                print(f"✅ Finished {venue['name']}.")
                
            except Exception as e:
                print(f"❌ Failed {venue['name']}: {e}")
            finally:
                page.close()
                time.sleep(10)

        browser.close()

if __name__ == "__main__":
    run_scraper()
    
