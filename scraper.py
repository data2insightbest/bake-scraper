import os
import time
import json
from bs4 import BeautifulSoup
from datetime import datetime
from google import genai 
from supabase import create_client
from playwright.sync_api import sync_playwright

# --- Setup ---
supabase = create_client(os.environ['VITE_SUPABASE_URL'], os.environ['VITE_SUPABASE_KEY'])
client = genai.Client(api_key=os.environ['GEMINI_API_KEY'])

# Standard headers for the browser context
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'

def clean_html(raw_html):
    """Strips HTML noise to focus the AI."""
    soup = BeautifulSoup(raw_html, 'html.parser')
    for element in soup(["script", "style", "footer", "nav", "header", "aside", "svg"]):
        element.decompose()
    return soup.get_text(separator=' ', strip=True)

def get_ai_extraction(cleaned_text, venue):
    """Uses Gemini 2.5 Flash-Lite."""
    if not cleaned_text or len(cleaned_text.strip()) < 200:
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
            clean_json = res_text.replace('```json', '').replace('```', '').strip()
            return json.loads(clean_json)
        except Exception as e:
            if "429" in str(e):
                time.sleep((attempt + 1) * 20)
            else:
                return []
    return []

def run_scraper():
    # 1. Database Cleanup
    today = datetime.now().strftime('%Y-%m-%d')
    print(f"🧹 Deleting events before {today}...")
    supabase.table("events").delete().lt("event_date", today).execute()
    
    # 2. Get Places
    places = supabase.table("places").select("*").execute().data[:10]
    
    # 3. Launch Playwright
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent=USER_AGENT)
        
        for venue in places:
            print(f"🔄 Scraping {venue['name']}...")
            page = context.new_page()
            try:
                # Optimized for speed and to avoid timeouts
                page.goto(venue['url'], wait_until="domcontentloaded", timeout=45000)
                time.sleep(5)
                
                # Scroll to reveal lazy-loaded content
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                time.sleep(3) 
                
                html_content = page.content()
                text = clean_html(html_content)
                
                events = get_ai_extraction(text, venue)
                
                for event in events:
                    # NEW: Attaching both ID and Name for better app performance
                    event['place_id'] = int(venue['id'])
                    event['place_name'] = venue['name']
                    
                    supabase.table("events").insert(event).execute()
                    print(f"   ✨ Added: {event['title']} at {venue['name']}")
                
                print(f"✅ Finished {venue['name']}.")
                
            except Exception as e:
                print(f"❌ Failed {venue['name']}: {e}")
            finally:
                page.close()
                time.sleep(10) # Gemini RPM safety buffer

        browser.close()

if __name__ == "__main__":
    run_scraper()
    
