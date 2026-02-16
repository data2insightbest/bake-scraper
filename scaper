import os
import time
import json
import re
from bs4 import BeautifulSoup
from datetime import datetime
from google import genai 
from supabase import create_client
from playwright.sync_api import sync_playwright

# --- Setup ---
supabase = create_client(os.environ['VITE_SUPABASE_URL'], os.environ['VITE_SUPABASE_KEY'])
client = genai.Client(api_key=os.environ['GEMINI_API_KEY'])

USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'

# --- Config ---
TEST_WORKSHOPS_ONLY = True  # Set to False to run the full library/store list

def clean_html(raw_html):
    soup = BeautifulSoup(raw_html, 'html.parser')
    for element in soup(["script", "style", "footer", "nav", "header", "aside", "svg"]):
        element.decompose()
    return soup.get_text(separator=' ', strip=True)

def is_valid_date(date_str):
    return bool(re.match(r'^\d{4}-\d{2}-\d{2}$', str(date_str)))

def get_ai_extraction(cleaned_text, venue):
    if not cleaned_text or len(cleaned_text.strip()) < 200:
        return []

    today_str = datetime.now().strftime('%B %d, %Y')

    prompt = f"""
    Today is {today_str}. 
    Find upcoming kids events from this text for {venue['name']}.
    
    Output a JSON list with:
    - "title": Event name
    - "event_date": YYYY-MM-DD (Use 2026 for the year)
    - "category_name": [Science, Art, Outdoor, Play, Animals]
    - "window_type": ['Daily', 'Weekly', 'Special']
    - "price_text": e.g. "$15" or "Free"
    - "snippet": 1 sentence summary
    - "found_location": The specific branch or city name mentioned for this event (e.g., 'Belmont', 'Pacific', 'Foster City'). If it's a general event for all locations, put 'All'.

    Rules:
    1. EXCLUDE events without a specific day/month.
    2. EXCLUDE "Adults Only" events.
    3. Ensure event_date is a valid YYYY-MM-DD.
    """
    
    for attempt in range(3):
        try:
            response = client.models.generate_content(
                model='gemini-2.0-flash',
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

def save_event_to_supabase(event_data, branch):
    """Formats and inserts a single event record."""
    entry = event_data.copy()
    # Remove the helper field before saving
    entry.pop('found_location', None)
    
    entry['place_id'] = int(branch['id'])
    entry['place_name'] = branch['name']
    entry['zip_code'] = branch['zip_code']
    
    supabase.table("events").insert(entry).execute()
    print(f"   ✨ Added: {entry['title']} to {branch['name']} ({branch['zip_code']})")

def run_scraper():
    today = datetime.now().strftime('%Y-%m-%d')
    print(f"🧹 Deleting events before {today}...")
    supabase.table("events").delete().lt("event_date", today).execute()
    
    # Query for Master locations
    query = supabase.table("places").select("*").eq("is_master", True)
    if TEST_WORKSHOPS_ONLY:
        print("🛠️ Test Mode: Workshop logic only...")
        query = query.ilike("name", "%workshop%")
    
    masters = query.execute().data
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent=USER_AGENT)
        
        for m in masters:
            print(f"🔄 Processing Master: {m['name']}...")
            page = context.new_page()
            try:
                page.goto(m['url'], wait_until="domcontentloaded", timeout=45000)
                time.sleep(5)
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                
                text = clean_html(page.content())
                events = get_ai_extraction(text, m)
                
                # Fetch all branches belonging to this master
                branches = supabase.table("places").select("*").eq("parent_id", m['id']).execute().data
                
                for ev in events:
                    if not is_valid_date(ev.get('event_date')):
                        continue

                    loc = str(ev.get('found_location', '')).lower()
                    
                    # LOGIC 1: Blanket Duplication (Workshops/Franchises)
                    if loc == 'all' or m['category'] == 'Workshop':
                        for b in branches:
                            save_event_to_supabase(ev, b)
                    
                    # LOGIC 2: Branch Matching (Libraries/Specific Stores)
                    else:
                        matched = False
                        for b in branches:
                            # Search for branch name (e.g., 'Belmont') in the AI's 'found_location'
                            branch_keyword = b['name'].lower().replace("library", "").strip()
                            if branch_keyword in loc or branch_keyword in ev['title'].lower():
                                save_event_to_supabase(ev, b)
                                matched = True
                                break
                        
                        if not matched:
                            print(f"   ⏩ Skipped: {ev['title']} (No matching branch found for '{loc}')")
                
            except Exception as e:
                print(f"❌ Error at {m['name']}: {e}")
            finally:
                page.close()
                time.sleep(10)

        browser.close()

if __name__ == "__main__":
    run_scraper()
