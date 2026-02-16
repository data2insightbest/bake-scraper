import os
import time
import json
import re
from bs4 import BeautifulSoup
from datetime import datetime, time as dt_time
from google import genai 
from supabase import create_client
from playwright.sync_api import sync_playwright

# --- Setup ---
supabase = create_client(os.environ['VITE_SUPABASE_URL'], os.environ['VITE_SUPABASE_KEY'])
client = genai.Client(api_key=os.environ['GEMINI_API_KEY'])

USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'

# --- Config ---
TEST_WORKSHOPS_ONLY = True  

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
    Today is {today_str}. Find upcoming kids events for {venue['name']}.
    Output a JSON list with: "title", "event_date" (YYYY-MM-DD), "category_name", "window_type", "price_text", "snippet", "found_location".
    Rules: Use 2026 for year. EXCLUDE if no date. EXCLUDE adults-only.
    """
    
    for attempt in range(3):
        try:
            response = client.models.generate_content(
                model='gemini-2.0-flash',
                contents=[prompt, cleaned_text[:18000]] 
            )
            res_text = response.text.strip().replace('```json', '').replace('```', '').strip()
            return json.loads(res_text)
        except Exception as e:
            time.sleep(20)
    return []

def run_scraper():
    midnight_today = datetime.combine(datetime.now().date(), dt_time.min).isoformat()
    print(f"🧹 Deleting events before {midnight_today}...")
    supabase.table("events").delete().lt("event_date", midnight_today).execute()
    
    target_cat = "Workshop and Hands on Experience"
    query = supabase.table("places").select("*").eq("is_master", True)
    if TEST_WORKSHOPS_ONLY:
        query = query.eq("category", target_cat)
    
    masters = query.execute().data
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent=USER_AGENT)
        
        for m in masters:
            # FIX 1: URL Validation
            raw_url = m['url']
            final_url = raw_url if raw_url.startswith('http') else f'https://{raw_url}'
            
            print(f"🔄 Processing: {m['name']} at {final_url}")
            page = context.new_page()
            
            # Tracking set to prevent double-adding in the same run
            processed_keys = set()
            
            try:
                page.goto(final_url, wait_until="networkidle", timeout=60000)
                time.sleep(10) 
                
                text = clean_html(page.content())
                events = get_ai_extraction(text, m)
                branches = supabase.table("places").select("*").eq("parent_id", m['id']).execute().data
                
                for ev in events:
                    if not is_valid_date(ev.get('event_date')):
                        continue
                    
                    loc = str(ev.get('found_location', '')).lower()
                    
                    # Logic for distribution
                    targets = []
                    if loc == 'all' or "workshop" in m['category'].lower():
                        targets = branches
                    else:
                        for b in branches:
                            keyword = b['name'].lower().replace("library", "").strip()
                            if keyword in loc or keyword in ev['title'].lower():
                                targets = [b]
                                break

                    for branch in targets:
                        # FIX 2: De-duplication Logic
                        unique_key = f"{ev['title']}-{ev['event_date']}-{branch['id']}"
                        if unique_key not in processed_keys:
                            entry = ev.copy()
                            entry.pop('found_location', None)
                            entry.update({
                                'place_id': int(branch['id']),
                                'place_name': branch['name'],
                                'zip_code': branch['zip_code']
                            })
                            supabase.table("events").insert(entry).execute()
                            processed_keys.add(unique_key)
                            print(f"   ✨ Added: {ev['title']} to {branch['name']}")

            except Exception as e:
                print(f"❌ Error at {m['name']}: {e}")
            finally:
                page.close()
        browser.close()

if __name__ == "__main__":
    run_scraper()
