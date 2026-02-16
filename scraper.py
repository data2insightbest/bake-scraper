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
    Output a JSON list with: "title", "event_date", "category_name", "window_type", "price_text", "snippet", "found_location".
    Rules: Use 2026 for year. EXCLUDE if no date.
    """
    
    for attempt in range(3):
        try:
            response = client.models.generate_content(
                model='gemini-2.0-flash',
                contents=[prompt, cleaned_text[:18000]] 
            )
            res_text = response.text.strip().replace('```json', '').replace('```', '').strip()
            return json.loads(res_text)
        except Exception:
            time.sleep(20)
    return []

def run_scraper():
    midnight_today = datetime.combine(datetime.now().date(), dt_time.min).isoformat()
    
    # 1. Global Cleanup (Deletes old history)
    print(f"🧹 Clearing history before {midnight_today}...")
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
            # URL Check
            final_url = m['url'] if m['url'].startswith('http') else f'https://{m['url']}'
            print(f"🔄 Processing Master: {m['name']}...")
            
            # Fetch branches early for cleanup
            branches = supabase.table("places").select("*").eq("parent_id", m['id']).execute().data
            branch_ids = [int(b['id']) for b in branches]

            # 2. TARGETED CLEANUP: Wipe today/future events for THESE branches before re-adding
            if branch_ids:
                print(f"   🗑️ Refreshing data for {len(branch_ids)} branches...")
                supabase.table("events").delete().in_("place_id", branch_ids).gte("event_date", midnight_today).execute()

            page = context.new_page()
            try:
                page.goto(final_url, wait_until="networkidle", timeout=60000)
                time.sleep(10) 
                
                text = clean_html(page.content())
                events = get_ai_extraction(text, m)
                
                # Internal de-duplication for the current page scrape
                seen_this_loop = set()

                for ev in events:
                    if not is_valid_date(ev.get('event_date')):
                        continue

                    loc_hint = str(ev.get('found_location', '')).lower()
                    targets = []
                    
                    # Workshop logic vs Library logic
                    if loc_hint == 'all' or "workshop" in m['category'].lower():
                        targets = branches
                    else:
                        for b in branches:
                            b_clean = b['name'].lower().replace("library", "").strip()
                            if b_clean in loc_hint or b_clean in ev['title'].lower():
                                targets = [b]
                                break
                    
                    for branch in targets:
                        unique_key = f"{ev['title']}-{ev['event_date']}-{branch['id']}"
                        if unique_key not in seen_this_loop:
                            entry = ev.copy()
                            entry.pop('found_location', None)
                            entry.update({
                                'place_id': int(branch['id']),
                                'place_name': branch['name'],
                                'zip_code': branch['zip_code']
                            })
                            supabase.table("events").insert(entry).execute()
                            seen_this_loop.add(unique_key)
                            print(f"   ✨ Added: {ev['title']} to {branch['name']}")
                
            except Exception as e:
                print(f"❌ Error at {m['name']}: {e}")
            finally:
                page.close()
        browser.close()

if __name__ == "__main__":
    run_scraper()
    
