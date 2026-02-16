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

def clean_html(raw_html):
    soup = BeautifulSoup(raw_html, 'html.parser')
    for element in soup(["script", "style", "footer", "nav", "header", "aside", "svg"]):
        element.decompose()
    return soup.get_text(separator=' ', strip=True)

def is_valid_date(date_str):
    return bool(re.match(r'^\d{4}-\d{2}-\d{2}$', str(date_str)))

def get_ai_extraction(cleaned_text, venue_name):
    today_str = datetime.now().strftime('%B %d, %Y')
    prompt = f"""
    Today is {today_str}. Find upcoming kids events for {venue_name}.
    Output JSON list: "title", "event_date" (YYYY-MM-DD), "category_name", "window_type", "price_text", "snippet", "found_location".
    Rules: Use 2026. EXCLUDE no-date. For libraries, extract the branch name into 'found_location'.
    """
    try:
        response = client.models.generate_content(model='gemini-2.0-flash', contents=[prompt, cleaned_text[:18000]])
        res_text = response.text.strip().replace('```json', '').replace('```', '').strip()
        return json.loads(res_text)
    except: return []

def run_scraper():
    midnight_today = datetime.combine(datetime.now().date(), dt_time.min).isoformat()
    supabase.table("events").delete().lt("event_date", midnight_today).execute()
    
    masters = supabase.table("places").select("*").eq("is_master", True).execute().data
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent=USER_AGENT)
        
        for m in masters:
            branches = supabase.table("places").select("*").eq("parent_id", m['id']).execute().data
            if not branches: continue
            
            # 1. PATHWAY A: Global Distribution (Home Depot, Lowe's)
            # Scrape once, apply to all branches.
            if any(x in m['name'].lower() for x in ["home depot", "lowe's"]):
                scrape_and_save(context, m, branches, mode="global", midnight=midnight_today)

            # 2. PATHWAY B: Branch-Specific Interaction (Lego, B&N, Slime Kitchen)
            # Must visit the URL multiple times, inputting zip for each branch.
            elif any(x in m['name'].lower() for x in ["lego", "barnes", "slime"]):
                for branch in branches:
                    scrape_and_save(context, m, [branch], mode="specific", midnight=midnight_today, zip_code=branch['zip_code'])

            # 3. PATHWAY C: Library Mapping
            # Scrape master list once, use AI to map events to specific branches.
            elif "library" in m['name'].lower():
                scrape_and_save(context, m, branches, mode="mapping", midnight=midnight_today)

        browser.close()

def scrape_and_save(context, master, target_branches, mode, midnight, zip_code=None):
    page = context.new_page()
    url = master['url'] if master['url'].startswith('http') else f'https://{master["url"]}'
    
    try:
        page.goto(url, wait_until="networkidle", timeout=60000)
        
        # Interaction for Branch-Specific stores
        if mode == "specific" and zip_code:
            print(f"📍 Interaction: Setting location to {zip_code} for {master['name']}...")
            # Interaction logic (Selectors vary by site, using a general approach)
            page.get_by_placeholder(re.compile("zip|city", re.I)).first.fill(zip_code)
            page.keyboard.press("Enter")
            time.sleep(10) 

        text = clean_html(page.content())
        events = get_ai_extraction(text, master['name'])
        
        if events:
            # Delete today's previous data for these specific branches to avoid duplicates
            b_ids = [int(b['id']) for b in target_branches]
            supabase.table("events").delete().in_("place_id", b_ids).gte("event_date", midnight).execute()

            for ev in events:
                if not is_valid_date(ev.get('event_date')): continue
                
                # Logic for mapping which branch gets the event
                for branch in target_branches:
                    should_add = False
                    
                    if mode == "global" or mode == "specific":
                        should_add = True # Global adds to all, Specific already filtered by zip
                    elif mode == "mapping":
                        # Match AI's 'found_location' to branch name
                        loc_hint = str(ev.get('found_location', '')).lower()
                        b_clean = branch['name'].lower().replace("library", "").strip()
                        if b_clean and (b_clean in loc_hint or b_clean in ev['title'].lower()):
                            should_add = True
                    
                    if should_add:
                        entry = ev.copy()
                        entry.pop('found_location', None)
                        entry.update({'place_id': branch['id'], 'place_name': branch['name'], 'zip_code': branch['zip_code']})
                        supabase.table("events").insert(entry).execute()
                        print(f"   ✨ Added: {ev['title']} -> {branch['name']}")
    except Exception as e:
        print(f"❌ Error at {master['name']}: {e}")
    finally:
        page.close()

if __name__ == "__main__":
    run_scraper()
    
