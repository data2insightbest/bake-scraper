import os
import time
import json
import re
import random
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, time as dt_time
from google import genai 
from supabase import create_client
from playwright.sync_api import sync_playwright

# --- Setup ---
supabase = create_client(os.environ['VITE_SUPABASE_URL'], os.environ['VITE_SUPABASE_KEY'])
client = genai.Client(api_key=os.environ['GEMINI_API_KEY'])

MOBILE_USER_AGENT = 'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1'

# --- Hybrid Step 1: The Project Bank ---
PROJECT_BANK = {
    "home depot": {
        "2026-02-07": "Kids Workshop: Penguin Mailbox",
        "2026-03-07": "Kids Workshop: Leprechaun Trap",
        "2026-04-04": "Kids Workshop: Farm Planter"
    },
    "lowe's": {
        "2026-02-21": "Lowe's Kids Club: Birdhouse",
        "2026-03-21": "Lowe's Kids Club: Lawn Mower",
        "2026-04-18": "Lowe's Kids Club: Terrarium"
    }
}

def get_hybrid_retail_events(venue_name):
    """Hybrid Logic: Checks Project Bank first, then calculates dates perpetually."""
    events = []
    today = datetime.now()
    clean_venue = venue_name.lower().replace("’", "'")
    name_key = "home depot" if "home depot" in clean_venue else "lowe's"
    for i in range(3):
        year = today.year + (today.month + i - 1) // 12
        month = (today.month + i - 1) % 12 + 1
        first_day = datetime(year, month, 1)
        days_to_sat = (5 - first_day.weekday() + 7) % 7
        target_date = first_day + timedelta(days=days_to_sat)
        if name_key == "lowe's":
            target_date = target_date + timedelta(weeks=2)
        date_str = target_date.strftime('%Y-%m-%d')
        if target_date.date() >= today.date():
            title = PROJECT_BANK.get(name_key, {}).get(date_str, f"{venue_name} Kids Workshop")
            events.append({
                "title": title, "event_date": date_str, "category_name": "Workshop",
                "window_type": "Morning", "price_text": "Free",
                "snippet": f"Free hands-on building event at {venue_name}. Materials provided."
            })
    return events

def clean_html(raw_html):
    soup = BeautifulSoup(raw_html, 'parser')
    for element in soup(["script", "style", "footer", "nav", "header", "aside", "svg"]):
        element.decompose()
    return soup.get_text(separator=' ', strip=True)

def is_valid_date(date_str):
    return bool(re.match(r'^\d{4}-\d{2}-\d{2}$', str(date_str)))

def save_events(events, target_branches, midnight, master_name, mode):
    b_ids = [int(b['id']) for b in target_branches]
    supabase.table("events").delete().in_("place_id", b_ids).gte("event_date", midnight).execute()
    for ev in events:
        if not is_valid_date(ev.get('event_date')): continue
        for branch in target_branches:
            should_add = (mode in ["global", "specific"])
            if mode == "mapping":
                loc_hint = str(ev.get('found_location', '')).lower()
                b_clean = branch['name'].lower().replace("library", "").strip()
                if b_clean and (b_clean in loc_hint or b_clean in ev['title'].lower()):
                    should_add = True
            if should_add:
                entry = ev.copy()
                entry.pop('found_location', None)
                entry.update({'place_id': branch['id'], 'place_name': branch['name'], 'zip_code': branch['zip_code']})
                supabase.table("events").insert(entry).execute()
                print(f"   ✨ {master_name} -> {branch['name']}: {ev['title']}")

def run_scraper():
    midnight_today = datetime.combine(datetime.now().date(), dt_time.min).isoformat()
    
    masters = supabase.table("places").select("*").eq("is_master", True).execute().data
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent=MOBILE_USER_AGENT)
        
        for m in masters:
            # --- UPDATED FILTER: ENSURE "LOST WORLDS" IS CAUGHT ---
            raw_cat = str(m.get('category') or m.get('category_name') or "").strip()
            
            # This is case-insensitive and handles hidden spaces
            if raw_cat.lower() != "indoor playground":
                continue 
            # ---------------------------------------------------

            branches = supabase.table("places").select("*").eq("parent_id", m['id']).execute().data
            if not branches: continue
            
            name_low = m['name'].lower().replace("’", "'")
            
            # Branching logic (remains untouched but filtered by category above)
            if any(x in name_low for x in ["home depot", "lowe's", "lowes"]):
                print(f"🛡️ Using Hybrid Logic for {m['name']}...")
                events = get_hybrid_retail_events(m['name'])
                save_events(events, branches, midnight_today, m['name'], mode="global")
            elif any(x in name_low for x in ["lego", "barnes", "slime"]):
                print(f"🔍 Dynamic Search for {m['name']}...")
                for branch in branches:
                    time.sleep(random.uniform(2.0, 4.0))
                    scrape_and_save(context, m, [branch], mode="specific", midnight=midnight_today, zip_code=branch['zip_code'])
            elif "library" in name_low:
                print(f"📚 Mapping Library Events for {m['name']}...")
                time.sleep(random.uniform(2.0, 4.0))
                scrape_and_save(context, m, branches, mode="mapping", midnight=midnight_today)
            else:
                # This covers Lost Worlds and Sky Zone
                print(f"🎡 Scraping Events for {m['name']}...")
                scrape_and_save(context, m, branches, mode="global", midnight=midnight_today)

        browser.close()

def scrape_and_save(context, master, target_branches, mode, midnight, zip_code=None):
    page = context.new_page()
    url = master['url'] if master['url'].startswith('http') else f'https://{master["url"]}'
    try:
        # Increased wait time for heavier pages like Facebook
        page.goto(url, wait_until="domcontentloaded", timeout=60000)
        time.sleep(10) 

        # --- FACEBOOK COOKIE & POP-UP BUSTER ---
        try:
            # Targets common "Allow Cookies" and "Close" buttons on FB and Playgrounds
            popups = page.locator("button:has-text('Allow all cookies'), div[aria-label='Close'], button:has-text('Accept'), button:has-text('Close')")
            if popups.first.is_visible():
                popups.first.click()
                time.sleep(2)
        except: pass

        if mode == "specific" and zip_code:
            try:
                search_field = page.locator("input[placeholder*='zip' i], input[placeholder*='City' i]").first
                search_field.wait_for(state="visible", timeout=10000)
                search_field.fill(str(zip_code))
                page.keyboard.press("Enter")
                time.sleep(15) 
            except: pass

        text = clean_html(page.content())
        today_str = datetime.now().strftime('%B %d, %Y')
        
        # Adjust prompt if it's a Facebook page
        platform_context = "This is a Facebook page. Look for event dates in the recent posts." if "facebook.com" in url else ""

        prompt = f"""
        Today is {today_str}. {platform_context}
        Find upcoming kids events (e.g., Toddler Time, Glow Jump, Special Parties) for {master['name']}.
        Output a JSON list with: "title", "event_date" (YYYY-MM-DD), "category_name", "window_type", "price_text", "snippet", "found_location".
        Rules: Use 2026. Return ONLY the JSON list inside brackets []. If no events found, return [].
        """
        
        events = []
        for attempt in range(3):
            try:
                # Use a larger slice of text for Facebook to ensure we get past the header
                response = client.models.generate_content(model='gemini-2.0-flash', contents=[prompt, text[:25000]])
                res_text = response.text.strip()
                json_match = re.search(r'\[.*\]', res_text, re.DOTALL)
                if json_match:
                    events = json.loads(json_match.group(0))
                break 
            except Exception as e:
                if "429" in str(e):
                    wait_time = (attempt + 1) * 15 
                    print(f"   ⏳ Rate limited. Waiting {wait_time}s...")
                    time.sleep(wait_time)
                else: raise e 

        if events:
            save_events(events, target_branches, midnight, master['name'], mode)
        else:
            print(f"   ℹ️ No events identified on page for {master['name']}.")
            
    except Exception as e:
        print(f"❌ Error scraping {master['name']}: {e}")
    finally:
        page.close()

if __name__ == "__main__":
    run_scraper()
    
