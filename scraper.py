import os
import time
import json
import re
import random
from datetime import datetime, timedelta, time as dt_time
from google import genai
from supabase import create_client
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

# --- Setup ---
supabase = create_client(os.environ['VITE_SUPABASE_URL'], os.environ['VITE_SUPABASE_KEY'])
client = genai.Client(api_key=os.environ['GEMINI_API_KEY'])

MOBILE_USER_AGENT = 'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1'

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

# --- Utilities ---

def clean_html(raw_html):
    soup = BeautifulSoup(raw_html, 'html.parser')
    for element in soup(["script", "style", "footer", "nav", "header", "aside", "svg"]):
        element.decompose()
    return soup.get_text(separator=' ', strip=True)

def is_valid_date(date_str):
    return bool(re.match(r'^\d{4}-\d{2}-\d{2}$', str(date_str)))

def get_window_type(event_date_str):
    try:
        ev_date = datetime.strptime(event_date_str, '%Y-%m-%d').date()
        today = datetime.now().date()
        diff = (ev_date - today).days
        if diff <= 14: return "Daily Refresh"
        if diff <= 45: return "Weekly Deep Dive"
        return "Special Scout"
    except:
        return "Daily Refresh"

def get_daily_batch(limit=24):
    three_days_ago = (datetime.now() - timedelta(days=3)).isoformat()
    res = supabase.table("places") \
        .select("*") \
        .eq("is_master", True) \
        .or_(f"last_scraped_at.is.null,last_scraped_at.lt.{three_days_ago}") \
        .order("last_scraped_at", desc=False) \
        .limit(limit) \
        .execute()
    return res.data

def generate_with_retry(prompt, text_content):
    for attempt in range(3):
        try:
            response = client.models.generate_content(
                model='gemini-2.0-flash', 
                contents=[prompt, text_content[:28000]]
            )
            res_text = response.text.strip()
            json_match = re.search(r'\[.*\]', res_text, re.DOTALL)
            if json_match: return json.loads(json_match.group(0))
            return []
        except Exception as e:
            if "429" in str(e):
                time.sleep((2 ** attempt) * 15)
            else: break
    return []

# --- Event Management ---

def save_events(events, target_branches, master_name, mode):
    for ev in events:
        if not is_valid_date(ev.get('event_date')): continue
        window = get_window_type(ev['event_date'])
        
        for branch in target_branches:
            existing = supabase.table("events").select("id") \
                .eq("event_date", ev['event_date']) \
                .eq("place_id", branch['id']) \
                .ilike("title", f"%{ev['title'][:15]}%").execute().data
            
            if existing: continue

            if mode == "mapping" and branch['name'].lower() not in (ev['title'] + ev.get('snippet', '')).lower():
                continue

            entry = {
                'title': ev['title'],
                'event_date': ev['event_date'],
                'category_name': ev.get('category_name', 'Activity'),
                'window_type': window,
                'price_text': ev.get('price_text', 'Free'),
                'snippet': ev.get('snippet', ''),
                'place_id': branch['id'],
                'place_name': branch['name'],
                'zip_code': branch['zip_code'],
                'created_at': datetime.now().isoformat()
            }
            supabase.table("events").insert(entry).execute()
            print(f"   ✨ {master_name} -> {branch['name']}: {ev['title']} ({window})")

# --- Scraper Pathways ---

def run_gemini_discovery():
    today_str = datetime.now().strftime('%Y-%m-%d')
    prompt = f"Today is {today_str}. Search for major San Francisco Bay Area kids festivals or seasonal pop-ups in the next 90 days. Return ONLY JSON list: ['title', 'event_date', 'category_name', 'price_text', 'snippet']."
    print("🧠 Running Daily Pop-up Discovery...")
    events = generate_with_retry(prompt, "Bay Area Kids Events")
    if events:
        community_branch = {"id": 1, "name": "Bay Area Pop-up", "zip_code": "94103"}
        save_events(events, [community_branch], "Discovery", mode="global")

def scrape_and_save(context, master, branches, mode, zip_code=None):
    page = context.new_page()
    url = master['url'] if master['url'].startswith('http') else f'https://{master["url"]}'
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=60000)
        page.evaluate("window.scrollBy(0, 1200)")
        time.sleep(5)
        text = clean_html(page.content())
        today = datetime.now().strftime('%Y-%m-%d')

        cat_str = (master.get('category') or "").lower()
        is_workshop = "workshop" in cat_str or "experience" in cat_str or "library" in master['name'].lower()
        
        if is_workshop:
            # High-Volume / Generous Mode
            prompt = f"Today is {today}. Extract ALL scheduled kids events and workshops from this text: {text}. Include weekly sessions."
        else:
            # Curator / Special Mode (for Museums/Zoos)
            prompt = f"""
            Today is {today}. Look at {master['name']} website: {text}.
            1. Focus specifically on 'Upcoming Events', 'Calendar', or 'Featured' sections.
            2. Extract scheduled programs with a specific date.
            3. IGNORE standard daily hours or permanent gallery info.
            Return ONLY JSON list: ["title", "event_date", "category_name", "price_text", "snippet"].
            """

        events = generate_with_retry(prompt, text)
        if events: save_events(events, branches, master['name'], mode)
    except Exception as e:
        print(f"❌ Error {master['name']}: {e}")
    finally:
        page.close()

def get_hybrid_retail_events(venue_name):
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
        if name_key == "lowe's": target_date += timedelta(weeks=2)
        date_str = target_date.strftime('%Y-%m-%d')
        if target_date.date() >= today.date():
            events.append({
                "title": PROJECT_BANK.get(name_key, {}).get(date_str, f"{venue_name} Workshop"),
                "event_date": date_str, "category_name": "Workshop", "price_text": "Free"
            })
    return events

# --- Main Runner ---

def run_scraper():
    midnight_today = datetime.combine(datetime.now().date(), dt_time.min).isoformat()
    masters = get_daily_batch(limit=24)
    
    if masters:
        print(f"🚀 Processing Batch: {len(masters)} masters...")
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(user_agent=MOBILE_USER_AGENT)
            
            for m in masters:
                # Update timestamp
                supabase.table("places").update({"last_scraped_at": datetime.now().isoformat()}).eq("id", m['id']).execute()
                
                branches = supabase.table("places").select("*").eq("parent_id", m['id']).execute().data
                if not branches: continue
                name_low = m['name'].lower().replace("’", "'")

                # PATHWAY 1: Hybrid Retail
                if any(x in name_low for x in ["home depot", "lowe's", "lowes"]):
                    print(f"🛡️ Using Hybrid Logic for {m['name']}...")
                    save_events(get_hybrid_retail_events(m['name']), branches, m['name'], mode="global")

                # PATHWAY 2: Dynamic Retail (LEGO, Barnes & Noble, Slime Kitchen)
                elif any(x in name_low for x in ["lego", "barnes", "slime"]):
                    print(f"🔍 Dynamic Search for {m['name']} branches...")
                    for branch in branches:
                        time.sleep(random.uniform(1.5, 3.5))
                        scrape_and_save(context, m, [branch], mode="specific", zip_code=branch['zip_code'])

                # PATHWAY 3: Libraries
                elif "library" in name_low:
                    print(f"📚 Mapping Library Events for {m['name']}...")
                    time.sleep(random.uniform(2.0, 4.0))
                    scrape_and_save(context, m, branches, mode="mapping")

                # PATHWAY 4: Everything else (Museums, Zoos, Playgrounds)
                else:
                    print(f"🏛️ Curator Search for {m['name']}...")
                    scrape_and_save(context, m, branches, mode="global")

            browser.close()

    run_gemini_discovery()

if __name__ == "__main__":
    run_scraper()
    
