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

# --- Utility Functions ---

def clean_html(raw_html):
    soup = BeautifulSoup(raw_html, 'html.parser')
    for element in soup(["script", "style", "footer", "nav", "header", "aside", "svg"]):
        element.decompose()
    return soup.get_text(separator=' ', strip=True)

def is_valid_date(date_str):
    return bool(re.match(r'^\d{4}-\d{2}-\d{2}$', str(date_str)))

def calculate_window(date_str):
    """Calculates the 3-tab window based on the event date."""
    try:
        ev_date = datetime.strptime(date_str, '%Y-%m-%d').date()
        diff = (ev_date - datetime.now().date()).days
        if diff <= 14: return "Daily Refresh"
        if diff <= 45: return "Weekly Deep Dive"
        return "Special Scout"
    except: return "Daily Refresh"

def get_daily_batch(limit=24):
    """Rotation logic for scraping masters."""
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
    """Central AI caller with rate-limit handling."""
    for attempt in range(3):
        try:
            response = client.models.generate_content(
                model='gemini-2.0-flash', 
                contents=[prompt, text_content[:28000]]
            )
            res_text = response.text.strip()
            json_match = re.search(r'\[.*\]', res_text, re.DOTALL)
            if json_match:
                return json.loads(json_match.group(0))
            return []
        except Exception as e:
            if "429" in str(e):
                wait_time = (attempt + 1) * 15
                print(f"   ⏳ Rate limited. Waiting {wait_time}s...")
                time.sleep(wait_time)
            else:
                print(f"   ⚠️ AI Error: {e}")
                break
    return []

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
            title = PROJECT_BANK.get(name_key, {}).get(date_str, f"{venue_name} Kids Workshop")
            events.append({
                "title": title, "event_date": date_str, "category_name": "Workshop",
                "price_text": "Free", "snippet": f"Building event at {venue_name}."
            })
    return events

# --- Logic & Saving ---

def save_events(events, target_branches, midnight, master_name, mode):
    b_ids = [int(b['id']) for b in target_branches]
    supabase.table("events").delete().in_("place_id", b_ids).gte("event_date", midnight).execute()

    for ev in events:
        if not is_valid_date(ev.get('event_date')): continue
        window = calculate_window(ev['event_date'])
        
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
                entry.update({
                    'place_id': branch['id'], 'place_name': branch['name'], 
                    'zip_code': branch['zip_code'], 'window_type': window
                })
                supabase.table("events").insert(entry).execute()
                print(f"   ✨ {master_name} -> {branch['name']}: {ev['title']} ({window})")

def run_gemini_discovery(midnight):
    print("🧠 Running Discovery for major Bay Area festivals...")
    today_str = datetime.now().strftime('%B %d, %Y')
    prompt = f"Today is {today_str}. Search for major San Francisco Bay Area kids festivals or seasonal community events in the next 90 days. Return JSON list: ['title', 'event_date', 'category_name', 'price_text', 'snippet']."
    events = generate_with_retry(prompt, "Bay Area Kids Events")
    if events:
        discovery_branch = {"id": 1, "name": "Bay Area Pop-up", "zip_code": "94103"}
        save_events(events, [discovery_branch], midnight, "Discovery", mode="global")

# --- Scraper Pathway ---

def scrape_and_save(context, master, target_branches, mode, midnight, zip_code=None):
    page = context.new_page()
    url = master['url'] if master['url'].startswith('http') else f'https://{master["url"]}'
    try:
        page.goto(url, wait_until="networkidle", timeout=60000)
        page.evaluate("window.scrollBy(0, 1000)")
        time.sleep(5) 

        if mode == "specific" and zip_code:
            try:
                search_field = page.locator("input[placeholder*='zip' i], input[placeholder*='City' i]").first
                search_field.fill(str(zip_code))
                page.keyboard.press("Enter")
                time.sleep(15) 
            except: pass

        text = clean_html(page.content())
        today_str = datetime.now().strftime('%B %d, %Y')
        
        cat_name = (master.get('category') or "").lower()
        if "workshop" in cat_name or "library" in master['name'].lower():
            prompt = f"Today is {today_str}. Find ALL kids events for {master['name']}. Output JSON list: [title, event_date, category_name, price_text, snippet, found_location]."
        else:
            prompt = f"Today is {today_str}. Act as a curator. Focus on 'Upcoming Events' or 'Featured' for {master['name']}. Ignore daily routine hours. Output JSON list: [title, event_date, category_name, price_text, snippet, found_location]."
        
        events = generate_with_retry(prompt, text)
        if events:
            save_events(events, target_branches, midnight, master['name'], mode)
            
    except Exception as e:
        print(f"❌ Error scraping {master['name']}: {e}")
    finally:
        page.close()

def run_scraper():
    midnight_today = datetime.combine(datetime.now().date(), dt_time.min).isoformat()
    supabase.table("events").delete().lt("event_date", midnight_today).execute()
    
    masters = get_daily_batch(limit=24)
    if not masters: return

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent=MOBILE_USER_AGENT)
        
        for m in masters:
            supabase.table("places").update({"last_scraped_at": datetime.now().isoformat()}).eq("id", m['id']).execute()
            branches = supabase.table("places").select("*").eq("parent_id", m['id']).execute().data
            if not branches: continue
            
            name_low = m['name'].lower().replace("’", "'")
            
            if any(x in name_low for x in ["home depot", "lowe's", "lowes"]):
                save_events(get_hybrid_retail_events(m['name']), branches, midnight_today, m['name'], "global")
            elif any(x in name_low for x in ["lego", "barnes", "slime"]):
                for branch in branches:
                    time.sleep(random.uniform(1.5, 3.5))
                    scrape_and_save(context, m, [branch], "specific", midnight_today, branch['zip_code'])
            elif "library" in name_low:
                time.sleep(random.uniform(2.0, 4.0))
                scrape_and_save(context, m, branches, "mapping", midnight_today)
            else:
                scrape_and_save(context, m, branches, "global", midnight_today)
        browser.close()
    
    run_gemini_discovery(midnight_today)

if __name__ == "__main__":
    run_scraper()
    
