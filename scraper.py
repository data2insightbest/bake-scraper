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

# --- Core Utilities ---

def clean_html(raw_html):
    """Fixed BS4 parser error by specifying 'html.parser'."""
    soup = BeautifulSoup(raw_html, 'html.parser')
    for element in soup(["script", "style", "footer", "nav", "header", "aside", "svg"]):
        element.decompose()
    return soup.get_text(separator=' ', strip=True)

def is_valid_date(date_str):
    return bool(re.match(r'^\d{4}-\d{2}-\d{2}$', str(date_str)))

def get_daily_batch(limit=24):
    """Batches places: 24+24+rest. Ensures a full rotation every 3 days."""
    three_days_ago = (datetime.now() - timedelta(days=3)).isoformat()
    res = supabase.table("places") \
        .select("*") \
        .eq("is_master", True) \
        .or_(f"last_scraped_at.is.null,last_scraped_at.lt.{three_days_ago}") \
        .order("last_scraped_at", ascending=True) \
        .limit(limit) \
        .execute()
    return res.data

# --- Gemini Logic with Exponential Backoff ---

def generate_with_retry(prompt, text_content, max_attempts=3):
    """Implements Exponential Backoff (10s, 20s, 40s) for API resilience."""
    for attempt in range(max_attempts):
        try:
            response = client.models.generate_content(
                model='gemini-2.0-flash', 
                contents=[prompt, text_content[:25000]]
            )
            res_text = response.text.strip()
            json_match = re.search(r'\[.*\]', res_text, re.DOTALL)
            if json_match:
                return json.loads(json_match.group(0))
            return []
        except Exception as e:
            if "429" in str(e):
                wait_time = (2 ** attempt) * 10 
                print(f"   ⏳ Rate limited (429). Attempt {attempt+1}/{max_attempts}. Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                print(f"   ⚠️ Gemini Error: {e}")
                break 
    return []

# --- Event Management ---

def save_events(events, target_branches, midnight, master_name, mode):
    for ev in events:
        if not is_valid_date(ev.get('event_date')): continue
        for branch in target_branches:
            # DEDUPLICATION: Check for existing title+date for this place
            existing = supabase.table("events") \
                .select("id") \
                .eq("event_date", ev['event_date']) \
                .eq("place_id", branch['id']) \
                .ilike("title", f"%{ev['title'][:15]}%") \
                .execute().data
            
            if existing: continue

            entry = ev.copy()
            entry.pop('found_location', None)
            entry.update({
                'place_id': branch['id'], 
                'place_name': branch['name'], 
                'zip_code': branch['zip_code'],
                'created_at': datetime.now().isoformat()
            })
            supabase.table("events").insert(entry).execute()
            print(f"   ✨ {master_name} -> {branch['name']}: {ev['title']}")

# --- Scraper Pathways ---

def get_hybrid_retail_events(venue_name):
    """Your original logic for Home Depot and Lowe's (Fully Restored)."""
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

def run_gemini_discovery(midnight):
    """Daily Discovery: Searching for Bay Area pop-ups in the next 3 months."""
    today_str = datetime.now().strftime('%Y-%m-%d')
    future_str = (datetime.now() + timedelta(days=90)).strftime('%Y-%m-%d')
    
    prompt = f"""
    Today is {today_str}. Find kids special events or holiday pop-ups in the SF Bay Area 
    from {today_str} to {future_str}. Focus on one-time events like festivals.
    Return ONLY a JSON list: ["title", "event_date", "category_name", "window_type", "price_text", "snippet", "found_location"].
    """
    print("🧠 Running Daily Pop-up Discovery...")
    events = generate_with_retry(prompt, "San Francisco Bay Area Special Events")
    if events:
        # 999 is a 'Community' marker place_id
        community_branch = {"id": 1, "name": "Bay Area Pop-up", "zip_code": "94103"}
        save_events(events, [community_branch], midnight, "Discovery", mode="global")

def scrape_and_save(context, master, target_branches, mode, midnight, zip_code=None):
    page = context.new_page()
    url = master['url'] if master['url'].startswith('http') else f'https://{master["url"]}'
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=60000)
        time.sleep(5)
        text = clean_html(page.content())
        today_str = datetime.now().strftime('%B %d, %Y')

        prompt = f"""
        Today is {today_str}. Find ONLY special, one-time kids events for {master['name']}. 
        IGNORE routine daily open play. Look for holiday events or themed parties.
        Return ONLY JSON list: ["title", "event_date", "category_name", "window_type", "price_text", "snippet", "found_location"].
        """
        events = generate_with_retry(prompt, text)
        if events:
            save_events(events, target_branches, midnight, master['name'], mode)
    except Exception as e:
        print(f"❌ Error scraping {master['name']}: {e}")
    finally:
        page.close()

# --- Main Runner ---

def run_scraper():
    midnight_today = datetime.combine(datetime.now().date(), dt_time.min).isoformat()
    
    # 1. Pop-up Search
    run_gemini_discovery(midnight_today)

    # 2. Batch Processing
    masters = get_daily_batch(limit=24)
    print(f"🚀 Processing Daily Batch: {len(masters)} places...")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent=MOBILE_USER_AGENT)
        
        for m in masters:
            # Update timestamp so it moves to the bottom of the list
            supabase.table("places").update({"last_scraped_at": datetime.now().isoformat()}).eq("id", m['id']).execute()

            branches = supabase.table("places").select("*").eq("parent_id", m['id']).execute().data
            if not branches: continue
            
            name_low = m['name'].lower().replace("’", "'")
            
            if any(x in name_low for x in ["home depot", "lowe's", "lowes"]):
                events = get_hybrid_retail_events(m['name'])
                save_events(events, branches, midnight_today, m['name'], mode="global")
            elif any(x in name_low for x in ["lego", "barnes", "slime"]):
                for br in branches:
                    scrape_and_save(context, m, [br], "specific", midnight_today, zip_code=br['zip_code'])
            elif "library" in name_low:
                scrape_and_save(context, m, branches, "mapping", midnight_today)
            else:
                scrape_and_save(context, m, branches, "global", midnight_today)

        browser.close()

if __name__ == "__main__":
    run_scraper()
    
