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

# --- Hybrid Step 1: Updated March Project Bank ---
PROJECT_BANK = {
    "home depot": {
        "2026-03-07": "Kids Workshop: Leprechaun Trap",
        "2026-04-04": "Kids Workshop: Farm Planter",
        "2026-05-02": "Kids Workshop: Mother's Day Frame"
    },
    "lowe's": {
        "2026-03-21": "Lowe's Kids Club: Lawn Mower",
        "2026-04-18": "Lowe's Kids Club: Terrarium",
        "2026-05-16": "Lowe's Kids Club: Birdhouse"
    }
}

# --- Utility Functions ---

def calculate_window(date_str):
    """Calculates the 3-tab window based on the event date."""
    try:
        ev_date = datetime.strptime(date_str, '%Y-%m-%d').date()
        diff = (ev_date - datetime.now().date()).days
        if diff <= 14: return "Daily Refresh"
        if diff <= 45: return "Weekly Deep Dive"
        return "Special Scout"
    except: return "Daily Refresh"

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
                "title": title,
                "event_date": date_str,
                "category_name": "Workshop",
                "window_type": "Morning",
                "price_text": "Free",
                "snippet": f"Free hands-on building event at {venue_name}. Materials provided."
            })
    return events

def clean_html(raw_html):
    soup = BeautifulSoup(raw_html, 'html.parser')
    for element in soup(["script", "style", "footer", "nav", "header", "aside", "svg"]):
        element.decompose()
    return soup.get_text(separator=' ', strip=True)

#def is_valid_date(date_str):
#    return bool(re.match(r'^\d{4}-\d{2}-\d{2}$', str(date_str)))

from datetime import datetime
import random
from dateutil import parser
def is_valid_date(date_str):
    """
    Normalizes date strings into YYYY-MM-DD.
    Ensures date is between Today and 90 days in the future.
    """
    if not date_str or not isinstance(date_str, str):
        return None
    
    today = datetime.now().date()
    limit = today + timedelta(days=90)
    
    try:
        # fuzzy=True helps ignore "at 10:00 AM" or "Thursday" inside the string
        parsed_date = parser.parse(date_str, fuzzy=True).date()
        
        # Validation: Must be today or in the next 90 days
        if today <= parsed_date <= limit:
            return parsed_date.strftime('%Y-%m-%d')
        
        # If it's a "Past" date from a static site, we ignore it
        return None
    except (ValueError, TypeError, OverflowError):
        return None
        
# --- Business Logic & Saving ---
def save_events(events, target_branches, midnight, master, mode):
    m_id = master['id']
    m_name = master.get('name', 'Unknown Place')
    today = datetime.now().date()
    
    # Update timestamp immediately to ensure last_scrape is NEVER null
    try:
        supabase.table("places").update({"last_scraped_at": datetime.now().isoformat()}).eq("id", m_id).execute()
    except Exception as e:
        print(f"   ⚠️ Failed to update timestamp for {m_name}: {e}")

    if not events:
        print(f"   ⚠️ No events found for {m_name}.")
        return

    print(f"   💾 Processing {len(events)} potential events for {m_name}...")

    # Clear old future records for this place
    supabase.table("events").delete().eq("place_id", m_id).gte("event_date", today.isoformat()).execute()

    for ev in events:
        # --- FIX: Handle List vs Dict (Type Safety) ---
        if isinstance(ev, list):
            # If AI returned [title, date, snippet, price]
            title = ev[0] if len(ev) > 0 else "Special Event"
            raw_date = ev[1] if len(ev) > 1 else today.isoformat()
            snippet = ev[2] if len(ev) > 2 else ""
            price = ev[3] if len(ev) > 3 else "See Website"
        elif isinstance(ev, dict):
            # If AI returned {"title": "...", "event_date": "..."}
            title = ev.get('title', 'Special Event')
            raw_date = ev.get('event_date', today.isoformat())
            snippet = ev.get('snippet', '')
            price = ev.get('price_text', 'See Website')
        else:
            continue

        # --- DATE VALIDATION ---
        date_str = is_valid_date(raw_date)
        if not date_str: continue
        
        ev_dt = datetime.strptime(date_str, '%Y-%m-%d').date()
        days_away = (ev_dt - today).days

        # Strict Date Guard (Today until +90 days)
        if days_away < 0 or days_away > 90: continue
        
        # Determine Window Label
        if days_away <= 14: window = "Daily Refresh"
        elif days_away <= 45: window = "Weekly Deep Dive"
        else: window = "Special Scout"

        # --- JUNK FILTER ---
        junk_words = ["hours", "closed", "private", "admission", "tickets", "schedule"]
        if any(word in title.lower() for word in junk_words):
            continue

        # --- SNIPPET CLEANUP ---
        if len(snippet) < 15 or any(char.isdigit() for char in snippet[:10]):
            snippet = f"Special exhibit: {title} at {m_name}."

        for branch in target_branches:
            entry = {
                'place_id': branch['id'],
                'place_name': branch.get('name', m_name),
                'title': title,
                'event_date': date_str,
                'snippet': snippet,
                'category_name': master.get('category', 'Special Activity'),
                'zip_code': branch.get('zip_code'),
                'window_type': window,
                'specificity_score': 10 if "exhibit" in title.lower() else 7,
                'price_text': price
            }
            
            try:
                supabase.table("events").insert(entry).execute()
            except Exception as e:
                print(f"      ❌ DB Insert Fail for '{title}': {e}")

def generate_with_retry(prompt, text_content, context_name="General"):
    """Centralized AI call logic with linear backoff (12s/24s/36s)."""
    for attempt in range(3):
        try:
            time.sleep(3) 
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
                wait_time = (attempt + 1) * 12
                print(f"   ⏳ Rate limited (429). Waiting {wait_time}s to retry {context_name}...")
                time.sleep(wait_time)
            else:
                print(f"   ⚠️ AI Error for {context_name}: {e}")
                break 
    return []

#def get_daily_batch(limit=24):
 #   """Reverted logic to fix nulls_first crash while keeping ID sorting."""
  #  three_days_ago = (datetime.now() - timedelta(days=3)).isoformat()
    # 1. Sort by last_scraped_at (NULLs naturally group together)
    # 2. Sort by ID (Ensures ID 1, 2, 3 come first within the NULL group)
   # res = supabase.table("places")\
    #    .select("*")\
     #   .eq("is_master", True)\
      #  .or_(f"last_scraped_at.is.null,last_scraped_at.lt.{three_days_ago}")\
       # .order("last_scraped_at")\
        #.order("id")\
        #.limit(limit)\
        #.execute()
    #return res.data
 
def get_daily_batch(limit=24):
    """Modified to strictly test IDs 1 through 5 only."""
    # We remove the three_days_ago filter to ensure we grab these 5 regardless of status
    res = supabase.table("places")\
        .select("*")\
        .in_("id", [1])\
        .order("id")\
        .limit(limit)\
        .execute()
    return res.data
       
# --- Scraper Pathway ---
# this function works for the category of workshop, but not others
#def scrape_and_save(context, master, target_branches, mode, midnight, zip_code=None):
#    page = context.new_page()
#    url = master['url'] if master['url'].startswith('http') else f'https://{master["url"]}'

#    today = datetime.now()
#    future_date = today + timedelta(days=90)
#    range_str = f"{today.strftime('%B %d, %Y')} to {future_date.strftime('%B %d, %Y')}"
    
#    try:
#        # 1. Navigation with 'networkidle' to catch initial API calls
#        page.goto(url, wait_until="networkidle", timeout=90000)
        
#        if mode == "specific" and zip_code:
#            try:
#                search_field = page.locator("input[placeholder*='zip' i], input[placeholder*='City' i]").first
#                search_field.wait_for(state="visible", timeout=10000)
#                search_field.fill(str(zip_code))
#                page.keyboard.press("Enter")
#                time.sleep(10) 
#            except: pass

#        if mode != "specific":
#            # 2. UNIVERSAL FIX: Exhaustive Scroll
#            # We scroll to the very bottom in small increments to trigger 'Lazy Loading' cards
#            print(f"   🖱️ Scrolling {master['name']} to trigger lazy-load...")
#            for _ in range(8):
#                page.evaluate("window.scrollBy(0, window.innerHeight)")
#                time.sleep(2) # Give the images/text time to 'pop' in
            
#            # 3. Buffer for any final background data
#            time.sleep(5) 
#            page.screenshot(path=f"debug_{re.sub(r'\W+', '', master['name'])}.png")

#        # 4. CAPTURE ALL DATA (Main + Iframes)
#        # By getting text AFTER the exhaustive scroll, we capture 'Featured' sections that were hidden
#        all_text = [page.evaluate("document.body.innerText")]
#        for frame in page.frames:
#            try:
#                f_text = frame.evaluate("document.body.innerText")
#                if len(f_text) > 50: all_text.append(f_text)
#            except: continue
#        combined_text = "\n---\n".join(all_text)
        
#        # 5. The 90-Day Sliding Prompt
#        prompt = f"""
#        Today is {today.strftime('%B %d, %Y')}. 
#        Find ALL upcoming public events, workshops, or special exhibits for {master['name']} between {range_str}.
#        I need the 'New and Featured' events as well as recurring programs.
#        Output JSON list: ["title", "event_date" (YYYY-MM-DD), "category_name", "window_type", "price_text", "snippet", "found_location"].
#        Rules:
#        1. Year must be 2026.
#        2. If no specific 'kids' events found, include family-friendly programs.
#        3. Return ONLY the JSON list []. If none, return [].
#        """
        
#        events = generate_with_retry(prompt, combined_text, master['name'])

#        if events:
#            save_events(events, target_branches, midnight, master, mode)
#            print(f"   ✅ Successfully found {len(events)} events for {master['name']}.")
#        else:
#            print(f"   ⚠️ Gemini found 0 events for {master['name']} in the {range_str} window.")
            
#    except Exception as e:
#        print(f"❌ Error scraping {master['name']}: {e}")
#    finally:
#        page.close()

def scrape_and_save(context, master, target_branches, mode, midnight, zip_code=None):
    page = context.new_page()
    url = master['url'] if master['url'].startswith('http') else f'https://{master["url"]}'
    
    today_dt = datetime.now()
    today_str = today_dt.strftime('%Y-%m-%d')
    limit_str = (today_dt + timedelta(days=90)).strftime('%Y-%m-%d')
    
    try:
        print(f"📡 Deep-Scanning: {master['name']} (ID: {master['id']})")
        
        # 🛡️ STEALTH & HEADERS
        page.set_extra_http_headers({
            "Referer": "https://www.google.com/",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36..."
        })

        # Navigate with a generous timeout
        page.goto(url, wait_until="networkidle", timeout=60000)

        # 🖱️ DYNAMIC LOADING (Crucial for IDs 2, 3, 4)
        # Scroll slowly to trigger lazy-loaded museum cards
        for i in range(3):
            page.mouse.wheel(0, 700)
            time.sleep(1.5)
        
        # 🧩 SMART EXTRACTION: Focus on main content, skip headers/footers
        # This prevents the AI from seeing "Museum Hours" in the footer.
        content_area = page.evaluate("""() => {
            const selectors = ['main', 'article', '#content', '.calendar', '.events-grid'];
            for (let s of selectors) {
                let el = document.querySelector(s);
                if (el && el.innerText.length > 200) return el.innerText;
            }
            return document.body.innerText; // Fallback
        }""")

        # 🧠 THE "SPECIAL EXHIBIT" PROMPT
        prompt = f"""
        Extract only HIGH-VALUE special events/exhibits for {master['name']}.
        Current Date: {today_str}. 
        Valid Range: {today_str} to {limit_str}.

        STRICT QUALITY RULES:
        1. ONLY extract named exhibits, workshops, or seasonal festivals.
        2. IGNORE "Daily Hours", "General Admission", "Private Events", or "Closures".
        3. IGNORE standard museum navigation links.
        4. If an exhibit is 'Permanent' or 'Featured' but has no date, use "{today_str}".
        5. snippet MUST be a unique description. Do NOT include dates/times in snippet.

        FORMAT: Return a JSON LIST of objects:
        [
          {{"title": "...", "event_date": "YYYY-MM-DD", "snippet": "...", "price_text": "..."}}
        ]
        """
        
        # Generate with Gemini
        events = generate_with_retry(prompt, content_area, master['name'])
        
        # Call save_events (Timestamp update is handled inside save_events)
        save_events(events or [], target_branches, midnight, master, mode)
        
    except Exception as e:
        print(f"   ❌ Error Scoping ID {master['id']} ({master['name']}): {e}")
        # Always attempt to update timestamp so the dashboard stays fresh
        try:
            supabase.table("places").update({"last_scraped_at": datetime.now().isoformat()}).eq("id", master['id']).execute()
        except: pass
    finally:
        page.close()

def run_gemini_discovery(midnight):
    today = datetime.now()
    future_date = today + timedelta(days=90)
    range_str = f"{today.strftime('%B %Y')} to {future_date.strftime('%B %Y')}"
    
    print(f"🧠 Running Discovery for Bay Area festivals ({range_str})...")
    
    prompt = f"Find 8 major kids festivals in the SF Bay Area happening between {range_str}. Return JSON: [title, event_date(YYYY-MM-DD), price_text, snippet]."
    
    events = generate_with_retry(prompt, "Bay Area", "Discovery")
    if events:
        # Use ID 9999 to prevent collision with Academy of Sciences (ID 1)
        discovery_master = {"id": 9999, "name": "Bay Area Pop-up", "category": "Special Events"}
        save_events(events, [{"id": 9999, "name": "Bay Area Pop-up", "zip_code": "94103"}], midnight, discovery_master, "global")
        
def run_scraper():
    midnight_today = datetime.combine(datetime.now().date(), dt_time.min).isoformat()
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
                print(f"🛡️ Hybrid: {m['name']}")
                save_events(get_hybrid_retail_events(m['name']), branches, midnight_today, m, "global")

            elif any(x in name_low for x in ["lego", "barnes", "slime"]):
                print(f"🔍 Dynamic: {m['name']}")
                for branch in branches:
                    time.sleep(random.uniform(1.5, 3.5))
                    scrape_and_save(context, m, [branch], "specific", midnight_today, branch['zip_code'])
            
            elif "library" in name_low:
                print(f"📚 Library Mapping: {m['name']}")
                time.sleep(random.uniform(2.0, 4.0))
                scrape_and_save(context, m, branches, "mapping", midnight_today)
            
            else:
                print(f"🌐 Universal Scrape: {m['name']}")
                scrape_and_save(context, m, branches, "global", midnight_today)

        browser.close()
    run_gemini_discovery(midnight_today)

#if __name__ == "__main__":
#    run_scraper()
if __name__ == "__main__":
    midnight = datetime.combine(datetime.now().date(), dt_time.min).isoformat()
    
    print("🚀 STARTING TARGETED TEST FOR IDs 1-5...")
    
    # Force reset so they aren't skipped by the 'last_scraped_at' filter
    try:
        supabase.table("places").update({"last_scraped_at": None}).in_("id", [1, 2, 3, 4, 5]).execute()
    except Exception as e:
        print(f"⚠️ Note: Could not reset timestamps: {e}")

    # Fetch IDs 1-5 directly
    res = supabase.table("places").select("*").in_("id", [1, 2, 3, 4, 5]).execute()
    batch = res.data

    if batch:
        with sync_playwright() as p:
            # Launch with 'Stealth' mode enabled
            browser = p.chromium.launch(headless=True, args=['--disable-blink-features=AutomationControlled'])
            
            # Use a Desktop context to ensure the 'Exhibits' layout is full-width
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            )
            
            for master in batch:
                # We pass [master] as the 'target_branches' because in your current 
                # setup, the master IS the place we want to save to.
                scrape_and_save(context, master, [master], "mapping", midnight)
            
            browser.close()
    else:
        print("❌ Could not find IDs 1-5 in the places table.")
