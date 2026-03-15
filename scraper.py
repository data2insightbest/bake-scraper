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
def get_clean_text(page):
    """Strips out scripts, styles, and footers to give Gemini clean data."""
    return page.evaluate("""() => {
        // 1. Remove non-content elements
        const junk = document.querySelectorAll('script, style, footer, nav, header, iframe, noscript');
        junk.forEach(el => el.remove());
        
        // 2. Return clean text
        return document.body.innerText.replace(/\\s+/g, ' ').trim();
    }""")
    
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
    m_name = master.get('name', 'Unknown')
    today = datetime.now().date()
    
    # ALWAYS Update timestamp immediately
    try:
        supabase.table("places").update({"last_scraped_at": datetime.now().isoformat()}).eq("id", m_id).execute()
    except: pass

    if not events:
        return

    # Delete existing data for the 90-day future window to avoid duplicates
    limit_date = today + timedelta(days=90)
    supabase.table("events").delete().eq("place_id", m_id).gte("event_date", today.isoformat()).lte("event_date", limit_date.isoformat()).execute()

    for ev in events:
        # Normalize AI output (List vs Dict)
        if isinstance(ev, list):
            title, r_date, snippet = ev[0], ev[1], ev[2]
        else:
            title = ev.get('title', 'Special Event')
            r_date = ev.get('event_date', str(today))
            snippet = ev.get('snippet', '')

        # Use our improved is_valid_date (no hard-coded 2024)
        date_str = is_valid_date(r_date)
        if not date_str: continue
        
        ev_dt = datetime.strptime(date_str, '%Y-%m-%d').date()
        days_away = (ev_dt - today).days
        
        # --- YOUR 3 STRICT CATEGORIES ---
        if days_away <= 14:
            window = "Daily Refresh"
        elif days_away <= 45:
            window = "Weekly Deep Dive"
        elif days_away <= 90:
            window = "Special Scout"
        else:
            continue

        # --- JUNK & HALLUCINATION CLEANUP ---
        title_low = title.lower()
        if any(j in title_low for j in ["incoming", "hours", "schedule", "admission", "closed", "private", "get started", "basics", "iphone", "ipad", "mac", "skills", "photo walk", "video walk"]):
            continue
            
        # If Gemini quotes the prompt or returns a placeholder snippet
        if "featured exhibit" in snippet.lower() or len(snippet) < 15:
            snippet = f"Special program: {title} at {m_name}."

        for branch in target_branches:
            found_loc = ev.get('found_location', 'All').lower()
            branch_name = branch.get('name', '').lower()
            # LOGIC: Save if it's for 'All' branches, OR if the branch name appears in the event's location text
            should_save = (found_loc == "all") or (found_loc in branch_name) or (branch_name in found_loc)
            if not should_save:
                 continue # Skip this branch if it's not the right match
            entry = {
                'place_id': branch['id'],
                'place_name': branch.get('name', m_name),
                'title': title,
                'event_date': date_str,
                'snippet': snippet,
                'category_name': master.get('category', 'Special Activity'),
                'zip_code': branch.get('zip_code'),
                'window_type': window,
                'specificity_score': 10 if "exhibit" in title_low else 7
            }
            try:
                supabase.table("events").insert(entry).execute()
            except: pass

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

def get_daily_batch(limit=24):
    """Reverted logic to fix nulls_first crash while keeping ID sorting."""
    three_days_ago = (datetime.now() - timedelta(days=3)).isoformat()
    # 1. Sort by last_scraped_at (NULLs naturally group together)
    # 2. Sort by ID (Ensures ID 1, 2, 3 come first within the NULL group)
    res = supabase.table("places")\
        .select("*")\
        .eq("is_master", True)\
        .or_(f"last_scraped_at.is.null,last_scraped_at.lt.{three_days_ago}")\
        .order("last_scraped_at")\
        .order("id")\
        .limit(limit)\
        .execute()
    return res.data
 
#def get_daily_batch(limit=24):
#    """Modified to strictly test IDs 1 through 5 only."""
    # We remove the three_days_ago filter to ensure we grab these 5 regardless of status
#    res = supabase.table("places")\
#        .select("*")\
#        .in_("id", [1])\
#        .order("id")\
#        .limit(limit)\
#        .execute()
#    return res.data
       
# --- Scraper Pathway --- this function works for the category of workshop, but not others
def scrape_and_save_1(context, master, target_branches, mode, midnight, zip_code=None):
    page = context.new_page()
    url = master['url'] if master['url'].startswith('http') else f'https://{master["url"]}'

    today = datetime.now()
    future_date = today + timedelta(days=90)
    range_str = f"{today.strftime('%B %d, %Y')} to {future_date.strftime('%B %d, %Y')}"
    
    try:
        # 1. Navigation with 'networkidle' to catch initial API calls
        page.goto(url, wait_until="networkidle", timeout=90000)
        
        if mode == "specific" and zip_code:
            try:
                search_field = page.locator("input[placeholder*='zip' i], input[placeholder*='City' i]").first
                search_field.wait_for(state="visible", timeout=10000)
                search_field.fill(str(zip_code))
                page.keyboard.press("Enter")
                time.sleep(10) 
            except: pass

        if mode != "specific":
            # 2. UNIVERSAL FIX: Exhaustive Scroll
            # We scroll to the very bottom in small increments to trigger 'Lazy Loading' cards
            print(f"   🖱️ Scrolling {master['name']} to trigger lazy-load...")
            for _ in range(8):
                page.evaluate("window.scrollBy(0, window.innerHeight)")
                time.sleep(2) # Give the images/text time to 'pop' in
            
            # 3. Buffer for any final background data
            time.sleep(5) 
            page.screenshot(path=f"debug_{re.sub(r'\W+', '', master['name'])}.png")

        # 4. CAPTURE ALL DATA (Main + Iframes)
        # By getting text AFTER the exhaustive scroll, we capture 'Featured' sections that were hidden
        # NEW: Optimized clean capture
        combined_text = get_clean_text(page)
        # 5. The 90-Day Sliding Prompt
        # Force a shorter, stricter JSON structure to avoid "Delimiter" errors
        prompt = f"""
        Extract events at {master['name']} from {today.strftime('%B %d, %Y')} to {future_date.strftime('%B %d, %Y')}.
        Rules:
        1. Return ONLY a JSON list of objects: [{{"title": "...", "event_date": "YYYY-MM-DD", "snippet": "..."}}]
        2. Snippet must be 1 sentence describing the activity.
        3. If no events found, return [].
        4. If no specific 'kids' events found, include family-friendly programs.
        5. TARGET: Only include events for children (0-12), teens, or families.
        6. EXCLUDE: Adult-only programming (Tax prep, ESL for adults, Career workshops, Senior socials, Book clubs for adults).
        7. EXCLUDE: Technical demos (iPhone/Mac basics) unless specifically for kids.
        8. LOCATION: Identify which specific branch the event is at. 
        9. RECURRING: For daily events, only provide TWO entries per week (Saturdays and Sundays).
        Output JSON list: ["title", "event_date" (YYYY-MM-DD), "category_name", "window_type", "price_text", "snippet"].
        Rule: If an event is ambiguous, ask: "Is this for a parent to bring a child to?" If No, ignore it.
        """
        
        #all_text = [page.evaluate("document.body.innerText")]
        #for frame in page.frames:
        #    try:
        #        f_text = frame.evaluate("document.body.innerText")
        #        if len(f_text) > 50: all_text.append(f_text)
        #    except: continue
        #combined_text = "\n---\n".join(all_text)
        # 5. The 90-Day Sliding Prompt
        #prompt = f"""
        #Today is {today.strftime('%B %d, %Y')}. 
        #Find ALL upcoming public events, workshops, or special exhibits for {master['name']} between {range_str}.
        #I need the 'New and Featured' events as well as recurring programs.
        #Output JSON list: ["title", "event_date" (YYYY-MM-DD), "category_name", "window_type", "price_text", "snippet", "found_location"].
        #Rules:
        #1. Year must be 2026.
        #2. If no specific 'kids' events found, include family-friendly programs.
        #3. Return ONLY the JSON list []. If none, return [].
        
        events = generate_with_retry(prompt, combined_text, master['name'])

        if events:
            save_events(events, target_branches, midnight, master, mode)
            print(f"   ✅ Successfully found {len(events)} events for {master['name']}.")
        else:
            print(f"   ⚠️ Gemini found 0 events for {master['name']} in the {range_str} window.")
            
    except Exception as e:
        print(f"❌ Error scraping {master['name']}: {e}")
    finally:
        page.close()

import random
def scrape_and_save_2(context, master, target_branches, mode, midnight, zip_code=None):
    page = context.new_page()
    url = master['url'] if master['url'].startswith('http') else f'https://{master["url"]}'
    
    # --- DYNAMIC DATE CALCULATIONS ---
    now = datetime.now()
    today_str = now.strftime('%Y-%m-%d')
    # Calculate 90 days from whatever today happens to be
    ninety_days_out = (now + timedelta(days=90)).strftime('%Y-%m-%d')
    
    try:
        print(f"📡 Scoping: {master['name']} (Today: {today_str})")
        
        # 🛡️ Universal Stealth Headers
        page.set_extra_http_headers({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Referer": "https://www.google.com/"
        })

        # Navigate - use 'load' state to ensure scripts run
        page.goto(url, wait_until="load", timeout=60000)
        time.sleep(2) 

        # 🖱️ Human-like scroll to trigger lazy-loaded calendars (IDs 2, 3, 4)
        for _ in range(4):
            page.evaluate("window.scrollBy(0, 600)")
            time.sleep(random.uniform(1.0, 1.8))

        # 🧩 Extraction: Capture text and ARIA labels (where dates often hide)
        extracted_text = page.evaluate("""() => {
            const root = document.querySelector('main') || document.body;
            let data = root.innerText;
            // Add aria-labels for accessibility-heavy sites like Exploratorium
            root.querySelectorAll('[aria-label]').forEach(el => data += " " + el.getAttribute('aria-label'));
            return data.replace(/\\s+/g, ' ').substring(0, 18000); 
        }""")

        # 🧠 THE DYNAMIC PROMPT: No hard-coded dates
        prompt = f"""
        Extract ONLY family-friendly special events or exhibits at {master['name']}.
        TODAY'S DATE: {today_str}.
        
        STRICT RULES:
        1. Only extract events between {today_str} and {ninety_days_out}.
        2. Look for explicit dates (e.g., "March 25", "Saturday", "April").
        3. If an exhibit is 'New' or 'Featured' but has no specific date, use {today_str}.
        4. IGNORE: "Museum Hours", "Closed", "Incoming", "General Admission", "Daily".
        5. SNIPPET: Must be a descriptive sentence about the event content.
        6. THEME: Only include events relevant to kids, families, or parenting (e.g., workshops, festivals, storytimes).
        7. EXCLUDE TECH DEMOS: For tech-heavy places (like Apple), IGNORE generic product training like "Get Started", "Photo Walk", or "iPad Basics" UNLESS it is specifically labeled for Kids/Families.
        8. NO DATES IN SNIPPET: Do not repeat the date or time in the snippet field; use it only for the description of the experience.

        FORMAT: Return a JSON LIST of objects:
        [{{"title": "...", "event_date": "YYYY-MM-DD", "snippet": "...", "price_text": "..."}}]
        """
        
        events = generate_with_retry(prompt, extracted_text, master['name'])
        save_events(events or [], target_branches, midnight, master, mode)
            
    except Exception as e:
        print(f"   ❌ Error Scoping ID {master['id']}: {e}")
        # Ensure timestamp updates even on failure
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
        # Using a standard desktop user agent often helps with ID 3 & 5 blocks
        browser = p.chromium.launch(headless=True, args=['--disable-blink-features=AutomationControlled'])
        DESKTOP_UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        context = browser.new_context(user_agent=DESKTOP_UA, viewport={'width': 1920, 'height': 1080})        
        for m in masters:
            # Mark as scraped immediately
            supabase.table("places").update({"last_scraped_at": datetime.now().isoformat()}).eq("id", m['id']).execute()
            
            # Fetch affiliated branches
            branches = supabase.table("places").select("*").eq("parent_id", m['id']).execute().data
            if not branches: continue
            
           # name_low = m['name'].lower().replace("’", "'")
           # category_low = (m.get('category_name') or "").lower() # Ensure this matches your column name
           
           # Use .get() and a fallback "" for BOTH name and category
            name_raw = m.get('name') or "Unknown Place"
            name_low = name_raw.lower().replace("’", "'")
            category_raw = m.get('category_name') or ""
            category_low = category_raw.lower()

            # 1. HYBRID RETAIL (Home Depot/Lowes)
            if any(x in name_low for x in ["home depot", "lowe's", "lowes"]):
                print(f"🛡️ Hybrid: {m['name']}")
                save_events(get_hybrid_retail_events(m['name']), branches, midnight_today, m, "global")

            # 2. SPECIFIC BRANCH SCRAPING (Lego/Barnes/Slime)
            elif any(x in name_low for x in ["lego", "barnes", "slime"]):
                print(f"🔍 Dynamic: {m['name']}")
                if "barnes" in name_low:
                        # B&N requires individual zip code searches
                        for branch in branches:
                            time.sleep(random.uniform(1.5, 3.5))
                            scrape_and_save_1(context, m, [branch], "specific", midnight_today, branch.get('zip_code'))
                    else:
                        # Slime and Lego: Scrape once, map to all branches in one go
                        time.sleep(random.uniform(2.0, 4.0))
                        scrape_and_save_1(context, m, branches, "mapping", midnight_today)
          
            # 3. LIBRARIES
            elif "library" in name_low:
                print(f"📚 Library Mapping: {m['name']}")
                time.sleep(random.uniform(2.0, 4.0))
                scrape_and_save_1(context, m, branches, "mapping", midnight_today)

            # 4. UNIVERSAL / MUSEUM SITES (the non-workshop category)
            else:
                print(f"🌐 Universal/Museum Scrape (Type 2): {m['name']}")
                scrape_and_save_2(context, m, branches, "global", midnight_today)
            
        browser.close()
    # Run the AI discovery for events with missing descriptions
    run_gemini_discovery(midnight_today)
    
if __name__ == "__main__":
    run_scraper()
#if __name__ == "__main__":
#    midnight = datetime.combine(datetime.now().date(), dt_time.min).isoformat()
    
#    print("🚀 STARTING TARGETED TEST FOR IDs 1-5...")
    
    # Force reset so they aren't skipped by the 'last_scraped_at' filter
#    try:
#        supabase.table("places").update({"last_scraped_at": None}).gte("id", 9).lte("id", 185).execute()
#    except Exception as e:
#        print(f"⚠️ Note: Could not reset timestamps: {e}")

    # Fetch IDs 1-5 directly
#    res = supabase.table("places").select("*").gte("id", 9).lte("id", 185).execute()
#    batch = res.data

#    if batch:
#        with sync_playwright() as p:
            # Launch with 'Stealth' mode enabled
#            browser = p.chromium.launch(headless=True, args=['--disable-blink-features=AutomationControlled'])
            
            # Use a Desktop context to ensure the 'Exhibits' layout is full-width
#            context = browser.new_context(
#                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
#            )
            
#            for master in batch:
                # We pass [master] as the 'target_branches' because in your current 
                # setup, the master IS the place we want to save to.
#                scrape_and_save(context, master, [master], "mapping", midnight)
            
#            browser.close()
#    else:
#        print("❌ Could not find IDs 1-5 in the places table.")
