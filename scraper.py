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
    """Normalizes any date string into YYYY-MM-DD."""
    if not date_str or not isinstance(date_str, str):
        return None
    try:
        parsed_date = parser.parse(date_str)
        if parsed_date.year < 2024: return None
        return parsed_date.strftime('%Y-%m-%d')
    except (ValueError, TypeError):
        return None

# --- Business Logic & Saving ---
def save_events(events, target_branches, midnight, master, mode):
    if not events:
        print(f"   ⚠️ No events found to save for {master.get('name')}")
        try:
            supabase.table("places").update({"last_scraped_at": datetime.now().isoformat()}).eq("id", master['id']).execute()
        except: pass
        return

    official_cat = master.get('category', 'Special Activity')
    m_id = master['id']
    m_name = master.get('name', 'Unknown Place')

    print(f"   💾 Attempting to save {len(events)} events for {m_name}...")

    # 1. CLEANUP: Remove old records to prevent duplicates
    try:
        supabase.table("events").delete().eq("place_id", m_id).gte("event_date", midnight).execute()
    except Exception as e:
        print(f"   ❌ Cleanup failed: {e}")

    for ev in events:
        # 2. DATE NORMALIZATION
        clean_date = is_valid_date(ev.get('event_date'))
        if not clean_date:
            # Fallback for permanent museum exhibits
            clean_date = datetime.now().strftime('%Y-%m-01') 
            print(f"      ℹ️ Defaulted date for '{ev.get('title')}' to {clean_date}")

        for branch in target_branches:
            # 3. THE MUSEUM BYPASS (Trust the source for single-branch places)
            is_single_place = (len(target_branches) == 1)
            
            should_add = False
            if is_single_place or mode != "mapping":
                should_add = True
            else:
                loc_hint = str(ev.get('found_location', '')).lower()
                b_name = branch['name'].lower().replace("library", "").strip()
                if b_name in loc_hint or b_name in ev['title'].lower():
                    should_add = True

            if should_add:
                # 4. PREPARE ENTRY (No is_featured column)
                entry = {
                    'place_id': branch['id'],
                    'title': ev.get('title'),
                    'event_date': clean_date,
                    'snippet': ev.get('snippet', ''),
                    'price_text': ev.get('price_text', 'Check website'),
                    'category_name': official_cat,
                    'zip_code': branch.get('zip_code')
                }

                # 5. INSERT
                try:
                    res = supabase.table("events").insert(entry).execute()
                    if res.data:
                        print(f"      ✅ SUCCESS: Saved '{entry['title']}'")
                except Exception as e:
                    print(f"      ❌ DB INSERT ERROR: {e}")

    # 6. UPDATE PLACE TIMESTAMP
    try:
        supabase.table("places").update({"last_scraped_at": datetime.now().isoformat()}).eq("id", m_id).execute()
        print(f"   🕒 Place {m_id} timestamp updated.")
    except Exception as e:
        print(f"   ❌ Failed to update place timestamp: {e}")
        
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
    
    try:
        print(f"📡 Scoping ID {master['id']}: {master['name']}")
        
        # 🛡️ STEALTH: Pretend we are coming from Google
        # This is the "Magic Fix" for 405 Errors
        page.set_extra_http_headers({
            "Referer": "https://www.google.com/",
            "Accept-Language": "en-US,en;q=0.9"
        })

        # Navigate with a longer timeout for heavy museum sites
        response = page.goto(url, wait_until="networkidle", timeout=60000)
        
        if response.status == 405 or response.status == 403:
            print(f"   ⚠️ Site blocked direct access ({response.status}). Trying deeper stealth...")
            # If blocked, we try one more time with a random human delay
            time.sleep(random.randint(5, 10))
            page.goto(url, wait_until="domcontentloaded")

        # 🖱️ Aggressive trigger for Palo Alto / CalAcademy dynamic cards
        page.mouse.wheel(0, 1500)
        time.sleep(4) 
        
        # Capture VISIBLE text + ALT text from images (where museum titles hide)
        raw_data = page.evaluate("""() => {
            let text = document.body.innerText;
            // Add image alts which often contain event names in galleries
            document.querySelectorAll('img').forEach(img => {
                if(img.alt) text += " | Image: " + img.alt;
            });
            return text;
        }""")

        today = datetime.now()
        prompt = f"""
        Find all events and exhibits for {master['name']} in the text.
        Date Range: March 2026 to June 2026.
        If it is a permanent exhibit with no date, use "{today.strftime('%Y-%m-01')}".
        Format as JSON list: ["title", "event_date", "snippet", "price_text"].
        """
        
        events = generate_with_retry(prompt, raw_data, master['name'])
        save_events(events or [], target_branches, midnight, master, mode)
        
    except Exception as e:
        print(f"   ❌ Scrape Logic Crash for ID {master['id']}: {e}")
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
