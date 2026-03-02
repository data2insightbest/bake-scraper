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

def is_valid_date(date_str):
    return bool(re.match(r'^\d{4}-\d{2}-\d{2}$', str(date_str)))

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
        .in_("id", [1, 2, 3, 4, 5])\
        .order("id")\
        .limit(limit)\
        .execute()
    return res.data
     
# --- Business Logic & Saving ---

def save_events(events, target_branches, midnight, master, mode):
    if not events: return
    
    official_cat = master.get('category', 'Activity') if isinstance(master, dict) else master
    b_ids = [int(b['id']) for b in target_branches]
    
    # Pre-clean existing records
    supabase.table("events").delete().in_("place_id", b_ids).gte("event_date", midnight).execute()

    # 1. TRACE: Frequency Calculation
    # We count occurrences of each title to find 'Common' vs 'Rare' events
    title_counts = {}
    for ev in events:
        t = ev['title'].lower()
        title_counts[t] = title_counts.get(t, 0) + 1

    special_keywords = ["festival", "annual", "holiday", "celebration", "fair", "parade"]

    for ev in events:
        if not is_valid_date(ev.get('event_date')): continue
        
        title_low = ev['title'].lower()
        count = title_counts.get(title_low, 1)
        
        # 2. TRACE: Implement the Specificity Score
        # RARE titles (count=1) get high scores. 
        # COMMON titles (count > 2) get low scores.
        if count == 1:
            # It's unique in this batch!
            spec_score = random.randint(8, 10)
        elif count == 2:
            # Semi-unique (maybe 2 branches have it)
            spec_score = random.randint(5, 7)
        else:
            # Common/Recurring (Home Depot, Storytimes, etc.)
            spec_score = random.randint(1, 4)

        # 3. TRACE: Keyword Override
        # Even if it's common, if it's a 'Festival', boost it.
        if any(kw in title_low for kw in special_keywords):
            spec_score = min(10, spec_score + 5)

        window = calculate_window(ev['event_date'])

        for branch in target_branches:
            should_add = (mode in ["global", "specific"])
            if mode == "mapping":
                loc_hint = str(ev.get('found_location', '')).lower()
                b_clean = branch['name'].lower().replace("library", "").strip()
                if b_clean and (b_clean in loc_hint or b_clean in title_low):
                    should_add = True
            
            if should_add:
                entry = ev.copy()
                entry.pop('found_location', None)
                entry.update({
                    'category_name': official_cat,
                    'specificity_score': spec_score,
                    'window_type': window,
                    'place_id': branch['id'], 
                    'place_name': branch['name'], 
                    'zip_code': branch['zip_code']
                })
                supabase.table("events").insert(entry).execute()
                
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
    
    today = datetime.now()
    future_date = today + timedelta(days=90)
    range_str = f"{today.strftime('%B %d, %Y')} to {future_date.strftime('%B %d, %Y')}"
    
    try:
        # 1. Wait for network to settle (Universal for SPAs and Museums)
        print(f"📡 Navigating to: {url}")
        page.goto(url, wait_until="networkidle", timeout=90000)
        
        # 2. Universal Human-Mimic Scroll
        # This triggers 'Featured' cards in museums AND 'Load More' in workshops
        for _ in range(10):
            page.mouse.wheel(0, 500)
            time.sleep(1.0) 

        # 3. Deep-Scan Technology
        # Pierces through Shadow DOM (Museums) and collects Iframe text (Workshop widgets)
        raw_content = page.evaluate("""() => {
            let allText = document.body.innerText;
            // Scan for Shadow Roots (Common in modern React/Vue museum sites)
            const hosts = Array.from(document.querySelectorAll('*')).filter(el => el.shadowRoot);
            hosts.forEach(h => { allText += '\\n' + h.shadowRoot.textContent; });
            return allText;
        }""")
        
        for frame in page.frames:
            try:
                f_text = frame.evaluate("document.body.innerText")
                if len(f_text) > 50: raw_content += f"\\n---\\n{f_text}"
            except: continue

        page.screenshot(path=f"debug_{re.sub(r'\W+', '', master['name'])}.png")

        # 4. Global Intelligence Prompt
        # Instructs Gemini to look for BOTH formal workshops and 'Featured' museum events
        prompt = f"""
        Today is {today.strftime('%B %d, %Y')}. 
        Find ALL public activities for {master['name']} between {range_str}.
        Look for: 'Workshops', 'Featured Events', 'Exhibits', 'Daily Programs', or 'Festivals'.
        Output JSON list: ["title", "event_date", "category_name", "window_type", "price_text", "snippet", "found_location"].
        Rules: Use year 2026. Return [] if none found.
        """
        
        events = generate_with_retry(prompt, raw_content, master['name'])

        if events:
            save_events(events, target_branches, midnight, master, mode)
            print(f"   ✅ Saved {len(events)} events for {master['name']}.")
        else:
            print(f"   ⚠️ Gemini found 0 events for {master['name']}. Check screenshot.")
            
    except Exception as e:
        print(f"❌ Error scraping {master['name']}: {e}")
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

if __name__ == "__main__":
    run_scraper()
