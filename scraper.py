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

# Defining this here fixes the NameError immediately
current_year = datetime.now().year 

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
        # parser.parse defaults to the current year
        parsed_date = parser.parse(date_str, fuzzy=True).date()     
        # 1. Check if it's in the past
        if parsed_date < today:
            # Try adding a year (for the Dec -> Jan rollover)
            future_check = parsed_date.replace(year=today.year + 1)      
            # 2. Only accept the +1 year IF it falls within our 90-day window
            if today <= future_check <= limit:
                parsed_date = future_check
            else:
                # If it's still in the past or way too far in the future, it's junk
                return None
        # Final validation
        if today <= parsed_date <= limit:
            return parsed_date.strftime('%Y-%m-%d')            
        return None
    except:
        return None
        
# --- Business Logic & Saving ---
import re
import json
import time
import random
from datetime import datetime, timedelta, time as dt_time

def save_events(events, target_branches, midnight, master, mode):
    """
    Saves events to Supabase. 
    Restores the PRE-CLEAN session for performance while supporting branch-looping.
    """
    from __main__ import supabase, is_valid_date

    # --- DYNAMIC CONTEXT BLOCK (For Date Handling) ---
    m_id = master['id']
    m_name = master.get('name', 'Unknown')
    today = datetime.now().date()
    # -------------------------------------------------
    
    # 1. ALWAYS Update heartbeat timestamp
    try:
        supabase.table("places").update({"last_scraped_at": datetime.now().isoformat()}).eq("id", m_id).execute()
    except Exception as e:
        print(f"    ⚠️ Heartbeat update failed: {e}")

    if not events:
        print(f"    ℹ️ No events extracted for {m_name}. Skipping save.")
        return

    # 2. TARGETED CLEANUP 
    # Delete existing data ONLY for the branches currently being processed
    branch_ids = [b['id'] for b in target_branches]
    limit_date = today + timedelta(days=90)
    if branch_ids:
        try:
            supabase.table("events").delete() \
                .in_("place_id", branch_ids) \
                .gte("event_date", today.isoformat()) \
                .lte("event_date", limit_date.isoformat()) \
                .execute()
        except Exception as e:
            print(f"    ⚠️ Cleanup error (skipping delete): {e}")

    # 3. SCORE WEIGHTS
    score_weights = [
        (10, ["festival", "fair", "exhibit", "performance", "concert", "parade", "celebration", "expo", "theater", "carnival", "show"]),
        (8, ["storytime", "story time", "lego", "maker", "craft", "lab", "workshop", "play", "science", "art", "steam", "stem", "construction", "diy", "paint", "build"]),
        (4, ["homework", "tutoring", "assistance", "esl", "citizenship", "help", "study", "exam", "test prep", "literacy", "reading buddies"])
    ]

    # 4. RESTORED PRE-CLEAN Session
    noise_pattern = r'library|branch|store|center|museum|main|county|system|[^a-z0-9\s]'
    processed_branches = []
    for b in target_branches:
        b_name = b.get('name', '').lower()
        clean_id = re.sub(noise_pattern, '', b_name).strip()
        processed_branches.append({**b, "clean_identity": clean_id})

    saved_count = 0
    for ev in events:
        if isinstance(ev, list):
            title, r_date, snippet = ev[0], ev[1], ev[2]
            found_loc = "all"
        else:
            title = ev.get('title', 'Special Event')
            r_date = ev.get('event_date', 'UNKNOWN')
            snippet = ev.get('snippet', '')
            found_loc = (ev.get('found_location') or ev.get('found_at') or "all").lower()

        date_str = is_valid_date(r_date)
        if not date_str or r_date == 'UNKNOWN': 
            continue   
        try:
            ev_dt = datetime.strptime(date_str, '%Y-%m-%d').date()
        except:
            continue
            
        # Hallucination & Window Guards
        if ev_dt == today and ("special program" in snippet.lower() or len(snippet) < 22):
            continue
        
        days_away = (ev_dt - today).days
        if days_away < 0 or days_away > 90: continue
        
        window = "Daily Refresh" if days_away <= 14 else "Weekly Deep Dive" if days_away <= 45 else "Special Scout"
      
        # Junk Cleanup
        title_low = title.lower()
        if any(h in title_low for h in ["unable to", "no internet", "access the internet", "no events found", "sorry"]):
            continue
        
        if any(j in title_low for j in ["incoming", "hours", "schedule", "admission", "closed", "private"]):
            continue
        
        # Calculate Score
        clean_text_for_score = re.sub(r'[^a-z0-9\s]', ' ', f"{title_low} {snippet.lower()}")
        clean_text_for_score = " ".join(clean_text_for_score.split())    
        spec_score = 7
        for score_val, keywords in score_weights:
            pattern = r'(^| )(' + '|'.join(map(re.escape, keywords)) + r')( |$)'     
            if re.search(pattern, clean_text_for_score):
                spec_score = score_val
                break
                
        # Identity Engine
        search_blob = f"{title_low} {snippet.lower()} {found_loc}"
        
        for branch in processed_branches:
            should_save = False

            # Scenario A: Specific Mode OR Single Location (Fixes B&N / LEGO / Slime Kitchen)
            # If we are only processing one branch, we don't need to 'match' the name, 
            # because the browser is already looking specifically at that branch's page.
            if mode == "specific" or len(processed_branches) == 1:
                should_save = True
            
            # Scenario B: Global Mode (Multiple branches in one scrape, e.g. Library System)
            else:
                clean_id = branch["clean_identity"]
                
                # Rule 1: Identity match
                if clean_id and clean_id in search_blob:
                    should_save = True
                # Rule 2: System-wide (Home Depot / Slime Kitchen)
                elif any(x in found_loc for x in ["all", "system", "multiple", "various", "in-store"]):
                    is_hybrid = any(h in m_name.lower() for h in ["home depot", "lowe", "slime kitchen", "lego"])
                    if is_hybrid or spec_score >= 10:
                        should_save = True

            if should_save:
                entry = {
                    'place_id': branch['id'],
                    'place_name': branch.get('name', m_name),
                    'title': title,
                    'event_date': date_str,
                    'snippet': snippet,
                    'category_name': master.get('category_name') or 'Special Activity',
                    'zip_code': branch.get('zip_code'),
                    'window_type': window,
                    'specificity_score': spec_score,
                }
                
                try:
                    # Final Deduplication
                    existing = supabase.table("events").select("id") \
                        .eq("place_id", branch['id']) \
                        .eq("title", title) \
                        .eq("event_date", date_str) \
                        .execute()
                    
                    if not existing.data:
                        supabase.table("events").insert(entry).execute()
                        saved_count += 1
                except Exception as e: 
                    print(f"      ⚠️ Database insert error: {e}")

    if saved_count > 0:
        print(f"    ✅ Saved {saved_count} entries for {m_name}")

import time
import json
import re
def generate_with_retry(prompt, text_content, context_name="General"):
    """
    Complete AI response handler. 
    Combines your original trailing-comma fix with a new salvage step 
    to handle truncated text (e.g., 'Welcome spr...').
    """
    for attempt in range(3):                
        try:
            # Linear backoff to respect API limits
            time.sleep(2 + attempt) 
            
            response = client.models.generate_content(
                model='gemini-2.0-flash', 
                contents=[prompt, text_content[:30000]]
            )
            
            if not response or not hasattr(response, 'text') or not response.text:
                print(f"    ⚠️ AI returned empty for {context_name}")
                continue
                
            res_text = response.text.strip()

            # --- LAYER 1: POSITIONAL EXTRACTION & TRUNCATION SALVAGE ---
            start_idx = res_text.find('[')
            end_idx = res_text.rfind(']') + 1
            
            if start_idx != -1:
                # If no closing bracket found, take everything from the start to end of string
                raw_json = res_text[start_idx:end_idx] if end_idx > start_idx else res_text[start_idx:]
                
                try:
                    # 1. Try standard parse
                    return json.loads(raw_json)
                except json.JSONDecodeError:
                    print(f"    🔧 Attempting advanced repair for {context_name}...")
                    
                    # 2. EMERGENCY SALVAGE: Find the last completed event '}'
                    # This recovers events if the AI cut off mid-sentence (e.g. "Welcome spr...")
                    last_brace = raw_json.rfind('}')
                    if last_brace != -1:
                        salvaged_json = raw_json[:last_brace + 1]
                        try:
                            # Try closing the array right after the last good object
                            return json.loads(salvaged_json + "]")
                        except:
                            pass
                    
                    # 3. YOUR ORIGINAL REPAIR LOGIC: Clean up trailing commas/half-written properties
                    clean_json = re.sub(r'\},[^\}]*$', '}', raw_json)    
                    
                    # 4. Try your original common closing sequences
                    for fix in [']', '}]', '"}]', '"}]}']:
                        try:
                            return json.loads(clean_json + fix)
                        except:
                            continue

            # --- LAYER 2: MARKDOWN BLOCK EXTRACTION (Fallback) ---
            # Finds content inside ```json ... ``` using safe bracket notation
            blocks = re.findall(r'[`]{3}(?:json)?\s*(.*?)\s*[`]{3}', res_text, re.DOTALL)
            for block in blocks:
                try:
                    return json.loads(block.strip())
                except:
                    continue

            # --- LAYER 3: AGGRESSIVE RAW STRIP (Last Resort) ---
            # Removes backticks and tries to parse the whole string
            final_attempt = re.sub(r'[`]{3}json\s*|[`]{3}', '', res_text).strip()
            try:
                return json.loads(final_attempt)
            except:
                pass

            print(f"    ⚠️ All extraction layers failed for {context_name}")
        
        except Exception as e:
            err_msg = str(e).lower()
            if "429" in err_msg:
                wait = (attempt + 1) * 30
                print(f"    ⏳ Rate limited. Sleeping {wait}s...")
                time.sleep(wait)
            else:
                print(f"    ❌ AI Error for {context_name}: {e}")
                time.sleep(5)

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
import re
import json
import time
from datetime import datetime, timedelta
def scrape_and_save_1(context, master, target_branches, mode, midnight, zip_code=None):
    m_name = master.get('name', 'Unknown')
    is_library = "library" in m_name.lower()
    is_bookstore = "barnes" in m_name.lower()
    is_lego = "lego" in m_name.lower()

    url = master['url'] if master['url'].startswith('http') else f'https://{master["url"]}' 
    today = datetime.now()
    future_date = today + timedelta(days=90)
    current_year = today.year
    range_str = f"{today.strftime('%B %d, %Y')} to {future_date.strftime('%B %d, %Y')}"

    branches_to_process = target_branches if (mode == "specific" and (is_bookstore or is_lego)) else [None]

    for current_branch in branches_to_process:
        page = context.new_page()
        page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
      
        active_zip = current_branch.get('zip_code') if current_branch else zip_code
        active_branch_name = current_branch.get('name', 'Main') if current_branch else "System"
 
        current_url = master['url'] if master['url'].startswith('http') else f'https://{master["url"]}' 
        if is_library:
            if not any(word in current_url.lower() for word in ["calendar", "events", "biblio", "program"]):
                current_url = current_url.rstrip('/') + "/events"
                print(f"    🚀 LIBRARY STRATEGY: Appending /events to path for {active_branch_name}")
                
        if is_bookstore:
            BN_ID_MAP = {
                "san mateo": "2306", "redwood city": "2265", "emeryville": "2934",
                "walnut creek": "2269", "dublin": "2202", "concord": "2323",
                "fairfield": "2322", "corte madera": "2274", "san jose": "2088"
            }
            store_id = current_branch.get('external_id') if current_branch else None
            if not store_id:
                clean_name = active_branch_name.lower().replace("barnes & noble", "").strip()
                for city_key, cid in BN_ID_MAP.items():
                    if city_key in clean_name:
                        store_id = cid
                        break

            if store_id:
                current_url = f"https://stores.barnesandnoble.com/store/{store_id}?view=list&type=event"
                print(f"    🚀 STRATEGY: Direct ID Injection for {active_branch_name} (ID: {store_id})")
            elif active_zip:
                current_url = f"https://stores.barnesandnoble.com/search?searchTerm={active_zip}&view=list"
                print(f"    🚀 STRATEGY: Search Parameter Injection for {active_zip}")
      
        try:
            page.set_extra_http_headers({
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Upgrade-Insecure-Requests": "1"
            }) 
                    
            print(f"    🌐 Navigating to {m_name} ({active_branch_name})...")
            # FIX: Reverted to 'networkidle' for dynamic sites (Libraries/B&N)
            wait_type = "networkidle" if (is_library or is_bookstore) else "domcontentloaded"
            page.goto(current_url, wait_until=wait_type, timeout=90000)
            page.wait_for_timeout(6000) # FIX: Increased back to 6s for JS hydration
            
            # Modal Handling
            if is_bookstore:
                try:
                    close_btn = page.locator("button[aria-label*='Close' i], .modal-close, #close-icon, button:has-text('No Thanks'), .bx-close-x-adaptive").first
                    if close_btn.is_visible(timeout=3000):
                        close_btn.click()
                        print("    ✨ Closed overlay/modal.")
                        page.wait_for_timeout(1000) 
                except: pass
            
            # Branch Selection & Store Interaction Logic
            if (is_bookstore or is_lego) and active_zip:
                try:
                    if is_bookstore:
                        page.evaluate("""() => {
                            const eventTab = document.querySelector('a[href*="type=event"], #store-details-events-tab');
                            if (eventTab) eventTab.click();
                        }""")
                        
                        if "type=event" in page.url:
                            print(f"    ✨ Already at destination via Direct Injection.")
                            print(f"    🖱️ Scrolling to trigger B&N event loading...")
                            for _ in range(5): # FIX: Increased back to 5 for lazy load
                                page.mouse.wheel(0, 1000)
                                page.wait_for_timeout(1000)
                        else:
                            print(f"    🖱️ Locating 'Store Events' for {active_branch_name}...")
                            clean_branch = active_branch_name.split('-')[-1].strip()
                            branch_card = page.locator(f"div.store-info-container, .store-card-details").filter(has_text=re.compile(clean_branch, re.I)).first
                            
                            if branch_card.is_visible(timeout=5000):
                                events_link = branch_card.locator("a:has-text('Store Events'), a[href*='/events/']").first
                                events_link.scroll_into_view_if_needed()
                                if events_link.is_visible():
                                    events_link.click(force=True)
                                    page.wait_for_timeout(6000) 
                                    for _ in range(5): # FIX: Increased back to 5
                                        page.mouse.wheel(0, 1000)
                                        page.wait_for_timeout(1000)

                    elif is_lego:
                        print(f"    🖱️ Selecting LEGO Store for {active_branch_name} (ZIP: {active_zip})...")
                        search_field = page.locator("input[placeholder*='zip' i], input[placeholder*='City' i], #store-search-input").first
                        if search_field.is_visible(timeout=5000):
                            search_field.fill(str(active_zip))
                            page.keyboard.press("Enter")
                            time.sleep(3)
                            select_btn = page.locator("text='Select This Store', button:has-text('Store Details'), .store-card button").first
                            if select_btn.is_visible(timeout=5000):
                                select_btn.click(force=True)
                                time.sleep(5)
                except Exception as e:
                    print(f"    ⚠️ Store selection failed for {active_branch_name}: {e}")

            # Global Scrolling & Pagination
            print(f"    🖱️ Scrolling {m_name}...")
            scroll_count = 8 if (is_library or is_bookstore) else 3 # FIX: Context-aware scrolling
            for _ in range(scroll_count):
                page.mouse.wheel(0, 2000)
                page.wait_for_timeout(1500)
            
            if is_library:
                print(f"    🖱️ LIBRARY STRATEGY: Deep Pagination...")
                for p_idx in range(4): # FIX: Increased back to 4 for more events
                    next_btn = page.locator("button[aria-label*='Next' i], .pagination-next, a:has-text('Next')").first
                    if next_btn.is_visible(timeout=3000):
                        next_btn.click()
                        page.wait_for_timeout(4000)
                        page.mouse.wheel(0, 2000)
                    else: break

                for i in range(5): # FIX: Increased back to 5
                    try:
                        load_more = page.get_by_role("button", name=re.compile(r"load more|view more|show more", re.I))
                        if load_more.is_visible(timeout=2000):
                            load_more.click()
                            page.wait_for_timeout(3000)
                        else: break
                    except: break

            page.wait_for_timeout(2000) 

            kids_tags = [
                "Babies & Toddler", "Kids", "Teens", "Preschoolers", "Teens (13 to 18 years)", 
                "Children (6 to 9 years)", "Preschoolers (3-5 years)", "Tweens (9 to 12 years)", 
                "Toddlers (1 to 3 years)", "Kids (5-9 yrs)", "Babies (0-1 yrs)", "Families", 
                "Preschool", "Family Friendly", "School Age", "Baby/Toddler", "Early Childhood", 
                "Elementary School Age", "Family", "Middle School Age", "Teen", "Children"
            ]

            events = None 
            if is_bookstore:
                print(f"    🧪 Attempting Direct JSON Extraction (B&N Native Data)...")
                events = page.evaluate("""() => {
                    const nextData = document.getElementById('__NEXT_DATA__');
                    if (!nextData) return null;
                    try {
                        const json = JSON.parse(nextData.textContent);
                        const store = json.props?.pageProps?.storeDetails;
                        if (!store || !store.events) return null;
                        return store.events.map(e => ({
                            title: e.title,
                            event_date: new Date(e.date).toISOString().split('T')[0],
                            snippet: (e.description || "Store event").substring(0, 150),
                            found_location: store.name
                        }));
                    } catch (err) { return null; }
                }""")

            combined_text = "" 
            if not events:
                def perform_dom_capture():
                    if is_library:
                        return page.evaluate("""(tagList) => {
                            const cardSelectors = ['.cp-event-item', '.biblio-item', '.event-item', '.cp-events-item', 'article', '.cp-event-list-item', '.event-card'];
                            let eventData = [];
                            let seenTitles = new Set();
                            function getDeepContent(root) {
                                let cards = Array.from(root.querySelectorAll(cardSelectors.join(',')));
                                root.querySelectorAll('*').forEach(el => {
                                    if (el.shadowRoot) cards = cards.concat(getDeepContent(el.shadowRoot));
                                });
                                return cards;
                            }
                            const allCards = getDeepContent(document);
                            allCards.forEach(card => {
                                const titleEl = card.querySelector('h2, h3, .title, .cp-event-title, [class*="title"]');
                                if (!titleEl) return;
                                const title = titleEl.innerText.trim();
                                const tagEls = card.querySelectorAll('span, .cp-screen-reader-message, [class*="audience"], .tags, .cp-event-item-metadata, .event-details');
                                let tagContext = "";
                                tagEls.forEach(s => tagContext += " " + s.innerText);
                                const isLikelyKids = tagList.some(tag => (tagContext + title).toLowerCase().includes(tag.toLowerCase()));
                                const uniqueKey = title + card.innerText.substring(0,30);
                                if (!seenTitles.has(uniqueKey)) {
                                    seenTitles.add(uniqueKey);
                                    const label = isLikelyKids ? "[KIDS_PROBABLE]" : "[GENERAL]";
                                    eventData.push(`${label} TITLE: ${title}\\nTAGS: ${tagContext}\\nTEXT: ${card.innerText.replace(/\\s\\s+/g, ' ').substring(0, 800)}`);
                                }
                            });
                            return eventData.slice(0, 60).join('\\n---\\n').substring(0, 18000);
                        }""", kids_tags)
                    else:
                        return page.evaluate("""() => {
                            function getDeepText(node) {
                                let text = "";
                                if (node.nodeType === Node.TEXT_NODE) text += node.textContent + " ";
                                else if (node.shadowRoot) text += getDeepText(node.shadowRoot);
                                else if (node.childNodes) node.childNodes.forEach(child => { text += getDeepText(child); });
                                return text;
                            }
                            const elements = document.querySelectorAll('h1, h2, h3, h4, a, span, p, li, [class*="event"], [class*="card"]');
                            let results = [];
                            elements.forEach(el => {
                                const content = getDeepText(el).trim();
                                if (content.length > 10) results.push(content);
                            });
                            return results.join('\\n').substring(0, 15000);
                        }""")

                print(f"    ⚠️ No JSON events found. Starting Comprehensive DOM Capture...")
                combined_text = perform_dom_capture()

                if is_bookstore and len(combined_text.strip()) < 200:
                    print("    🔄 B&N RECOVERY: Retrying with deep wait...")
                    page.reload(wait_until="networkidle") # FIX: Harder reload for B&N
                    page.wait_for_timeout(10000) 
                    for _ in range(5):
                        page.mouse.wheel(0, 1000)
                        page.wait_for_timeout(1000)
                    combined_text = perform_dom_capture()
                
                # FIX: Lowered threshold from 400 back to 100 to capture legitimate but small results
                if combined_text and len(combined_text.strip()) > 100:
                    print(f"    🤖 Sending {len(combined_text)} chars to AI for parsing...")
                    library_exclusion_rule = "11. LIBRARY EXCLUSION: If the SAME event title happens 3 or more times within a single week at the same location, EXCLUDE it." if is_library else ""
                    prompt = f"""
                    Extract events at {master['name']} ({active_branch_name}) from {today.strftime('%B %d, %Y')} to {future_date.strftime('%B %d, %Y')}.
                    Rules: 
                    1. Return ONLY a JSON list of objects: [{{"title": "...", "event_date": "YYYY-MM-DD", "snippet": "...", "found_location": "..."}}].
                    2. DATE RULE: Calculate recurrences for the next 90 days. IMPORTANT: If a date is found without a year (e.g., "Oct 12"), assume the year is {current_year}.
                    3. Snippet: 1 sentence, under 20 words.
                    4. If no events found, return [].
                    5. IDENTITY: Identify the specific branch/city. Context: {active_branch_name}. 
                    6. LOCATION: MUST set "found_location" to "{active_branch_name}" for every event.
                    7. EXCLUDE: Adult-only programming.
                    8. B&N SPECIAL: Look for Storytime, Book Clubs, or Author events.
                    9. MAX EVENTS: Up to 25. Ensure JSON is valid and closed.
                    10. RECURRING: For daily events, only provide TWO entries per week (Saturdays and Sundays).
                    {library_exclusion_rule}
                    """   
                    events = generate_with_retry(prompt, combined_text, f"{m_name}-{active_branch_name}")        
                else:
                    print(f"    ⚠️ Skipping AI: Content too thin ({len(combined_text or '')} chars).")
            
            if events:
                exclude_list = ["adult", "senior", "tax prep", "citizenship test"]
                filtered_events = [
                    ev for ev in events 
                    if not any(word in (ev.get('title', '') + " " + ev.get('snippet', '')).lower() for word in exclude_list) 
                    or "family" in (ev.get('title', '') + " " + ev.get('snippet', '')).lower()
                ]
                
                if filtered_events:
                    targets = [current_branch] if current_branch else target_branches
                    save_events(filtered_events, targets, midnight, master, mode)
                    print(f"    ✅ Successfully found {len(filtered_events)} events for {active_branch_name}.")
            else:
                print(f"    ⚠️ No events extracted for {active_branch_name}. Skipping save.")

            if current_branch != branches_to_process[-1]:
                print(f"    ⏳ Spacing out requests (10s)...")
                time.sleep(10) # FIX: Increased back to 10s to avoid bot detection

        except Exception as e:
            print(f"❌ Error scraping {m_name} - {active_branch_name}: {e}")
        finally:
            page.close()

import random
def scrape_and_save_2(context, master, target_branches, mode, midnight, zip_code=None):
    # --- 1. INITIALIZE IMMEDIATELY ---
    # This is the most important line. It prevents the 'unbound' crash.
    clean_events = [] 
    page = None
    
    try:
        page = context.new_page()
        url = master['url'] if master['url'].startswith('http') else f'https://{master["url"]}'
        
        # --- 2. DYNAMIC DATE CALCULATIONS ---
        now = datetime.now()
        today_str = now.strftime('%Y-%m-%d')
        ninety_days_out = (now + timedelta(days=90)).strftime('%Y-%m-%d')
        
        print(f"📡 Scoping: {master['name']} (Today: {today_str})")
        
        # 🛡️ Universal Stealth Headers
        page.set_extra_http_headers({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Referer": "https://www.google.com/"
        })

        # Navigate
        page.goto(url, wait_until="load", timeout=60000)
        time.sleep(2) 

        # 🖱️ Human-like scroll
        for _ in range(4):
            page.evaluate("window.scrollBy(0, 600)")
            time.sleep(random.uniform(1.0, 1.8))

        # 🧩 Extraction
        extracted_text = page.evaluate("""() => {
            const root = document.querySelector('main') || document.body;
            let data = root.innerText;
            root.querySelectorAll('[aria-label]').forEach(el => data += " " + el.getAttribute('aria-label'));
            return data.replace(/\\s+/g, ' ').substring(0, 18000); 
        }""")

        # 🧠 THE DYNAMIC PROMPT
        prompt = f"""
        Extract ONLY family-friendly special events or exhibits at {master['name']}.
        TODAY'S DATE: {today_str}.
        
        STRICT RULES:
        1. Only extract events between {today_str} and {ninety_days_out}.
        2. Look for explicit dates (e.g., "March 25", "Saturday", "April").
        3. If an exhibit is 'New' or 'Featured' but has no specific date, use {today_str}.
        4. IGNORE: "Museum Hours", "Closed", "Incoming", "General Admission", "Daily".
        5. SNIPPET: Must be a descriptive sentence about the event content under 20 words.
        6. THEME: Only include events relevant to kids, families, or parenting.
        7. EXCLUDE TECH DEMOS: IGNORE generic product training unless specifically for Kids/Families.
        8. NO DATES IN SNIPPET: Use snippet only for the description.
        9. LIMIT: Extract up to 30 events.
        FORMAT: Return a JSON LIST of objects:
        [{{"title": "...", "event_date": "YYYY-MM-DD", "snippet": "...", "price_text": "...", "found_location": "..."}}]
        """
        
        # --- 3. CALL AI ---
        # We assign the result to the variable we defined at the top
        clean_events = generate_with_retry(prompt, extracted_text, master['name'])
        
        # --- 4. SAVE ---
        save_events(clean_events or [], target_branches, midnight, master, mode)
            
    except Exception as e:
        print(f"    ❌ Error Scoping ID {master['id']}: {e}")
        # Mark as attempted in database even if it failed
        try:
            supabase.table("places").update({"last_scraped_at": datetime.now().isoformat()}).eq("id", master['id']).execute()
        except: 
            pass
    finally:
        # --- 5. CLEANUP ---
        if page:
            page.close()
            
    return clean_events

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
    """
    Main orchestration function for the scraping pipeline.
    """
    from __main__ import supabase, get_daily_batch, run_gemini_discovery, scrape_and_save_1, scrape_and_save_2, get_hybrid_retail_events
    from playwright.sync_api import sync_playwright

    midnight_today = datetime.combine(datetime.now().date(), dt_time.min).isoformat()
    masters = get_daily_batch(limit=24)
    if not masters: return

    with sync_playwright() as p:
        # Using a standard desktop user agent often helps with block detection
        browser = p.chromium.launch(headless=True, args=['--disable-blink-features=AutomationControlled'])
        DESKTOP_UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        context = browser.new_context(user_agent=DESKTOP_UA, viewport={'width': 1920, 'height': 1080})        
        
        for m in masters:
            # Fetch affiliated branches
            branches = supabase.table("places").select("*").eq("parent_id", m['id']).execute().data
            if not branches:
                branches = [m] # Essential for single-location sites
            
            name_raw = m.get('name') or "Unknown Place"
            name_low = name_raw.lower().replace("’", "'")

            # 1. HYBRID RETAIL (Home Depot/Lowes)
            is_hd = "home depot" in name_low
            is_lowes = "lowe's" in name_low or "lowes" in name_low
            if is_hd or is_lowes:
                print(f"🛡️ Hybrid: {m['name']}")
                save_events(get_hybrid_retail_events(m['name']), branches, midnight_today, m, "global")
                continue 
    
            # 2. SPECIFIC BRANCH SCRAPING (Lego/Barnes/Slime)
            elif any(x in name_low for x in ["lego", "barnes", "slime"]):
                print(f"🔍 Dynamic: {m['name']}")
                if "barnes" in name_low:
                    for branch in branches:
                        time.sleep(random.uniform(2.0, 4.0))
                        scrape_and_save_1(context, m, [branch], "specific", midnight_today, branch.get('zip_code'))
                else:
                    time.sleep(random.uniform(3.0, 5.0))
                    scrape_and_save_1(context, m, branches, "mapping", midnight_today)
            
            # 3. LIBRARIES
            elif "library" in name_low:
                print(f"📚 Library Mapping: {m['name']}")
                time.sleep(random.uniform(2.0, 4.0))
                scrape_and_save_1(context, m, branches, "mapping", midnight_today)

            # 4. UNIVERSAL / MUSEUM SITES
            else:
                print(f"🌐 Universal/Museum Scrape (Type 2): {m['name']}")
                scrape_and_save_2(context, m, branches, "global", midnight_today)
            
        browser.close()

    # Run AI discovery for events with missing descriptions
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
