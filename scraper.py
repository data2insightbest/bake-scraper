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
        "2026-05-02": "Kids Workshop",
        "2026-06-06": "Kids Workshop: Goalie Game",
        "2026-07-04": "Kids Workshop: Go Kart",
        "2026-08-01": "Kids Workshop: Rocket Game",
        "2026-09-05": "Kids Workshop: School Bus Organizer",
        "2026-10-03": "Kids Workshop: Witch Candy Box",
        "2026-11-07": "Kids Workshop: Dump Truck",
        "2026-12-05": "Kids Workshop: Holiday Train"
    }
    "lowe's": {
        "2026-05-16": "Lowe's Kids Club: Garden Basket",
        "2026-06-13": "Lowe's Kids Club: Trophy Cup",
        "2026-07-18": "Lowe's Kids Club: Mini Toy Box",
        "2026-08-15": "Lowe's Kids Club: Paw Patrol: The Dino Movie Workshop",
        "2026-09-12": "Lowe's Kids Club: Haunted House",
        "2026-10-17": "Lowe's Kids Club: Firefighting Plane",
        "2026-11-14": "Lowe's Kids Club: Holiday Engine",
        "2026-12-12": "Lowe's Kids Club: Holiday Trolley Car"
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
from datetime import datetime, timedelta
def save_events(events, target_branches, midnight, master, mode):
    m_id = master['id']
    m_name = master.get('name', 'Unknown')
    today = datetime.now().date()
    
    # 1. ALWAYS Update heartbeat timestamp
    try:
        supabase.table("places").update({"last_scraped_at": datetime.now().isoformat()}).eq("id", m_id).execute()
    except Exception as e:
        print(f"    ⚠️ Heartbeat update failed: {e}")

    if not events:
        print(f"    ℹ️ No events extracted for {m_name}. Skipping save.")
        return

    # Delete existing data for the 90-day future window for these specific branches
    branch_ids = [b['id'] for b in target_branches]
    limit_date = today + timedelta(days=90)
    if branch_ids:
        print(f"    🧹 Refreshing 90-day window for {len(branch_ids)} branches of {m_name}...")
        try:
            # Wrapped in try/except to prevent network timeouts from killing the whole script
            supabase.table("events").delete() \
                .in_("place_id", branch_ids) \
                .gte("event_date", today.isoformat()) \
                .lte("event_date", limit_date.isoformat()) \
                .execute()
        except Exception as e:
            print(f"    ⚠️ Cleanup error (skipping delete): {e}")
    # --- FLEXIBLE SCORING MAP ---
    # Centralized weights for easy updates - No longer hardcoded in IF statements
    score_weights = [
        (10, ["festival", "fair", "exhibit", "performance", "concert", "parade", "celebration", "expo", "theater", "carnival", "show"]),
        (8, ["storytime", "story time", "lego", "maker", "craft", "lab", "workshop", "play", "science", "art", "steam", "stem", "construction", "diy", "paint", "build"]),
        (4, ["homework", "tutoring", "assistance", "esl", "citizenship", "help", "study", "exam", "test prep", "literacy", "reading buddies"])
    ]
    # 4. PRE-CLEAN Branch Identities for faster matching
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

        # Use our improved is_valid_date (no hard-coded 2024)        
        date_str = is_valid_date(r_date)
        if not date_str or r_date == 'UNKNOWN': 
            continue   
        try:
            ev_dt = datetime.strptime(date_str, '%Y-%m-%d').date()
        except:
            continue
            
        # --- IMPROVED HALLUCINATION FILTER ---
        if ev_dt == today:
            # If the snippet is generic or too short, it's likely a hallucination
            # Otherwise, if it's a detailed description, keep it!
            if "special program" in snippet.lower() or len(snippet) < 22:
                continue
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
        snippet_low = snippet.lower()
         # 1. Skip AI Refusals / "No Internet" hallucinations
        if any(h in title_low for h in ["unable to", "no internet", "access the internet", "no events found", "sorry"]):
            continue
            
        # 2. Skip Logistical/Commercial Junk (Restored from your previous version)
        junk_keywords = [
            "incoming", "hours", "schedule", "admission", "closed", "private", 
            "get started", "basics", "iphone", "ipad", "mac", "skills", 
            "photo walk", "video walk"
        ]
        if any(j in title_low for j in junk_keywords):
            continue
        # 90-day Limit Guard
        if days_away < 0 or days_away > 90: 
            continue 
            
        # If Gemini quotes the prompt or returns a placeholder snippet
        if "featured exhibit" in snippet.lower() or len(snippet) < 15:
            snippet = f"Special program: {title} at {m_name}."

        # --- CALCULATE SCORE USING REGEX ---
        # 1. Normalize: strip symbols but keep spaces
        clean_text_for_score = re.sub(r'[^a-z0-9\s]', ' ', f"{title_low} {snippet_low}")
        # 2. Collapse multiple spaces into one to help regex matching
        clean_text_for_score = " ".join(clean_text_for_score.split())    
        spec_score = 7  # THE DEFAULT  
        for score_val, keywords in score_weights:
            # We escape keywords just in case, and use a simpler boundary check
            # This ensures "story time" (with a space) matches correctly
            pattern = r'(^| )(' + '|'.join(map(re.escape, keywords)) + r')( |$)'     
            if re.search(pattern, clean_text_for_score):
                spec_score = score_val
                break
                
        # --- NEW: SEARCH BLOB FOR ACCURATE MAPPING ---
        # Search title, snippet, and location field for branch keywords
        search_blob = f"{title_low} {snippet.lower()} {found_loc}"
        
        for branch in target_branches:
            should_save = False
            if mode == "specific":
                should_save = True
            elif len(target_branches) == 1:
                should_save = True   
            else:
                # --- NEW: IDENTITY MATCHING ---
                # Find unique name (e.g. 'fremont') from 'Fremont Main Library'
                noise_pattern = r'library|branch|store|center|museum|[^a-z0-9\s]'
                branch_name_full = branch.get('name', '').lower()
                clean_identity = re.sub(noise_pattern, '', branch_name_full).strip()
                
                # Rule 1: Specific identity exists in the text blob
                if clean_identity and clean_identity in search_blob:
                    should_save = True

                # Rule 2: Handle 'all' or 'system-wide' keywords (FIXED FOR HARDWARE)
                elif any(x in found_loc for x in ["all", "system", "multiple", "various"]):
                    is_hybrid = any(h in m_name.lower() for h in ["home depot", "lowe", "slime"])
                    if is_hybrid or spec_score >= 10:
                        should_save = True
                    #else:
                        # Keep blocking routine library events (Storytime/Homework) from 'all'
                        #should_save = False
                
                # Rule 3: Direct location match
                elif clean_identity in found_loc or found_loc in clean_identity:
                    should_save = True

            if should_save:
                entry = {
                    'place_id': branch['id'],
                    'place_name': branch.get('name', m_name),
                    'title': title,
                    'event_date': date_str,
                    'snippet': snippet,
                    'category_name': master.get('category_name') or master.get('category') or 'Special Activity',
                    'zip_code': branch.get('zip_code'),
                    'window_type': window,
                    'specificity_score': spec_score, # Make sure this isn't hardcoded to 7!
                }
                
                try:
                    # Final Duplicate Check to prevent double-entries
                    existing = supabase.table("events").select("id") \
                        .eq("place_id", branch['id']) \
                        .eq("title", title) \
                        .eq("event_date", date_str) \
                        .execute()
                    
                    if not existing.data:
                        supabase.table("events").insert(entry).execute()
                except Exception as e: 
                    print(f"      ⚠️ Database insert error: {e}")

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

def get_daily_batch(limit=None):
    """
    Fetches places for scraping.
    If limit is None, it fetches all master records.
    """
    query = supabase.table("places")\
        .select("*")\
        .eq("is_master", True)\
        .order("last_scraped_at")\
        .order("id")

    if limit:
        query = query.limit(limit)

    res = query.execute()
    return res.data
    
#def get_daily_batch(limit=24):
#    """Reverted logic to fix nulls_first crash while keeping ID sorting."""
#    three_days_ago = (datetime.now() - timedelta(days=3)).isoformat()
#    # 1. Sort by last_scraped_at (NULLs naturally group together)
#    # 2. Sort by ID (Ensures ID 1, 2, 3 come first within the NULL group)
#    res = supabase.table("places")\
#        .select("*")\
#        .eq("is_master", True)\
#        .or_(f"last_scraped_at.is.null,last_scraped_at.lt.{three_days_ago}")\
#        .order("last_scraped_at")\
#        .order("id")\
#        .limit(limit)\
#        .execute()
#    return res.data

       
# --- Scraper Pathway --- this function works for the category of workshop, but not others
def scrape_and_save_1(context, master, target_branches, mode, midnight, zip_code=None):
    page = context.new_page()
    page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    url = master['url'] if master['url'].startswith('http') else f'https://{master["url"]}' 
    m_name = master.get('name', 'Unknown')
    is_library = "library" in m_name.lower()
    today = datetime.now()
    future_date = today + timedelta(days=90)
    range_str = f"{today.strftime('%B %d, %Y')} to {future_date.strftime('%B %d, %Y')}"
    
    try:
        # 1. Navigation with 'networkidle' to catch initial API calls
        page.set_extra_http_headers({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.google.com/",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Upgrade-Insecure-Requests": "1"
        }) 
        try:
            print(f"   🌐 Navigating to {m_name}...")
            page.goto(url, wait_until="load", timeout=60000)
            page.wait_for_timeout(5000) 
            try:
                page.wait_for_load_state("networkidle", timeout=15000)
            except:
                print(f"   ⚠️ Network busy for {master['name']}, moving to scrape anyway.")
        except Exception as e:
            print(f"   ❌ Fatal navigation error for {master['name']}: {e}")
            return # Exit this master if we can't even load the page
      
        if mode == "specific" and zip_code:
            try:
                search_field = page.locator("input[placeholder*='zip' i], input[placeholder*='City' i]").first
                search_field.wait_for(state="visible", timeout=10000)
                search_field.fill(str(zip_code))
                page.keyboard.press("Enter")
                time.sleep(12) 
            except: pass
        
        if mode != "specific":
            # 2. UNIVERSAL FIX: Exhaustive Scroll
            # We scroll to the very bottom in small increments to trigger 'Lazy Loading' cards
            print(f"   🖱️ Scrolling {master['name']} to trigger lazy-load...")
            for _ in range(8):
                page.mouse.wheel(0, 2000)
                time.sleep(2.5) # Wait for the 'spinning wheel' to finish loading data
            # --- ADD THE EXTRA STEP HERE ---
            print(f"   🔘 Checking for 'Load More' buttons...")
            for i in range(5):
                try:
                # This looks for buttons containing "Load More", "View More", etc.
                    load_more = page.get_by_role("button", name=re.compile(r"load more|view more|show more|see more", re.I))
                    if load_more.is_visible():
                        load_more.click()
                        print(f"   ✅ Clicked 'Load More' for {master['name']}")
                        time.sleep(4) # Wait for the new content to pop in
                    else: 
                        break
                except:
                    break # No button found, which is fine          
        # 3. DEBUG & CAPTURE (Restoring Screenshot & Cleanup)
        print(f"    📸 Capturing debug state and cleaning data...")
        time.sleep(5) 
        # Create a safe filename for the screenshot
        safe_name = re.sub(r'\W+', '', m_name)
        page.screenshot(path=f"debug_{safe_name}.png")      

        # 4. CAPTURE & CLEAN DATA (Stealth + HTML Fallback)
        print(f"    🧹 Cleaning HTML and extracting text...")

        # Get Raw and Clean HTML first as the broad safety net
        raw_html = page.content()
        clean_html_text = re.sub(r'<(script|style|meta|link|svg|path|footer|nav|header)[^>]*>.*?</\1>', '', raw_html, flags=re.DOTALL)
        clean_html_text = re.sub(r'<[^>]+>', ' ', clean_html_text)
        clean_html_text = re.sub(r'\s+', ' ', clean_html_text).strip()
        print(f"    🧹 Extracting content for {m_name}...") 
        combined_text = ""
       
        # SEPARATED LOGIC FOR LIBRARIES VS WORKSHOPS
        kids_tags = [
            "Babies & Toddler", "Kids", "Teens", "Preschoolers", "Teens (13 to 18 years)", 
            "Children (6 to 9 years)", "Preschoolers (3-5 years)", "Tweens (9 to 12 years)", 
            "Toddlers (1 to 3 years)", "Kids (5-9 yrs)", "Babies (0-1 yrs)", "Families", 
            "Preschoolers (3-5 yrs)", "Teens (12-18 yrs)", "Toddlers (1-3 yrs)", "Tweens (9-12 yrs)", 
            "Preschool", "Family Friendly", "Teens", "School Age", "Baby/Toddler", "Early Childhood", 
            "Elementary School Age", "Family", "Middle School Age", "Teen", "Baby – Preschool", "Children"
        ]
        if is_library:
            combined_text = page.evaluate("""(tagList) => {
                const cardSelectors = ['.cp-event-item', '.biblio-item', '.event-item', '.cp-events-item', 'article'];
                let eventData = [];
                let seenTitles = new Set();
                
                // --- ATTEMPT 1: Selective Card-Based Grab ---
                cardSelectors.forEach(selector => {
                    document.querySelectorAll(selector).forEach(card => {
                        const titleEl = card.querySelector('h2, h3, .title, .cp-event-title');
                        const title = titleEl?.innerText.trim() || "Unknown";

                        // Identify TAG containers
                        const tagEls = card.querySelectorAll('.cp-screen-reader-message, [class*="screen-reader"], .sr-only, .cp-event-audience, .audience, .tags, .cp-event-item-metadata');
                        let tagContext = "";
                        tagEls.forEach(s => tagContext += " " + s.innerText);
                        
                        const lowerContext = tagContext.toLowerCase();
                        const lowerTitle = title.toLowerCase();

                        // Strict Metadata Inclusion
                        const hasKidTag = tagList.some(tag => {
                            const t = tag.toLowerCase();
                            return lowerContext.includes(t) || lowerTitle.includes(t);
                        });

                        if (hasKidTag && !seenTitles.has(title)) {
                            seenTitles.add(title);
                            eventData.push(`TITLE: ${title}\\nVERIFIED_TAGS: ${tagContext}\\nBODY: ${card.innerText.substring(0, 1000)}`);
                        }
                    });
                });

                // --- ATTEMPT 2: Recursive Recursive Fallback (Only if Attempt 1 fails) ---
                if (eventData.length === 0) {
                    // Deep scan every meaningful container on the page
                    const elements = document.querySelectorAll('div, section, li');
                    elements.forEach(el => {
                        const txt = el.innerText;
                        // Avoid grabbing the entire page at once; look for specific event-sized chunks
                        if (txt.length > 100 && txt.length < 1500) {
                            // Only include if it strictly contains one of the tags
                            const hasTag = tagList.some(t => txt.toLowerCase().includes(t.toLowerCase()));
                            if (hasTag) {
                                eventData.push(`[RECURSIVE_MATCH]\\nCONTENT: ${txt}`);
                            }
                        }
                    });
                }

                return eventData.slice(0, 60).join('\\n---\\n');
            }""", kids_tags)
        else:
            # Barnes & Noble / Workshops - Expanded to capture all location markers
            combined_text = page.evaluate("""() => {
                const items = document.querySelectorAll('.event-card, .event-item, .bn-events-item, article, [class*="event-item"]');
                return Array.from(items).map(i => {
                    // Look for location specific spans or classes
                    const loc = i.querySelector('.event-location, .store-name, .venue')?.innerText || "";
                    return `EVENT_START\\nLOCATION_MARKER: ${loc}\\nCONTENT: ${i.innerText.substring(0, 1200)}\\nEVENT_END`;
                }).join('\\n---\\n');
            }""")

        # 5. The 90-Day Sliding Prompt
        # Force a shorter, stricter JSON structure to avoid "Delimiter" errors
        prompt = f"""
        Extract events at {master['name']} from {today.strftime('%B %d, %Y')} to {future_date.strftime('%B %d, %Y')}.
        Rules:
        1. Return ONLY a JSON list of objects: [{{"title": "...", "event_date": "YYYY-MM-DD", "snippet": "...", "found_location": "..."}}]. No intro text, no markdown backticks, and no summary at the end.
        2. DATE RULE: You must find the specific date. If the text says 'Every Monday', calculate the next 3 Mondays starting after {today.strftime('%B %d, %Y')}. 
           IMPORTANT: If NO specific date is found, use 'UNKNOWN' for event_date. DO NOT use today's date ({today.strftime('%Y-%m-%d')}) as a fallback.
           DATE EXPANSION: For recurring events like "Homework Help" or "Storytime" (e.g., 'Every Monday' or 'Mon-Thu'), you MUST generate a separate JSON object for EVERY SINGLE DATE for the next 90 days.
           DO NOT summarize. I need individual entries to fill the calendar.
        3. Snippet must be 1 sentence describing the activity under 20 words.
        4. If no events found, return [].
        5. IDENTIFY AGE GROUP: Look for tags like 'Babies & Toddler', 'Kids', 'Teens', 'Preschoolers', 'Teens (13 to 18 years)', 'Children (6 to 9 years)', 'Preschoolers (3-5 years)', 'Tweens (9 to 12 years)', 'Toddlers (1 to 3 years)', 'Kids (5-9 yrs)', 'Babies (0-1 yrs)', 'Families', 'Preschoolers (3-5 yrs)', 'Teens (12-18 yrs)', 'Toddlers (1-3 yrs)', 'Tweens (9-12 yrs)', 'Preschool', 'Family Friendly', 'Teens', 'School Age', 'Baby/Toddler', 'Early Childhood', 'Elementary School Age', 'Family', 'Middle School Age', 'Teen', 'Baby – Preschool', 'Children'
        6. CONTEXT: If the text looks like a list of store names without times, ignore them. Only extract items that have a TITLE and a specific DATE.
        7. EXCLUDE: Adult-only programming (Tax prep, ESL for adults, Career workshops, Senior socials, Book clubs for adults).
        8. EXCLUDE: Technical demos (iPhone/Mac basics) unless specifically for kids.
        9. LOCATION: Identify which specific branch the event is at. You MUST identify the specific branch name (e.g., 'Albany' or 'Fremont'). Do not omit the branch name. Search the entire text, including headers and descriptions. If the text says 'In Store [Location]', use that location. NO SUMMARIES: Do not combine events from different branches into a single "All Locations" entry. If the same activity happens at different branches, return exact the same number of separate JSON objects. LOCATION EXTRACTION: Check descriptions and metadata carefully for branch names. NEVER use 'All Locations', 'System-wide', or 'Multiple'. If a specific branch name is not found in the text, skip the event. 'found_location' must contain ONLY the specific branch name (e.g., 'Castro Valley').
        10. RECURRING: For daily events, only provide TWO entries per week (Saturdays and Sundays).
        11. Extract as many events as possible (up to 25). CRITICAL: Ensure the JSON remains valid and every object is closed correctly. If you approach your output limit, stop after a complete object. If you reach your token limit, STOP and close the JSON array `]` properly. Never leave a JSON object hanging open.
        12. Output JSON list with these EXACT keys: ["title", "event_date" (YYYY-MM-DD), "snippet", "found_location"].
        Rule: If an event is ambiguous, ask: "Is this for a parent to bring a child to?" If No, ignore it.
        IMPORTANT: Use the date format YYYY-MM-DD. If year is missing in text, assume {today.year}.
        """
        events = generate_with_retry(prompt, combined_text, master['name'])

        if events:
            # Secondary check to ensure no "Adult" events slipped through the AI
            exclude_list = ["adult", "senior", "tax prep", "citizenship test"]
            filtered_events = []
            for ev in events:
                check_text = (ev.get('title', '') + " " + ev.get('snippet', '')).lower()
                if not any(word in check_text for word in exclude_list) or "family" in check_text:
                    filtered_events.append(ev)
            
            if filtered_events:
                save_events(filtered_events, target_branches, midnight, master, mode)
                print(f"    ✅ Successfully found {len(filtered_events)} kid-friendly events for {master['name']}.")
            else:
                print(f"    ⚠️ All found events were filtered out as non-kid-friendly.")
        else:
            print(f"    ⚠️ Gemini found 0 events for {master['name']} in the {range_str} window.")
            
    except Exception as e:
        print(f"❌ Error scraping {master['name']}: {e}")
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
   
    # --- CHANGE HERE: Increase limit to none to cover all current and future places ---
    masters = get_daily_batch(limit=None)
    if not masters: return

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=['--disable-blink-features=AutomationControlled'])
        DESKTOP_UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        context = browser.new_context(user_agent=DESKTOP_UA, viewport={'width': 1920, 'height': 1080})        
       
        for m in masters:
            branches = supabase.table("places").select("*").eq("parent_id", m['id']).execute().data
            if not branches:
                branches = [m]
           
            name_raw = m.get('name') or "Unknown Place"
            name_low = name_raw.lower().replace("’", "'")

            if "home depot" in name_low or "lowe's" in name_low or "lowes" in name_low:
                print(f"🛡️ Hybrid: {m['name']}")
                save_events(get_hybrid_retail_events(m['name']), branches, midnight_today, m, "global")
                continue
   
            elif any(x in name_low for x in ["lego", "barnes", "slime"]):
                print(f"🔍 Dynamic: {m['name']}")
                if "barnes" in name_low:
                    for branch in branches:
                        time.sleep(random.uniform(3.0, 6.0)) # Increased buffer
                        scrape_and_save_1(context, m, [branch], "specific", midnight_today, branch.get('zip_code'))
                else:
                    time.sleep(random.uniform(4.0, 7.0)) # Increased buffer
                    scrape_and_save_1(context, m, branches, "mapping", midnight_today)
           
            elif "library" in name_low:
                print(f"📚 Library Mapping: {m['name']}")
                time.sleep(random.uniform(3.0, 6.0)) # Increased buffer
                scrape_and_save_1(context, m, branches, "mapping", midnight_today)

            else:
                print(f"🌐 Universal/Museum Scrape (Type 2): {m['name']}")
                scrape_and_save_2(context, m, branches, "global", midnight_today)

            # --- BUFFER FOR FREE TIER STABILITY ---
            # Increased from 15s to 25s to prevent TPM (Tokens Per Minute) exhaustion
            # when running 70+ consecutive AI requests.
            print(f"☕ Finished {m['name']}. Cooling down 25s for API stability...")
            time.sleep(25)
           
        browser.close()

    run_gemini_discovery(midnight_today)
        
#def run_scraper():
#    midnight_today = datetime.combine(datetime.now().date(), dt_time.min).isoformat()
#    masters = get_daily_batch(limit=24)
#    if not masters: return

#    with sync_playwright() as p:
#        # Using a standard desktop user agent often helps with ID 3 & 5 blocks
#        browser = p.chromium.launch(headless=True, args=['--disable-blink-features=AutomationControlled'])
#        DESKTOP_UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
#        context = browser.new_context(user_agent=DESKTOP_UA, viewport={'width': 1920, 'height': 1080})        
#        for m in masters:
#            # Mark as scraped immediately
#            # supabase.table("places").update({"last_scraped_at": datetime.now().isoformat()}).eq("id", m['id']).execute()
            
#            # Fetch affiliated branches
#            branches = supabase.table("places").select("*").eq("parent_id", m['id']).execute().data
#            if not branches:
#                branches = [m] # This is vital for single-location sites like Academy!
            
#           # name_low = m['name'].lower().replace("’", "'")
#           # category_low = (m.get('category_name') or "").lower() # Ensure this matches your column name
           
#           # Use .get() and a fallback "" for BOTH name and category
#            name_raw = m.get('name') or "Unknown Place"
#            name_low = name_raw.lower().replace("’", "'")
#            category_raw = m.get('category_name') or ""
#            category_low = category_raw.lower()

#            # 1. HYBRID RETAIL (Home Depot/Lowes)
#            is_hd = "home depot" in name_low
#            is_lowes = "lowe's" in name_low or "lowes" in name_low
#            if is_hd or is_lowes:
#                print(f"🛡️ Hybrid: {m['name']}")
#                # Pass the raw name to your existing working function
#                save_events(get_hybrid_retail_events(m['name']), branches, midnight_today, m, "global")
#                continue # Skip standard scraping
    
#            # 2. SPECIFIC BRANCH SCRAPING (Lego/Barnes/Slime)
#            elif any(x in name_low for x in ["lego", "barnes", "slime"]):
#                print(f"🔍 Dynamic: {m['name']}")
#                if "barnes" in name_low:
#                    # B&N requires individual zip code searches
#                    for branch in branches:
#                        time.sleep(random.uniform(2.0, 4.0))
#                        scrape_and_save_1(context, m, [branch], "specific", midnight_today, branch.get('zip_code'))
#                else:
#                    # Slime and Lego: Scrape once, map to all branches in one go
#                    time.sleep(random.uniform(3.0, 5.0))
#                    scrape_and_save_1(context, m, branches, "mapping", midnight_today)
            
#            # 3. LIBRARIES
#            elif "library" in name_low:
#                print(f"📚 Library Mapping: {m['name']}")
#                time.sleep(random.uniform(2.0, 4.0))
#                scrape_and_save_1(context, m, branches, "mapping", midnight_today)

#            # 4. UNIVERSAL / MUSEUM SITES (the non-workshop category)
#            else:
#                print(f"🌐 Universal/Museum Scrape (Type 2): {m['name']}")
#                scrape_and_save_2(context, m, branches, "global", midnight_today)
            
#        browser.close()
#    # Run the AI discovery for events with missing descriptions
#    run_gemini_discovery(midnight_today)
    
if __name__ == "__main__":
    run_scraper()
