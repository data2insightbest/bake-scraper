import os
import requests
import re
from bs4 import BeautifulSoup
from supabase import create_client
from datetime import datetime

# 1. Setup
URL = os.environ.get("SUPABASE_URL")
KEY = os.environ.get("SUPABASE_KEY")
supabase = create_client(URL, KEY)

def get_rich_data(text, keywords):
    text_lower = text.lower()
    score = 0
    found_words = []
    
    # --- 1. Specificity & Keywords ---
    for word in keywords:
        if word in text_lower:
            score += 15
            found_words.append(word)
    
    # Bonus for rare/special events
    boosters = ["one-day", "limited", "registration required", "annual", "festival"]
    for boost in boosters:
        if boost in text_lower:
            score += 20
            
    # --- 2. Price Detection ---
    price = "See Website"
    if "free" in text_lower:
        price = "Free"
    else:
        price_match = re.search(r'\$\d+(?:\.\d{2})?', text)
        if price_match:
            price = price_match.group(0)

    # --- 3. Snippet (The "Proof") ---
    snippet = ""
    if found_words:
        first_word = found_words[0]
        idx = text_lower.find(first_word)
        start = max(0, idx - 50)
        end = min(len(text), idx + 100)
        snippet = f"...{text[start:end].strip()}..."
        snippet = snippet.replace('\n', ' ')

    return min(score, 100), found_words, price, snippet

def run_bake_scraper():
    # Fetch places
    places = supabase.table("places").select("*").execute().data
    keywords = ["workshop", "festival", "holiday", "storytime", "camp", "exhibit", "performance", "family day", "kids"]

    print(f"🚀 Starting Deep Scrape of {len(places)} locations...")

    for place in places:
        try:
            headers = {'User-Agent': 'Mozilla/5.0'}
            res = requests.get(place['url'], timeout=15, headers=headers)
            if res.status_code != 200: continue

            soup = BeautifulSoup(res.text, 'html.parser')
            # Focus on visible text only
            page_text = ' '.join(soup.stripped_strings)
            
            score, found, price, snippet = get_rich_data(page_text, keywords)

            if score > 0:
                # Create a "Smart Title"
                primary = found[0].title() if found else "Activity"
                smart_title = f"{primary} at {place['name']}"

                event_data = {
                    "place_id": place['id'],
                    "title": smart_title,
                    "description": f"Found mentions of: {', '.join(found[:3])}",
                    "snippet": snippet,
                    "price_text": price,
                    "specificity_score": score,
                    "event_date": datetime.now().strftime("%Y-%m-%d"),
                    "zip_code": place.get('zip_code'),
                    "category_name": place.get('category'),
                    "event_url": place['url']
                }
                
                supabase.table("events").upsert(event_data).execute()
                print(f"✅ Saved: {smart_title} (${price})")

        except Exception as e:
            print(f"❌ Error at {place['name']}: {e}")

if __name__ == "__main__":
    run_bake_scraper()
