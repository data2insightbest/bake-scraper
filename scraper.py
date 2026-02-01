import os
import requests
import re
from bs4 import BeautifulSoup
from supabase import create_client
from datetime import datetime

# 1. Setup Supabase
URL = os.environ.get("SUPABASE_URL")
KEY = os.environ.get("SUPABASE_KEY")
supabase = create_client(URL, KEY)

def clean_and_summarize(text, keywords):
    """
    Filters out 'junk' and finds the most informative sentence 
    containing the event keywords.
    """
    # Remove excessive whitespace and hidden characters
    clean_text = ' '.join(text.split())
    
    # Split the page into actual sentences
    sentences = re.split(r'(?<=[.!?]) +', clean_text)
    
    found_words = []
    best_sentence = ""
    
    # Identify keywords present
    text_lower = clean_text.lower()
    for word in keywords:
        if word in text_lower:
            found_words.append(word)

    # Scoring logic for the "Best Sentence"
    for s in sentences:
        s_lower = s.lower()
        # Check if the sentence contains our keywords
        matches = [w for w in keywords if w in s_lower]
        
        if matches:
            # We want sentences that are informative (not too short) 
            # but not 'terms and conditions' (not too long).
            if 40 < len(s) < 200:
                # Priority: Sentences with 'you', 'join', 'kids', 'family', or '$'
                if any(x in s_lower for x in ['you', 'join', 'kids', 'family', '$']):
                    best_sentence = s
                    break # Found a perfect 'Hero' sentence
                elif not best_sentence:
                    best_sentence = s

    # Final Cleanup of the snippet
    snippet = best_sentence.strip() if best_sentence else "Discover special programs and seasonal activities at this location."
    
    # Calculate Specificity Score
    score = (len(set(found_words)) * 15)
    if any(x in text_lower for x in ["one-day", "limited", "annual", "registration"]):
        score += 25
        
    # Price Detection
    price = "See Website"
    if "free" in text_lower:
        price = "Free"
    else:
        price_match = re.search(r'\$\d+(?:\.\d{2})?', clean_text)
        if price_match:
            price = price_match.group(0)

    return min(score, 100), list(set(found_words)), price, snippet

def run_bake_scraper():
    places = supabase.table("places").select("*").execute().data
    keywords = ["workshop", "festival", "holiday", "storytime", "camp", "exhibit", "performance", "family day", "kids", "free day"]

    print(f"🚀 BAKE Scraper: Processing {len(places)} locations for informative snippets...")

    for place in places:
        try:
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
            res = requests.get(place['url'], timeout=15, headers=headers)
            if res.status_code != 200: continue

            soup = BeautifulSoup(res.text, 'html.parser')
            
            # Remove scripts and styles so we don't 'scrape' code
            for script in soup(["script", "style"]):
                script.decompose()

            page_text = soup.get_text()
            score, found, price, snippet = clean_and_summarize(page_text, keywords)

            if score > 0:
                primary = found[0].title() if found else "Activity"
                smart_title = f"{primary} at {place['name']}"

                event_data = {
                    "place_id": place['id'],
                    "title": smart_title,
                    "description": f"Targeted search for: {', '.join(found[:3])}",
                    "snippet": snippet, # This is now a full, clean sentence
                    "price_text": price,
                    "specificity_score": score,
                    "event_date": datetime.now().strftime("%Y-%m-%d"),
                    "zip_code": place.get('zip_code'),
                    "category_name": place.get('category'),
                    "event_url": place['url']
                }
                
                supabase.table("events").upsert(event_data).execute()
                print(f"✅ {place['name']}: {snippet[:50]}...")

        except Exception as e:
            print(f"❌ Error at {place['name']}: {e}")

if __name__ == "__main__":
    run_bake_scraper()
    
