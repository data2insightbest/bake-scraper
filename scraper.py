import os
import requests
import json
import time
import google.generativeai as genai
from bs4 import BeautifulSoup
from supabase import create_client
from datetime import datetime

# 1. SETUP & AUTHENTICATION
# These must be set in your GitHub Secrets and Replit Secrets
URL = os.environ.get("SUPABASE_URL")
KEY = os.environ.get("SUPABASE_KEY")
GEMINI_KEY = os.environ.get("GEMINI_API_KEY")

# Initialize Clients
supabase = create_client(URL, KEY)
genai.configure(api_key=GEMINI_KEY)
# Using flash for speed and to stay in the free tier
model = genai.GenerativeModel('gemini-1.5-flash')

def get_ai_summary(text, museum_name):
    """
    Sends raw text to Gemini AI to extract a specific, high-quality event summary.
    This mimics the human-like search results we discussed.
    """
    prompt = f"""
    You are a professional kids' event curator for 'BAKE'. 
    Analyze the following text from {museum_name}.
    Identify the most interesting UPCOMING specific workshop, festival, or event for children.
    
    Return ONLY a JSON object in this format:
    {{
      "title": "Specific Event Name (e.g. Penguin Mailbox Build)",
      "snippet": "One catchy, informative sentence describing what kids will actually do.",
      "price": "Exact price (e.g. $15) or 'Free'",
      "found": true
    }}
    If you only find general admission info and no specific upcoming event, return: {{"found": false}}
    
    Website Data: {text[:4000]}
    """
    try:
        response = model.generate_content(prompt)
        # Clean the response in case the AI wraps it in markdown code blocks
        clean_json = response.text.replace('```json', '').replace('```', '').strip()
        return json.loads(clean_json)
    except Exception as e:
        print(f"   ⚠️ AI Processing error: {e}")
        return {"found": False}

def run_bake_scraper():
    # Fetch all locations from your 'places' table
    print("Fetching location list from Supabase...")
    places_res = supabase.table("places").select("*").execute()
    places = places_res.data
    
    print(f"🚀 BAKE AI Scraper Started. Processing {len(places)} locations.")
    print("Note: Running with a 4.5s delay to stay within Gemini's Free Tier (15 RPM).")

    for index, place in enumerate(places):
        name = place.get('name', 'Unknown Location')
        target_url = place.get('url')
        
        try:
            # 1. FETCH WEBSITE HTML
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
            res = requests.get(target_url, timeout=15, headers=headers)
            if res.status_code != 200:
                print(f"   ⏩ Skipping {name}: Status {res.status_code}")
                continue

            # 2. CLEAN HTML (Remove scripts, styles, and nav to focus on content)
            soup = BeautifulSoup(res.text, 'html.parser')
            for element in soup(["script", "style", "nav", "footer", "header"]):
                element.decompose()
            
            # Join text and remove extra whitespace
            clean_text = ' '.join(soup.stripped_strings)

            # 3. CALL THE AI MAGIC
            ai_data = get_ai_summary(clean_text, name)

            if ai_data and ai_data.get("found"):
                event_data = {
                    "place_id": place['id'],
                    "title": ai_data.get('title'),
                    "snippet": ai_data.get('snippet'),
                    "price_text": ai_data.get('price'),
                    "specificity_score": 95, # AI-verified events get top priority
                    
                    "event_date": datetime.now().strftime("%Y-%m-%d"),
                    "zip_code": place.get('zip_code'),
                    "category_name": place.get('category'),
                    "event_url": target_url
                }
                
                # 4. UPSERT TO SUPABASE
                supabase.table("events").upsert(event_data).execute()
                print(f"✅ [{index+1}/{len(places)}] {name}: Found '{ai_data['title']}'")
            else:
                print(f"⚪ [{index+1}/{len(places)}] {name}: No specific event identified.")

            # 5. RATE LIMITING (Wait 4.5 seconds to stay under 15 requests per minute)
            time.sleep(4.5)

        except Exception as e:
            print(f"❌ Error at {name}: {e}")
            time.sleep(2) # Short pause on error before next attempt

    print("🏁 Scrape Complete! Your database is now AI-enriched.")

if __name__ == "__main__":
    run_bake_scraper()
