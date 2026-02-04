import os
import requests
import json
import time
import google.generativeai as genai
from bs4 import BeautifulSoup
from supabase import create_client
from datetime import datetime

# 1. SETUP & AUTHENTICATION
URL = os.environ.get("SUPABASE_URL")
KEY = os.environ.get("SUPABASE_KEY")
GEMINI_KEY = os.environ.get("GEMINI_API_KEY")

# Initialize Clients
supabase = create_client(URL, KEY)
genai.configure(api_key=GEMINI_KEY)
model = genai.GenerativeModel('gemini-1.5-flash')

def get_ai_summary(text, museum_name):
    """
    Sends raw text to Gemini AI to extract a specific event.
    """
    prompt = f"""
    You are a professional kids' event curator for 'BAKE'. 
    Analyze the following text from {museum_name}.
    Identify the most interesting UPCOMING specific workshop, festival, or event for children.
    
    Return ONLY a JSON object in this format:
    {{
      "title": "Specific Event Name",
      "snippet": "One catchy sentence describing what kids will actually do.",
      "price": "Price or 'Free'",
      "found": true
    }}
    If you only find general info, return: {{"found": false}}
    
    Website Data: {text[:4000]}
    """
    try:
        response = model.generate_content(prompt)
        clean_json = response.text.replace('```json', '').replace('```', '').strip()
        return json.loads(clean_json)
    except Exception as e:
        print(f"    ⚠️ AI Processing error: {e}")
        return {"found": False}

def run_bake_scraper():
    # Fetch all locations
    print("Fetching location list from Supabase...")
    places_res = supabase.table("places").select("*").execute()
    places = places_res.data
    
    print(f"🚀 BAKE AI Scraper Started. Processing {len(places)} locations.")

    for index, place in enumerate(places):
        name = place.get('name', 'Unknown Location')
        target_url = place.get('url')
        
        try:
            # 1. FETCH WEBSITE HTML
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
            res = requests.get(target_url, timeout=15, headers=headers)
            if res.status_code != 200:
                print(f"    ⏩ Skipping {name}: Status {res.status_code}")
                continue

            # 2. CLEAN HTML
            soup = BeautifulSoup(res.text, 'html.parser')
            for element in soup(["script", "style", "nav", "footer", "header"]):
                element.decompose()
            clean_text = ' '.join(soup.stripped_strings)

            # 3. CALL THE AI MAGIC
            ai_data = get_ai_summary(clean_text, name)

            if ai_data and ai_data.get("found"):
                # CURRENT DATE for the record
                today_str = datetime.now().strftime("%Y-%m-%d")
                
                event_data = {
                    "place_id": place['id'],
                    "title": ai_data.get('title'),
                    "snippet": ai_data.get('snippet'),
                    "price_text": ai_data.get('price'),
                    "specificity_score": 95,
                    "event_date": today_str,
                    "zip_code": place.get('zip_code'),
                    "category_name": place.get('category'),
                    "event_url": target_url
                }
                
                # 4. UPSERT TO SUPABASE
                # We capture the result to see if Supabase actually accepted it
                result = supabase.table("events").upsert(event_data).execute()
                
                if result.data:
                    print(f"✅ [{index+1}/{len(places)}] {name}: Saved '{ai_data['title']}' for {today_str}")
                else:
                    print(f"⚠️ [{index+1}/{len(places)}] {name}: Connected, but data was not saved. Check RLS or Constraints.")
                
            else:
                print(f"⚪ [{index+1}/{len(places)}] {name}: AI found no new specific events.")

            time.sleep(4.5)

        except Exception as e:
            # THIS IS CRITICAL: It will tell you exactly why a row fails (like the Date error we saw)
            print(f"❌ Error at {name}: {e}")
            time.sleep(2)

    print("🏁 Scrape Complete!")

if __name__ == "__main__":
    run_bake_scraper()
    
