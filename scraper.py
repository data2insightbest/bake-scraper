import os
import json
import time
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import google.generativeai as genai
from supabase import create_client

# 1. Setup Connections
supabase = create_client(os.environ["VITE_SUPABASE_URL"], os.environ["VITE_SUPABASE_KEY"])
genai.configure(api_key=os.environ["GEMINI_API_KEY"])
model = genai.GenerativeModel('gemini-2.0-flash')

# 2. Browser Headers (Fixes 403 Forbidden)
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
}

def cleanup_old_events():
    """Deletes events older than 2 days to save Supabase space."""
    cutoff_date = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
    print(f"🧹 Cleaning up events older than {cutoff_date}...")
    try:
        # Delete rows where event_date is earlier than cutoff
        supabase.table("events").delete().lt("event_date", cutoff_date).execute()
        print(f"✅ Cleanup complete.")
    except Exception as e:
        print(f"⚠️ Cleanup skipped: {e}")

def run_bake_scraper():
    # Fetch museums from your 'places' table
    places_res = supabase.table("places").select("*").execute()
    #places = places_res.data
    places = places_res.data[:5] # Still limited to 5 for testing
    
    print(f"🚀 BAKE AI Scraper Started. Processing {len(places)} locations.")

    for i, place in enumerate(places):
        name = place['name']
        url = place['url']
        
        try:
            # Added headers here to fix 403/405 errors
            response = requests.get(url, headers=HEADERS, timeout=15)
            
            if response.status_code != 200:
                print(f"    ⏩ Skipping {name}: Status {response.status_code}")
                continue

            soup = BeautifulSoup(response.text, 'html.parser')
            text_content = soup.get_text()[:4000] # Feed AI first 4k chars

            # AI Logic (Brief summary for brevity)
            prompt = f"Extract today's kid events from this text for {name}. Return JSON."
            ai_res = model.generate_content(prompt)
            
            # Assuming AI returns valid JSON for your 'events' table structure
            event_data = json.loads(ai_res.text.strip('`json\n ')) 
            
            if event_data.get("found"):
                # Use .insert() with minimal returning to bypass RLS select issues
                supabase.table("events").insert(event_data, count="minimal").execute()
                print(f"✅ [{i+1}/{len(places)}] {name}: Saved successfully.")
            
            # Vital: Sleep to stay within Gemini Free Tier limits
            time.sleep(12) 

        except Exception as e:
            print(f"❌ Error at {name}: {e}")

    # Final Step: Clean up the old Feb 1st data
    cleanup_old_events()
    print("🏁 Scrape and Cleanup Complete!")

if __name__ == "__main__":
    run_bake_scraper()
