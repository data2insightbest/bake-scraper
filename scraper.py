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

# USE FLASH-LITE: Better for high-volume automated tasks in 2026
model = genai.GenerativeModel('gemini-2.0-flash-lite')

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
}

def cleanup_old_events():
    """Wipes out any records older than 2 days to keep Supabase under 500MB."""
    cutoff_date = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
    print(f"🧹 Database Maintenance: Deleting events before {cutoff_date}...")
    try:
        supabase.table("events").delete().lt("event_date", cutoff_date).execute()
        print(f"✅ Cleanup successful.")
    except Exception as e:
        print(f"⚠️ Cleanup skipped: {e}")

def run_bake_scraper():
    # Fetch museums
    places_res = supabase.table("places").select("*").execute()
    #places = places_res.data
    places = places_res.data[:5]
    
    print(f"🚀 BAKE AI Scraper Started. Mode: Gemini 2.0 Flash-Lite.")

    for i, place in enumerate(places):
        name = place['name']
        url = place['url']
        
        try:
            # Step 1: Fetch Website
            response = requests.get(url, headers=HEADERS, timeout=15)
            if response.status_code != 200:
                print(f"    ⏩ Skipping {name}: Status {response.status_code}")
                continue

            # Step 2: Extract Text
            soup = BeautifulSoup(response.text, 'html.parser')
            text_content = soup.get_text()[:4000]

            # Step 3: AI Processing
            prompt = f"Extract today's kids events from this text for {name}. Return JSON."
            ai_res = model.generate_content(prompt)
            event_data = json.loads(ai_res.text.strip('`json\n ')) 
            
            # Step 4: Save to Supabase
            if event_data.get("found"):
                supabase.table("events").insert(event_data, count="minimal").execute()
                print(f"✅ [{i+1}/{len(places)}] {name}: Saved.")
            else:
                print(f"ℹ️ [{i+1}/{len(places)}] {name}: No events found for today.")

            # --- THE SPEED LIMIT ---
            # Even though limit is 15 RPM, 10 seconds ensures total stability
            # and accounts for network jitter.
            print("⏳ Cooling down for 10 seconds...")
            time.sleep(10) 

        except Exception as e:
            if "429" in str(e):
                print("🛑 RPM LIMIT HIT: Stopping to protect account.")
                break
            print(f"❌ Error at {name}: {e}")

    # Final Step: Remove the old data from Feb 1st
    cleanup_old_events()
    print("🏁 Scrape and Maintenance Cycle Complete!")

if __name__ == "__main__":
    run_bake_scraper()
    
