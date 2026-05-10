import os
import resend
import pgeocode
from datetime import datetime, timedelta
from supabase import create_client

# 1. Setup Connections
supabase = create_client(os.environ['VITE_SUPABASE_URL'], os.environ['VITE_SUPABASE_KEY'])
resend.api_key = os.environ['RESEND_API_KEY']
dist_calc = pgeocode.GeoDistance('us')

def get_upcoming_weekend():
    """Calculates the dates for the upcoming Saturday and Sunday."""
    today = datetime.now()
    # 5 is Saturday, 6 is Sunday
    days_to_sat = (5 - today.weekday()) % 7
    saturday = today + timedelta(days=days_to_sat)
    sunday = saturday + timedelta(days=1)
    return [saturday.date().isoformat(), sunday.date().isoformat()]

def create_event_block(event):
    """Generates HTML that mimics the Replit 'Card' UI."""
    score = event.get('specificity_score', 0)
    # Mapping colors if you have them, otherwise default orange
    cat_color = "#ea580c" 
    
    return f"""
    <div style="border: 1px solid #fed7aa; border-radius: 16px; padding: 20px; margin-bottom: 20px; font-family: sans-serif; background-color: #ffffff;">
        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;">
            <span style="font-size: 18px; font-weight: bold; color: #1e293b;">{event['title']}</span>
            <span style="background-color: #ffedd5; color: #ea580c; padding: 4px 12px; border-radius: 20px; font-size: 12px; font-weight: bold;">
                ★ {score}
            </span>
        </div>
        <div style="color: #64748b; font-size: 14px; margin-bottom: 12px; line-height: 1.4;">
            {event.get('snippet', 'No description available.')}
        </div>
        <div style="display: flex; align-items: center; gap: 10px; font-size: 12px; color: #475569;">
            <span style="background-color: {cat_color}; color: white; padding: 2px 8px; border-radius: 4px; margin-right: 8px;">
                {event.get('category_name', 'General')}
            </span>
            <span>📍 {event.get('venue_name', 'Local')}</span>
            <span style="margin-left: 8px;">📅 {event['event_date']}</span>
        </div>
        <div style="margin-top: 15px;">
            <a href="{event.get('url', '#')}" style="color: #ea580c; text-decoration: none; font-weight: bold; font-size: 14px;">View Details →</a>
        </div>
    </div>
    """

def send_weekly_digest():
    weekend_dates = get_upcoming_weekend()
    users = supabase.table("preferences").select("*").eq("receive_emails", True).execute()
    
    for user in users.data:
        # ... (keep your existing setup code for u_email, u_zip, etc.) ...

        # 1. Fetch events from Supabase
        events_resp = supabase.table("events").select("*")\
            .in_("event_date", weekend_dates)\
            .in_("category_name", u_categories)\
            .order("specificity score", desc=True)\
            .execute()

        # 2. Filter by distance first
        valid_events = []
        for ev in events_resp.data:
            dist = dist_calc.query_postal_code(u_zip, str(ev.get('zip_code')))
            if (dist * 0.621371) <= u_radius:
                valid_events.append(ev)

        # 3. MERGE LOGIC
        # We use a dictionary where the key is the event title
        merged_data = {}

        for ev in valid_events:
            title = ev['title']
            date = ev['event_date']
            loc = ev.get('venue_name', 'Local')

            if title not in merged_data:
                # First time seeing this event title
                merged_data[title] = ev
                # Turn date and location into sets to handle merging
                merged_data[title]['dates_set'] = {date}
                merged_data[title]['locations_set'] = {loc}
            else:
                # We've seen this event, add the new date and location
                merged_data[title]['dates_set'].add(date)
                merged_data[title]['locations_set'].add(loc)

        # 4. Prepare for Display
        final_display_list = []
        for title, ev in merged_data.items():
            # Convert sets back to sorted strings for the email
            ev['event_date'] = ", ".join(sorted(list(ev['dates_set'])))
            ev['venue_name'] = ", ".join(sorted(list(ev['locations_set'])))
            final_display_list.append(ev)

        # 5. Build and Send Email
        if final_display_list:
            # Sort again by score after merging (in case order shifted)
            final_display_list.sort(key=lambda x: x.get('specificity score', 0), reverse=True)
            
            event_html = "".join([create_event_block(e) for e in final_display_list])
            # ... (the rest of your email sending code) ...
            

if __name__ == "__main__":
    send_weekly_digest()
    
