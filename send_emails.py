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
    days_to_sat = (5 - today.weekday()) % 7
    saturday = today + timedelta(days=days_to_sat)
    sunday = saturday + timedelta(days=1)
    return [saturday.date().isoformat(), sunday.date().isoformat()]

def create_event_block(event):
    """Generates HTML that mimics the Replit 'Card' UI."""
    score = event.get('specificity_score', 0)
    cat_color = "#ea580c" 
    # Ensure the URL is valid; defaults to '#' if missing
    event_link = event.get('url', '#')
    
    return f"""
    <div style="border: 1px solid #fed7aa; border-radius: 16px; padding: 20px; margin-bottom: 20px; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; background-color: #ffffff;">
        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;">
            <span style="font-size: 18px; font-weight: bold; color: #1e293b;">{event['title']}</span>
            <span style="background-color: #ffedd5; color: #ea580c; padding: 4px 12px; border-radius: 20px; font-size: 12px; font-weight: bold; white-space: nowrap;">
                ★ {score}
            </span>
        </div>
        <div style="color: #64748b; font-size: 14px; margin-bottom: 12px; line-height: 1.5;">
            {event.get('snippet', 'Explore this local activity!')}
        </div>
        <div style="font-size: 12px; color: #475569; line-height: 1.8;">
            <div style="margin-bottom: 4px;">
                <span style="background-color: {cat_color}; color: white; padding: 2px 8px; border-radius: 4px; font-weight: 600; text-transform: uppercase; font-size: 10px; margin-right: 8px;">
                    {event.get('category_name', 'General')}
                </span>
            </div>
            <div style="display: block; margin-bottom: 2px;">📍 <strong>Location:</strong> {event.get('venue_name', 'Multiple Locations')}</div>
            <div style="display: block; margin-bottom: 12px;">📅 <strong>Date:</strong> {event.get('event_date', 'This Weekend')}</div>
        </div>
        <div style="margin-top: 10px; border-top: 1px solid #f1f5f9; padding-top: 15px;">
            <a href="{event_link}" target="_blank" style="display: inline-block; background-color: #ea580c; color: #ffffff; padding: 10px 20px; border-radius: 10px; text-decoration: none; font-weight: bold; font-size: 14px;">
                View Details & Location →
            </a>
        </div>
    </div>
    """

def send_weekly_digest():
    weekend_dates = get_upcoming_weekend()
    
    # 2. Get users who opted in
    users_resp = supabase.table("preferences").select("*").eq("receive_emails", True).execute()
    
    for user in users_resp.data:
        u_email = user.get('email')
        u_zip = str(user.get('zip_code', ''))
        u_radius = user.get('search_radius', 25)
        u_categories = user.get('selected_categories', [])

        if not u_email or not u_categories or not u_zip:
            continue

        # 3. Fetch events matching weekend + categories
        events_resp = supabase.table("events").select("*")\
            .in_("event_date", weekend_dates)\
            .in_("category_name", u_categories)\
            .order("specificity_score", desc=True)\
            .execute()

        # 4. Filter by distance & Merge logic
        merged_events = {}
        
        for ev in events_resp.data:
            ev_zip = str(ev.get('zip_code', ''))
            if not ev_zip: continue
            
            distance = dist_calc.query_postal_code(u_zip, ev_zip)
            # pgeocode returns NaN if zip is invalid, check for that
            import math
            if math.isnan(distance): distance = 0
            
            distance_miles = distance * 0.621371
            
            if distance_miles <= u_radius:
                title = ev['title']
                date = ev['event_date']
                loc = ev.get('venue_name', 'Local')

                if title not in merged_events:
                    merged_events[title] = ev
                    merged_events[title]['dates_set'] = {date}
                    merged_events[title]['locs_set'] = {loc}
                else:
                    merged_events[title]['dates_set'].add(date)
                    merged_events[title]['locs_set'].add(loc)

        # 5. Final Formatting & Emailing
        final_list = []
        for title, ev in merged_events.items():
            ev['event_date'] = ", ".join(sorted(list(ev['dates_set'])))
            ev['venue_name'] = ", ".join(sorted(list(ev['locs_set'])))
            final_list.append(ev)

        if final_list:
            final_list.sort(key=lambda x: x.get('specificity_score', 0), reverse=True)
            
            event_html = "".join([create_event_block(e) for e in final_list])
            
            email_body = f"""
            <div style="background-color: #fff7ed; padding: 40px 10px; font-family: sans-serif;">
                <div style="max-width: 600px; margin: 0 auto; background-color: transparent;">
                    <h1 style="color: #ea580c; text-align: center; font-size: 24px; margin-bottom: 10px;">Your Weekend Kids Activity Digest</h1>
                    <p style="text-align: center; color: #7c2d12; font-size: 16px; margin-bottom: 30px;">Top-rated picks near {u_zip}</p>
                    {event_html}
                    <p style="text-align: center; font-size: 12px; color: #94a3b8; margin-top: 40px;">
                        You are receiving this because you signed up for BAKE Weekly Digests.<br>
                        To change your settings, visit the BAKE app.
                    </p>
                </div>
            </div>
            """

            try:
                resend.Emails.send({
                    "from": "BAKE <onboarding@resend.dev>",
                    "to": u_email,
                    "subject": "Your Weekend Kids Activity Digest",
                    "html": email_body
                })
                print(f"✅ Email sent successfully to {u_email}")
            except Exception as e:
                print(f"❌ Failed to send to {u_email}: {e}")

if __name__ == "__main__":
    send_weekly_digest()
    
