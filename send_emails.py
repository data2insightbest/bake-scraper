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
    
    # 2. Get users who opted in
    users_resp = supabase.table("preferences").select("*").eq("receive_emails", True).execute()
    
    for user in users_resp.data:
        u_email = user.get('email')
        u_zip = str(user.get('zip_code'))
        u_radius = user.get('search_radius', 25)
        u_categories = user.get('selected_categories', [])

        if not u_email or not u_categories:
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
            ev_zip = str(ev.get('zip_code'))
            distance = dist_calc.query_postal_code(u_zip, ev_zip)
            # pgeocode returns km, convert to miles
            distance_miles = distance * 0.621371
            
            if distance_miles <= u_radius:
                title = ev['title']
                date = ev['event_date']
                loc = ev.get('venue_name', 'Local')

                if title not in merged_events:
                    # New unique event title found
                    merged_events[title] = ev
                    merged_events[title]['dates_set'] = {date}
                    merged_events[title]['locs_set'] = {loc}
                else:
                    # Same event title, add date or location to the sets
                    merged_events[title]['dates_set'].add(date)
                    merged_events[title]['locs_set'].add(loc)

        # 5. Final Formatting & Emailing
        final_list = []
        for title, ev in merged_events.items():
            # Join multiple dates/locations with commas
            ev['event_date'] = ", ".join(sorted(list(ev['dates_set'])))
            ev['venue_name'] = ", ".join(sorted(list(ev['locs_set'])))
            final_list.append(ev)

        if final_list:
            # Final sort by score before building the HTML
            final_list.sort(key=lambda x: x.get('specificity_score', 0), reverse=True)
            
            event_html = "".join([create_event_block(e) for e in final_list])
            
            email_body = f"""
            <div style="background-color: #fff7ed; padding: 40px 20px; font-family: sans-serif;">
                <div style="max-width: 600px; margin: 0 auto;">
                    <h1 style="color: #ea580c; text-align: center;">Your BAKE Weekend Roundup</h1>
                    <p style="text-align: center; color: #7c2d12;">Top picks within {u_radius} miles of {u_zip}</p>
                    <hr style="border: none; border-top: 1px solid #fed7aa; margin: 20px 0;">
                    {event_html}
                    <p style="text-align: center; font-size: 12px; color: #94a3b8; margin-top: 30px;">
                        Manage your preferences in the BAKE app.
                    </p>
                </div>
            </div>
            """

            try:
                resend.Emails.send({
                    "from": "BAKE <onboarding@resend.dev>",
                    "to": u_email,
                    "subject": f"Weekend Activities for {u_zip}",
                    "html": email_body
                })
                print(f"✅ Success: Email sent to {u_email}")
            except Exception as e:
                print(f"❌ Error sending to {u_email}: {e}")

if __name__ == "__main__":
    send_weekly_digest()
    
