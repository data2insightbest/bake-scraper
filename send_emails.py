import os
import resend
import pgeocode
import math
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
    """Generates HTML card using the place_name column."""
    score = event.get('specificity_score', 0)
    cat_color = "#ea580c" 
    
    # URL Logic: Ensure the link is functional
    raw_url = str(event.get('url') or event.get('link') or "").strip()
    if raw_url and not raw_url.startswith(('http://', 'https://')):
        event_link = f"https://{raw_url}"
    else:
        event_link = raw_url if raw_url else "#"

    return f"""
    <div style="border: 1px solid #fed7aa; border-radius: 16px; padding: 20px; margin-bottom: 20px; font-family: sans-serif; background-color: #ffffff;">
        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px;">
            <span style="font-size: 18px; font-weight: bold; color: #1e293b;">{event['title']}</span>
            <span style="background-color: #ffedd5; color: #ea580c; padding: 4px 12px; border-radius: 20px; font-size: 12px; font-weight: bold;">
                ★ {score}
            </span>
        </div>

        <div style="color: #64748b; font-size: 14px; margin-bottom: 12px; line-height: 1.5;">
            {event.get('snippet', 'Check out this local family event!')}
        </div>

        <div style="display: flex; justify-content: space-between; align-items: flex-end; font-size: 12px; color: #475569; border-top: 1px solid #f1f5f9; padding-top: 12px;">
            <div style="flex: 1;">
                <div style="margin-bottom: 4px;">
                    📍 <strong>Location:</strong> {event.get('display_locations', 'Multiple Locations')}
                </div>
                <div style="margin-bottom: 6px;">
                    📅 <strong>Date:</strong> {event.get('display_dates')}
                </div>
                <span style="background-color: {cat_color}; color: white; padding: 2px 8px; border-radius: 4px; font-weight: bold; font-size: 10px; text-transform: uppercase;">
                    {event.get('category_name', 'General')}
                </span>
            </div>
            
            <div style="text-align: right; margin-left: 10px;">
                <a href="{event_link}" target="_blank" style="color: #ea580c; text-decoration: underline; font-weight: bold; font-size: 13px; display: inline-block;">
                    Visit Website →
                </a>
            </div>
        </div>
    </div>
    """

def send_weekly_digest():
    weekend_dates = get_upcoming_weekend()
    users_resp = supabase.table("preferences").select("*").eq("receive_emails", True).execute()
    
    for user in users_resp.data:
        u_email = user.get('email')
        u_zip = str(user.get('zip_code', ''))
        u_radius = user.get('search_radius', 25)
        u_categories = user.get('selected_categories', [])

        if not u_email or not u_categories or not u_zip:
            continue

        events_resp = supabase.table("events").select("*")\
            .in_("event_date", weekend_dates)\
            .in_("category_name", u_categories)\
            .order("specificity_score", desc=True)\
            .execute()

        merged_events = {}
        for ev in events_resp.data:
            ev_zip = str(ev.get('zip_code', ''))
            distance = dist_calc.query_postal_code(u_zip, ev_zip)
            
            if math.isnan(distance): distance = 0
            
            if (distance * 0.621371) <= u_radius:
                title = ev['title']
                date = ev['event_date']
                
                # FIXED: Pulling from place_name column
                loc = ev.get('place_name') or 'Local'

                if title not in merged_events:
                    merged_events[title] = ev.copy()
                    merged_events[title]['dates_set'] = {date}
                    merged_events[title]['locs_set'] = {loc}
                else:
                    merged_events[title]['dates_set'].add(date)
                    merged_events[title]['locs_set'].add(loc)

        final_list = []
        for title, ev in merged_events.items():
            ev['display_dates'] = " & ".join(sorted(list(ev['dates_set'])))
            ev['display_locations'] = " | ".join(sorted(list(ev['locs_set'])))
            final_list.append(ev)

        if final_list:
            final_list.sort(key=lambda x: x.get('specificity_score', 0), reverse=True)
            event_html = "".join([create_event_block(e) for e in final_list])
            
            email_body = f"""
            <div style="background-color: #fff7ed; padding: 40px 10px; font-family: sans-serif;">
                <div style="max-width: 600px; margin: 0 auto;">
                    <h1 style="color: #ea580c; text-align: center; font-size: 22px;">Your Weekend Kids Activity Digest</h1>
                    <p style="text-align: center; color: #7c2d12; margin-bottom: 30px;">Top picks near {u_zip} for {weekend_dates[0]} & {weekend_dates[1]}</p>
                    {event_html}
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
                print(f"✅ Success: Digest sent to {u_email}")
            except Exception as e:
                print(f"❌ Error: {e}")

if __name__ == "__main__":
    send_weekly_digest()
    
