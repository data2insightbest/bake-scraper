import os
import resend
from supabase import create_client

# 1. Setup Connections
supabase = create_client(os.environ['VITE_SUPABASE_URL'], os.environ['VITE_SUPABASE_KEY'])
resend.api_key = os.environ['RESEND_API_KEY']

def send_weekly_digest():
    # 2. Get users who want emails
    # Note: Make sure your table has a 'receive_emails' column or similar
    users = supabase.table("preferences").select("*").execute()
    
    for user in users.data:
        user_email = user.get('email')
        user_zip = user.get('zip_code')
        categories = user.get('selected_categories', [])

        # 3. Fetch events matching this user's zip and categories
        # (This is a simplified query logic)
        events = supabase.table("events").select("*")\
            .eq("zip_code", user_zip)\
            .in_("category", categories)\
            .gte("event_date", "today").execute()

        if events.data:
            # 4. Construct the Email
            html_content = f"<h1>Hi! Here are your BAKE events for {user_zip}</h1>"
            for ev in events.data:
                html_content += f"<p><strong>{ev['title']}</strong> - {ev['event_date']}</p>"

            # 5. Send via Resend
            resend.Emails.send({
                "from": "BAKE <onboarding@resend.dev>",
                "to": user_email,
                "subject": "Your Sunday Kids Activity Digest",
                "html": html_content
            })
            print(f"✅ Email sent to {user_email}")

if __name__ == "__main__":
    send_weekly_digest()
