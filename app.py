import gspread
import json
import os
import base64
import sqlite3
from collections import defaultdict
from datetime import timedelta
from google.oauth2.service_account import Credentials
from flask import Flask, render_template, jsonify, request, redirect, url_for, session
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "berry-clean-secret-2026")
app.config["REMEMBER_COOKIE_DURATION"]   = timedelta(days=365)
app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(days=365)
app.config["REMEMBER_COOKIE_HTTPONLY"]   = True
app.config["SESSION_PERMANENT"]          = True

# ── Auth ──────────────────────────────────────────────────────────────────────
login_manager = LoginManager(app)
login_manager.login_view = "login"

DB_PATH = os.path.join(os.path.dirname(__file__), "users.db")

def init_db():
    con = sqlite3.connect(DB_PATH)
    con.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id    INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT UNIQUE NOT NULL,
            name  TEXT NOT NULL
        )
    """)
    con.commit()
    con.close()

def sync_users_from_sheet():
    """Load all users from the Google Sheet Users tab into SQLite.
    This ensures users persist across deploys since SQLite is ephemeral."""
    try:
        sheet = open_sheet()
        ws = sheet.worksheet("Users")
        rows = ws.get_all_values()
        if len(rows) <= 1:
            return
        con = sqlite3.connect(DB_PATH)
        for row in rows[1:]:
            email = row[0].strip().lower()
            name = row[1].strip() if len(row) > 1 else ""
            if email and name:
                con.execute("INSERT OR IGNORE INTO users (email, name) VALUES (?, ?)", (email, name))
        con.commit()
        con.close()
    except Exception as e:
        print(f"[sync_users] Warning: {e}")

def save_user_to_sheet(email, name):
    """Write a new user to the Google Sheet Users tab for persistence."""
    try:
        sheet = open_sheet()
        ws = sheet.worksheet("Users")
        from datetime import datetime
        ws.append_row([email.lower().strip(), name.strip(), datetime.utcnow().isoformat()])
    except Exception as e:
        print(f"[save_user] Warning: {e}")

init_db()
sync_users_from_sheet()

class User(UserMixin):
    def __init__(self, id, email, name):
        self.id = id; self.email = email; self.name = name

def get_user_by_id(uid):
    con = sqlite3.connect(DB_PATH)
    row = con.execute("SELECT id, email, name FROM users WHERE id=?", (uid,)).fetchone()
    con.close()
    return User(*row) if row else None

def get_user_by_email(email):
    con = sqlite3.connect(DB_PATH)
    row = con.execute("SELECT id, email, name FROM users WHERE email=?", (email.lower().strip(),)).fetchone()
    con.close()
    return User(*row) if row else None

def create_user(email, name):
    con = sqlite3.connect(DB_PATH)
    cur = con.execute("INSERT INTO users (email, name) VALUES (?,?)", (email.lower().strip(), name.strip()))
    con.commit(); uid = cur.lastrowid; con.close()
    # Persist to Google Sheet so it survives redeploys
    save_user_to_sheet(email, name)
    return get_user_by_id(uid)

@login_manager.user_loader
def load_user(uid):
    return get_user_by_id(int(uid))

@app.route("/login", methods=["GET", "POST"])
def login():
    if current_user.is_authenticated:
        return redirect(url_for("dashboard"))
    if request.method == "POST":
        data  = request.get_json()
        email = (data.get("email") or "").strip().lower()
        name  = (data.get("name")  or "").strip()
        if not email:
            return jsonify({"ok": False, "error": "Email required"})
        user = get_user_by_email(email)
        if user:
            login_user(user, remember=True)
            return jsonify({"ok": True, "name": user.name, "new_user": False})
        if not name:
            return jsonify({"ok": False, "needs_name": True})
        user = create_user(email, name)
        login_user(user, remember=True)
        return jsonify({"ok": True, "name": user.name, "new_user": True})
    return render_template("login.html")

@app.route("/logout")
@login_required
def logout():
    logout_user()
    return redirect(url_for("login"))

# ── Google Sheets ─────────────────────────────────────────────────────────────
SPREADSHEET_ID = "1mozKN3vJveQHIeK0LVJEhFrH3F8ukWUsX1dJB6qaY64"
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

def get_creds():
    sa_env = os.environ.get("GOOGLE_SERVICE_ACCOUNT")
    if sa_env:
        info = json.loads(base64.b64decode(sa_env).decode("utf-8"))
        return Credentials.from_service_account_info(info, scopes=SCOPES)
    return Credentials.from_service_account_file(
        os.path.join(os.path.dirname(__file__), "service-account.json"), scopes=SCOPES
    )

def open_sheet():
    return gspread.authorize(get_creds()).open_by_key(SPREADSHEET_ID)

def parse_date(val):
    val = str(val).strip()[:10]
    if not val: return ""
    try:
        if "/" in val:
            parts = val.split("/")
            m, d, y = int(parts[0]), int(parts[1]), int(parts[2])
            return f"{y:04d}-{m:02d}-{d:02d}"
        return val
    except:
        return ""

def rows_by_header(worksheet):
    all_values = worksheet.get_all_values()
    if not all_values: return []
    headers = all_values[0]
    result = []
    for row in all_values[1:]:
        if not any(c.strip() for c in row): continue
        d = {}
        for i, h in enumerate(headers):
            if h and h not in d:
                d[h] = row[i] if i < len(row) else ""
        result.append(d)
    return result

LAUNCH_DATE = "2026-03-11"   # campaign go-live date

def is_live_campaign(campaign_name, row_date):
    """A campaign is live if it's 'window washing' type AND on/after launch date."""
    name_lower = str(campaign_name).lower()
    is_ww = "window" in name_lower or "washing" in name_lower
    date_ok = bool(row_date) and row_date >= LAUNCH_DATE
    return is_ww and date_ok

def fetch_meta_data():
    sheet = open_sheet()
    meta_rows = rows_by_header(sheet.worksheet("Meta_Performance_Log"))

    def num(v):
        try: return float(str(v).replace(",","").replace("$","") or 0)
        except: return 0.0

    raw_meta_live = []
    raw_meta_past = []
    ad_map_live   = {}
    ad_map_past   = {}
    for r in meta_rows:
        ad_id = str(r.get("ad_id","")).strip()
        if not ad_id: continue
        row_date = parse_date(r.get("Date_SOT","") or r.get("reporting ends",""))
        campaign_name = r.get("campaign_name","").strip()
        # If no campaign name set, default to Christmas Lights (all old data)
        if not campaign_name:
            campaign_name = "Holiday Lighting"

        row_data = {
            "date":        row_date,
            "spend":       round(num(r.get("spend",0)), 2),
            "impressions": int(num(r.get("impressions",0))),
            "clicks":      int(num(r.get("clicks",0))),
            "ctr":         round(num(r.get("ctr",0)), 4),
            "cpc":         round(num(r.get("cpc",0)), 2),
            "cpm":         round(num(r.get("cpm",0)), 2),
            "campaign":    campaign_name,
        }

        live = is_live_campaign(campaign_name, row_date)
        if live:
            raw_meta_live.append(row_data)
            ad_map = ad_map_live
        else:
            raw_meta_past.append(row_data)
            ad_map = ad_map_past

        if ad_id not in ad_map:
            ad_map[ad_id] = {
                "ad_id": ad_id, "ad_name": r.get("ad_name",""),
                "campaign": campaign_name,
                "impressions":0,"clicks":0,"spend":0.0,
                "ctr_sum":0.0,"cpc_sum":0.0,"cpm_sum":0.0,"row_count":0
            }
        a = ad_map[ad_id]
        a["impressions"] += int(num(r.get("impressions",0)))
        a["clicks"]      += int(num(r.get("clicks",0)))
        a["spend"]       += num(r.get("spend",0))
        a["ctr_sum"]     += num(r.get("ctr",0))
        a["cpc_sum"]     += num(r.get("cpc",0))
        a["cpm_sum"]     += num(r.get("cpm",0))
        a["row_count"]   += 1

    def build_ads(ad_map):
        ads = []
        for ad_id, a in ad_map.items():
            n = a["row_count"] or 1
            ads.append({
                "ad_id":       ad_id,
                "ad_name":     a["ad_name"],
                "campaign":    a["campaign"],
                "impressions": a["impressions"],
                "clicks":      a["clicks"],
                "spend":       round(a["spend"], 2),
                "ctr":         round(a["ctr_sum"]/n, 4),
                "cpc":         round(a["cpc_sum"]/n, 2),
                "cpm":         round(a["cpm_sum"]/n, 2),
                "leads":       0,
                "creative_url": "",
            })
        ads.sort(key=lambda x: (-x["leads"], -x["ctr"]))
        return ads

    return build_ads(ad_map_live), raw_meta_live, build_ads(ad_map_past), raw_meta_past

def fetch_leads():
    """Return (live_leads_list, past_leads_list) from Perspective_Leads."""
    sheet = open_sheet()
    ws = sheet.worksheet("Perspective_Leads")
    all_values = ws.get_all_values()
    if not all_values: return [], []
    headers = [h.strip().lower() for h in all_values[0]]

    def col(name): return next((i for i,h in enumerate(headers) if h==name), None)
    id_idx   = col("contact_id")
    date_idx = col("date_sot")
    utm_idx  = col("utm_content")
    created_idx = col("created_at")

    seen = set()
    live_leads, past_leads = [], []
    for row in all_values[1:]:
        if not any(c.strip() for c in row): continue
        contact = row[id_idx].strip() if id_idx is not None and id_idx < len(row) else ""
        if not contact or contact in seen: continue
        seen.add(contact)
        raw_date = row[date_idx].strip() if date_idx is not None and date_idx < len(row) else ""
        created  = row[created_idx].strip() if created_idx is not None and created_idx < len(row) else ""
        utm      = row[utm_idx].strip()  if utm_idx  is not None and utm_idx  < len(row) else ""
        lead_date = parse_date(raw_date[:10]) or parse_date(created[:10])
        lead = {"id": contact, "date": lead_date, "utm": utm}
        if lead_date and lead_date >= LAUNCH_DATE:
            live_leads.append(lead)
        else:
            past_leads.append(lead)

    return live_leads, past_leads

def fetch_conversions():
    """Return (live_conversions, past_conversions) from Conversion_Leads_Data.
    Each conversion is a dict with: name, email, job_status, revenue, date, utm."""
    sheet = open_sheet()
    try:
        ws = sheet.worksheet("Conversion_Leads_Data")
    except gspread.exceptions.WorksheetNotFound:
        return [], []
    all_values = ws.get_all_values()
    if not all_values:
        return [], []
    headers = [h.strip().lower().replace(" ", "_") for h in all_values[0]]

    def col(name):
        for i, h in enumerate(headers):
            if name in h:
                return i
        return None

    first_name_idx = col("first_name")
    last_name_idx  = col("last_name")
    status_idx     = col("lead_status")
    value_idx      = col("converted_lead_value")
    date_idx       = col("date_sot")
    utm_idx        = col("utm_content")
    job_id_idx     = col("job_id")

    def parse_money(v):
        try:
            return float(str(v).replace(",", "").replace("$", "").strip() or "0")
        except:
            return 0.0

    seen_jobs = set()
    live_conv, past_conv = [], []
    for row in all_values[1:]:
        if not any(c.strip() for c in row):
            continue
        job_id = row[job_id_idx].strip() if job_id_idx is not None and job_id_idx < len(row) else ""
        if not job_id or job_id in seen_jobs:
            continue
        seen_jobs.add(job_id)

        first = row[first_name_idx].strip() if first_name_idx is not None and first_name_idx < len(row) else ""
        last  = row[last_name_idx].strip()  if last_name_idx  is not None and last_name_idx  < len(row) else ""
        status = row[status_idx].strip()    if status_idx     is not None and status_idx     < len(row) else ""
        revenue = parse_money(row[value_idx] if value_idx is not None and value_idx < len(row) else "0")
        raw_date = row[date_idx].strip()    if date_idx       is not None and date_idx       < len(row) else ""
        utm     = row[utm_idx].strip()      if utm_idx        is not None and utm_idx        < len(row) else ""
        lead_date = parse_date(raw_date[:10])

        is_converted = status.lower() == "converted" and revenue > 0
        is_opportunity = status.lower() in ("invoice sent", "estimate sent") and revenue > 0

        conv = {
            "name": f"{first} {last}".strip(),
            "lead_status": status,
            "revenue": revenue if is_converted else 0,
            "opportunity": revenue if is_opportunity else 0,
            "converted": is_converted,
            "is_opportunity": is_opportunity,
            "date": lead_date,
            "utm": utm,
        }
        if lead_date and lead_date >= LAUNCH_DATE:
            live_conv.append(conv)
        else:
            past_conv.append(conv)

    return live_conv, past_conv

# ── Routes ────────────────────────────────────────────────────────────────────
@app.route("/")
@login_required
def dashboard():
    try:
        live_ads, raw_meta_live, past_ads, raw_meta_past = fetch_meta_data()
        live_leads, past_leads = fetch_leads()
        live_conversions, past_conversions = fetch_conversions()

        # Attach lead counts to live ads via utm_content → ad_id
        utm_counts_live = defaultdict(int)
        for r in live_leads:
            if r["utm"]: utm_counts_live[r["utm"]] += 1
        for ad in live_ads:
            ad["leads"] = utm_counts_live.get(ad["ad_id"], 0)
        live_ads.sort(key=lambda x: (-x["leads"], -x["ctr"]))

        # Attach lead counts to past ads via utm_content → ad_id
        utm_counts_past = defaultdict(int)
        for r in past_leads:
            if r["utm"]: utm_counts_past[r["utm"]] += 1
        matched_past = 0
        for ad in past_ads:
            ad["leads"] = utm_counts_past.get(ad["ad_id"], 0)
            matched_past += ad["leads"]
        unmatched_past = len(past_leads) - matched_past
        if unmatched_past > 0 and past_ads:
            top_ad = max(past_ads, key=lambda a: a["spend"])
            top_ad["leads"] += unmatched_past
        past_ads.sort(key=lambda x: (-x["leads"], -x["ctr"]))

        # Live = any live meta data exists
        is_live = len(raw_meta_live) > 0

        error = None
    except Exception as exc:
        live_ads, past_ads = [], []
        raw_meta_live, raw_meta_past = [], []
        live_leads, past_leads = [], []
        live_conversions, past_conversions = [], []
        is_live = False
        error = str(exc)

    return render_template("index.html",
        live_ads=live_ads, past_ads=past_ads,
        live_leads=live_leads, past_leads=past_leads,
        live_conversions=live_conversions, past_conversions=past_conversions,
        raw_meta_live=raw_meta_live, raw_meta_past=raw_meta_past,
        is_live=is_live, launch_date=LAUNCH_DATE,
        error=error, user_name=current_user.name
    )

if __name__ == "__main__":
    app.run(debug=True, port=5001)
