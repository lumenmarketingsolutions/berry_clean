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
    # Seeded accounts — always present
    for email, name in [("avesta70@gmail.com", "Reza"), ("bre@avalon-laser.com", "Bre")]:
        con.execute("INSERT OR IGNORE INTO users (email, name) VALUES (?, ?)", (email, name))
    # Berry Clean client
    con.execute("INSERT OR IGNORE INTO users (email, name) VALUES (?, ?)", ("berrycleanidaho@gmail.com", "Spencer"))
    con.commit()
    con.close()

init_db()

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
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
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

def fetch_meta_data():
    sheet = open_sheet()
    meta_rows = rows_by_header(sheet.worksheet("Meta_Performance_Log"))

    def num(v):
        try: return float(str(v).replace(",","").replace("$","") or 0)
        except: return 0.0

    raw_meta = []
    ad_map   = {}
    for r in meta_rows:
        ad_id = str(r.get("ad_id","")).strip()
        if not ad_id: continue
        row_date = parse_date(r.get("Date_SOT","") or r.get("reporting ends",""))
        raw_meta.append({
            "date":        row_date,
            "spend":       round(num(r.get("spend",0)), 2),
            "impressions": int(num(r.get("impressions",0))),
            "clicks":      int(num(r.get("clicks",0))),
            "ctr":         round(num(r.get("ctr",0)), 4),
            "cpc":         round(num(r.get("cpc",0)), 2),
            "cpm":         round(num(r.get("cpm",0)), 2),
        })
        if ad_id not in ad_map:
            ad_map[ad_id] = {
                "ad_id": ad_id, "ad_name": r.get("ad_name",""),
                "campaign": r.get("campaign_name",""),
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
    ads.sort(key=lambda x: x["spend"], reverse=True)
    return ads, raw_meta

def fetch_leads():
    """Return (total, raw_list) from Perspective_Leads, keyed by unique contact_id."""
    sheet = open_sheet()
    ws = sheet.worksheet("Perspective_Leads")
    all_values = ws.get_all_values()
    if not all_values: return 0, []
    headers = [h.strip().lower() for h in all_values[0]]
    id_idx   = next((i for i,h in enumerate(headers) if h=="contact_id"), None)
    date_idx = next((i for i,h in enumerate(headers) if h=="date_sot"), None)
    utm_idx  = next((i for i,h in enumerate(headers) if h=="utm_content"), None)
    seen, rows = set(), []
    for row in all_values[1:]:
        if not any(c.strip() for c in row): continue
        contact = row[id_idx].strip() if id_idx is not None and id_idx < len(row) else ""
        if not contact or contact in seen: continue
        seen.add(contact)
        raw_date = row[date_idx].strip() if date_idx is not None and date_idx < len(row) else ""
        utm      = row[utm_idx].strip()  if utm_idx  is not None and utm_idx  < len(row) else ""
        rows.append({"id": contact, "date": parse_date(raw_date[:10]), "utm": utm})
    return len(rows), rows

# ── Routes ────────────────────────────────────────────────────────────────────
@app.route("/")
@login_required
def dashboard():
    try:
        ads, raw_meta       = fetch_meta_data()
        total_leads, raw_leads = fetch_leads()

        # Attach lead counts to ads via utm_content
        utm_counts = defaultdict(int)
        for r in raw_leads:
            if r["utm"]: utm_counts[r["utm"]] += 1
        for ad in ads:
            ad["leads"] = utm_counts.get(ad["ad_id"], 0)

        error = None
    except Exception as exc:
        ads, raw_meta, raw_leads, total_leads = [], [], [], 0
        error = str(exc)

    return render_template("index.html",
        ads=ads, total_leads=total_leads,
        raw_meta=raw_meta, raw_leads=raw_leads,
        error=error, user_name=current_user.name
    )

if __name__ == "__main__":
    app.run(debug=True, port=5001)
