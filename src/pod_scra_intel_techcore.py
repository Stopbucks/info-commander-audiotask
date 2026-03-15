# ---------------------------------------------------------
# 程式碼：src/pod_scra_intel_techcore.py (V5.3 絕對防禦與雷達升級版)
# 職責：1. 封裝所有 Supabase 複雜查詢 2. 處理二進制檔案下載與編碼
# 3. 呼叫外部 AI API 4. 發送 Telegram 戰報
# 特色：用完即丟！將記憶體消耗限制在函式內部，保護 256~512MB 戰機
# ---------------------------------------------------------
import requests, base64, re, gc
from datetime import datetime

def fetch_stt_tasks(sb, mem_tier, fetch_limit=50):
    query = sb.table("view_worker_task_inbox").select("*")
    if mem_tier < 512:
        query = query.or_("r2_url.ilike.%.opus,r2_url.ilike.%.ogg").lt("audio_size_mb", 15).order("audio_size_mb", desc=False)
    else:
        query = query.order("audio_size_mb", desc=True)
    return query.limit(fetch_limit).execute().data or []

def fetch_summary_tasks(sb, fetch_limit=50):
    return sb.table("mission_intel").select("*, mission_queue(episode_title, source_name, r2_url)").eq("intel_status", "Sum.-pre").order("created_at").limit(fetch_limit).execute().data or []

def upsert_intel_status(sb, task_id, status, provider=None, stt_text=None):
    payload = {"task_id": task_id, "intel_status": status}
    if provider: payload["ai_provider"] = provider
    if stt_text: payload["stt_text"] = stt_text
    sb.table("mission_intel").upsert(payload, on_conflict="task_id").execute()

def update_intel_success(sb, task_id, summary, score):
    sb.table("mission_intel").update({
        "summary_text": summary, 
        "intel_status": "Sum.-sent",
        "report_date": datetime.now().strftime("%Y-%m-%d"), 
        "total_score": score
    }).eq("task_id", task_id).execute()
    try:
        sb.table("mission_queue").update({"scrape_status": "completed"}).eq("id", task_id).execute()
    except: pass

def delete_intel_task(sb, task_id):
    try: sb.table("mission_intel").delete().eq("task_id", task_id).execute()
    except: pass

def call_groq_stt(secrets, r2_url_path):
    url = f"{secrets['R2_URL']}/{r2_url_path}"
    m_type = "audio/ogg" if ".opus" in url else "audio/mpeg"
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    audio_data = resp.content
    headers = {"Authorization": f"Bearer {secrets['GROQ_KEY']}"}
    files = {'file': (r2_url_path, audio_data, m_type)}
    data = {'model': 'whisper-large-v3', 'response_format': 'text', 'language': 'en'}
    stt_resp = requests.post("https://api.groq.com/openai/v1/audio/transcriptions", headers=headers, files=files, data=data, timeout=120)
    del audio_data, files, resp; gc.collect()
    if stt_resp.status_code == 200: return stt_resp.text
    else: raise Exception(f"Groq API Error: HTTP {stt_resp.status_code} - {stt_resp.text}")

def call_gemini_summary(secrets, r2_url_path, sys_prompt):
    url = f"{secrets['R2_URL']}/{r2_url_path}"
    m_type = "audio/ogg" if ".opus" in url.lower() or ".ogg" in url.lower() else "audio/mpeg"
    raw_bytes = requests.get(url, timeout=120).content
    b64_audio = base64.b64encode(raw_bytes).decode('utf-8')
    del raw_bytes; gc.collect() 
    gemini_model = "gemini-2.5-flash"
    g_url = f"https://generativelanguage.googleapis.com/v1beta/models/{gemini_model}:generateContent?key={secrets['GEMINI_KEY']}"
    payload = {"contents": [{"parts": [{"text": sys_prompt}, {"inline_data": {"mime_type": m_type, "data": b64_audio}}]}]}
    ai_resp = requests.post(g_url, json=payload, timeout=180)
    del b64_audio, payload; gc.collect() 
    if ai_resp.status_code == 200:
        cands = ai_resp.json().get('candidates', [])
        if cands and cands[0].get('content'): return cands[0]['content']['parts'][0].get('text', "")
        return ""
    else: raise Exception(f"Gemini API Error: HTTP {ai_resp.status_code}")

def parse_intel_metrics(text):
    metrics = {"score": 0, "evidence": 0}
    try:
        s_match = re.search(r"綜合情報分.*?(\d+)", text)
        if s_match: metrics["score"] = int(s_match.group(1))
    except: pass
    return metrics

def send_tg_report(secrets, source, title, summary):
    safe_summary = summary[:3800] + ("...\n(因字數限制截斷)" if len(summary) > 3800 else "")
    safe_source = str(source).replace("_", "＿").replace("*", "＊").replace("[", "〔").replace("]", "〕").replace("`", "‵")
    safe_title = str(title).replace("_", "＿").replace("*", "＊").replace("[", "〔").replace("]", "〕").replace("`", "‵")
    report_msg = f"🎙️ *{safe_source}*\n📌 *{safe_title}*\n\n{safe_summary}"
    url = f"https://api.telegram.org/bot{secrets['TG_TOKEN']}/sendMessage"
    payload = {"chat_id": secrets["TG_CHAT"], "text": report_msg, "parse_mode": "Markdown"}
    try:
        resp = requests.post(url, json=payload, timeout=15)
        if resp.status_code != 200:
            payload["parse_mode"] = None
            resp = requests.post(url, json=payload, timeout=15)
        if resp.status_code == 200: return True
        else: raise Exception(f"Telegram 終極發送失敗: {resp.text}")
    except Exception as e: raise e