# ---------------------------------------------------------
# 程式碼：src/pod_scra_intel_techcore.py (V5.0 RENDER/KOYEB 軍械與通訊模組)
# 職責：1. 封裝所有 Supabase 複雜查詢 2. 處理二進制檔案下載與編碼
# 3. 呼叫外部 AI API 4. 發送 Telegram 戰報
# 特色：用完即丟！將記憶體消耗限制在函式內部，保護 256接近512MB 戰機
# 不同於FLY，RENDER稍微加重任務量(FLY確定是256MB，KOYEB/Render稍多)
# ---------------------------------------------------------
import requests, base64, re, gc
from datetime import datetime

# --- 📊 資料庫閘道 (Database Gateway) ---
def fetch_stt_tasks(sb, mem_tier):
    """【智慧閘道】根據體量精準配發 STT 任務，拒絕胖檔案"""
    query = sb.table("view_worker_task_inbox").select("*")
    if mem_tier < 512:
        # 輕裝部隊 (FLY): 僅允許 <15MB 且為 .opus/.ogg
        query = query.or_("r2_url.ilike.%.opus,r2_url.ilike.%.ogg").lt("audio_size_mb", 15).order("audio_size_mb", desc=False)
    else:
        # 重裝部隊 (DBOS/HF): 優先處理大檔案
        query = query.order("audio_size_mb", desc=True)
    # 🚀 關鍵修復：limit 10，避免被已完成的任務卡死頂端
    return query.limit(10).execute().data or []

def fetch_summary_tasks(sb):
    """【視線穿透】一次抓 15 筆，避免被頂端損壞任務卡死"""
    return sb.table("mission_intel").select("*, mission_queue(episode_title, source_name, r2_url)").eq("intel_status", "Sum.-pre").order("created_at").limit(15).execute().data or []

def upsert_intel_status(sb, task_id, status, provider=None, stt_text=None):
    """【幽靈輾壓】強制寫入/覆蓋狀態，避免 Duplicate Key 卡死"""
    payload = {"task_id": task_id, "intel_status": status}
    if provider: payload["ai_provider"] = provider
    if stt_text: payload["stt_text"] = stt_text
    sb.table("mission_intel").upsert(payload).execute()

def update_intel_success(sb, task_id, summary, score):
    """【安全結案】將摘要存入資料庫"""
    sb.table("mission_intel").update({
        "summary_text": summary, 
        "intel_status": "Sum.-sent",
        "report_date": datetime.now().strftime("%Y-%m-%d"), 
        "total_score": score
    }).eq("task_id", task_id).execute()

def delete_intel_task(sb, task_id):
    """【戰損清理】清除失敗的任務，讓它回歸佇列"""
    try: sb.table("mission_intel").delete().eq("task_id", task_id).execute()
    except: pass

# --- 🧠 AI 火控系統 (API Weapons) ---
def call_groq_stt(secrets, r2_url_path):
    """【記憶體隔離】在函式內下載、發送、銷毀音檔"""
    url = f"{secrets['R2_URL']}/{r2_url_path}"
    m_type = "audio/ogg" if ".opus" in url else "audio/mpeg"
    
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    audio_data = resp.content
    
    headers = {"Authorization": f"Bearer {secrets['GROQ_KEY']}"}
    files = {'file': (r2_url_path, audio_data, m_type)}
    data = {'model': 'whisper-large-v3', 'response_format': 'text', 'language': 'en'}
    
    stt_resp = requests.post("https://api.groq.com/openai/v1/audio/transcriptions", headers=headers, files=files, data=data, timeout=120)
    
    # 💥 強制銷毀二進制記憶體
    del audio_data, files, resp; gc.collect()
    
    if stt_resp.status_code == 200:
        return stt_resp.text
    else:
        raise Exception(f"Groq API Error: HTTP {stt_resp.status_code} - {stt_resp.text}")

def call_gemini_summary(secrets, r2_url_path, sys_prompt):
    """【記憶體隔離】處理 Gemini 摘要的音檔下載與 Base64 編碼"""
    url = f"{secrets['R2_URL']}/{r2_url_path}"
    m_type = "audio/ogg" if ".opus" in url.lower() or ".ogg" in url.lower() else "audio/mpeg"
    
    raw_bytes = requests.get(url, timeout=120).content
    b64_audio = base64.b64encode(raw_bytes).decode('utf-8')
    del raw_bytes; gc.collect() # 💥 立刻釋放原始位元組
    
    gemini_model = "gemini-2.5-flash"
    g_url = f"https://generativelanguage.googleapis.com/v1beta/models/{gemini_model}:generateContent?key={secrets['GEMINI_KEY']}"
    payload = {"contents": [{"parts": [{"text": sys_prompt}, {"inline_data": {"mime_type": m_type, "data": b64_audio}}]}]}
    
    ai_resp = requests.post(g_url, json=payload, timeout=180)
    del b64_audio, payload; gc.collect() # 💥 立刻釋放 Base64 巨獸
    
    if ai_resp.status_code == 200:
        cands = ai_resp.json().get('candidates', [])
        if cands and cands[0].get('content'): 
            return cands[0]['content']['parts'][0].get('text', "")
        return ""
    else:
        raise Exception(f"Gemini API Error: HTTP {ai_resp.status_code}")

# --- 📡 通訊與輔助 (Comms & Utils) ---
def parse_intel_metrics(text):
    metrics = {"score": 0, "evidence": 0}
    try:
        s_match = re.search(r"綜合情報分.*?(\d+)", text)
        if s_match: metrics["score"] = int(s_match.group(1))
    except: pass
    return metrics

def send_tg_report(secrets, source, title, summary):
    report_msg = f"🎙️ {source}\n📌 {title}\n\n{summary}"
    resp = requests.post(f"https://api.telegram.org/bot{secrets['TG_TOKEN']}/sendMessage", 
                         json={"chat_id": secrets["TG_CHAT"], "text": report_msg[:4000], "parse_mode": "Markdown"}, timeout=15)
    return resp.status_code == 200