# ---------------------------------------------------------
# 程式碼：src/pod_scra_intel_techcore.py (V5.8.1 雷達實體濾鏡 終極版)
# 職責：1. [雷達] fetch_stt_tasks：依據 mem_tier 與 worker_id 進行動態三級分流。
#       2. [容錯] increment_soft_failure：處理失敗不墜機，打上標記交接重裝。
#       3. [火力] 封裝 Supabase 讀寫、手刻 REST API (Gemini/Groq) 呼叫。
# [V5.8.1 更新] 破除雷達盲區！過濾 r2_url 為空的幽靈任務，防止其佔滿 100 筆掃描配額。
# 適用：全軍通用 (AUDIO_EAT, FLY, RENDER, KOYEB, ZEABUR, DBOS, HF)
# ---------------------------------------------------------
import requests, base64, re, gc
from datetime import datetime

# =========================================================
# 📡 戰略雷達 (Strategic Radar)
# =========================================================
def fetch_stt_tasks(sb, mem_tier, worker_id="UNKNOWN", fetch_limit=50):
    """【低耦合戰略閘道】依據軟失敗次數與檔案大小進行動態三級分流"""
    query = sb.table("view_worker_task_inbox").select("*")
    
    # ☠️ 毒藥天花板：全軍皆無視軟失敗 6 次(含)以上的絕對死檔
    query = query.or_("soft_failure_count.lt.6,soft_failure_count.is.null")


    # 💡 [雷達盲區修復] 絕對防彈物理防線 (相容 Supabase 2.12+)
    # 捨棄容易報錯的否定語法 (not_, neq)，改用正向表列：
    # 只要有副檔名 (.mp3 或 .opus 或 .m4a)，就代表這是真實存在的實體檔案！
    query = query.or_("r2_url.ilike.%.mp3,r2_url.ilike.%.opus,r2_url.ilike.%.m4a")

    if mem_tier < 512:
        # 🏹 輕裝游擊隊 (FLY): 安全第一
        # 💡 雷達校準：精準鎖定 %.opus，接手兵工廠產出
        query = query.or_("soft_failure_count.eq.0,soft_failure_count.is.null") \
                     .gte("audio_size_mb", 0).ilike("r2_url", "%.opus").lt("audio_size_mb", 15) \
                     .order("audio_size_mb", desc=False)
                     
    elif worker_id in ["DBOS", "HUGGINGFACE", "AUDIO_EAT"]:
        # 🚜 重裝巨獸 (HF / DBOS / GHA吞噬者)：無差別碾壓
        query = query.order("audio_size_mb", desc=True, nullsfirst=True)
                     
    else:
        # 🛡️ 中型部隊 (RENDER / KOYEB / ZEABUR)：穩健推進
        query = query.order("soft_failure_count", desc=False, nullsfirst=True) \
                     .order("audio_size_mb", desc=True, nullsfirst=True)
        
    return query.limit(fetch_limit).execute().data or []

def increment_soft_failure(sb, task_id):
    """【容錯推進】遇到異常不崩潰，僅增加失敗計數並抹除 R2，讓系統下次動態重試"""
    try:
        res = sb.table("mission_queue").select("soft_failure_count").eq("id", task_id).single().execute()
        current_count = res.data.get("soft_failure_count") or 0
        sb.table("mission_queue").update({
            "soft_failure_count": current_count + 1,
            "scrape_status": "success", 
            "r2_url": None # 🚀 狀態淨化：精準寫入 Python None，對應資料庫 SQL NULL
        }).eq("id", task_id).execute()
        print(f"🚩 [容錯推進] 任務 {task_id[:8]} 失敗次數 +1 (目前: {current_count + 1}/6)")
    except Exception as e: 
        print(f"⚠️ 容錯推進紀錄失敗: {e}")

# =========================================================
# 📊 資料庫軍械庫 (Database Armory)
# =========================================================
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

def parse_intel_metrics(text):
    metrics = {"score": 0, "evidence": 0}
    try:
        s_match = re.search(r"綜合情報分.*?(\d+)", text)
        if s_match: metrics["score"] = int(s_match.group(1))
    except: pass
    return metrics

# =========================================================
# 🧠 AI 火控與通訊 (AI & Comms)
# =========================================================
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
    
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()
    raw_bytes = resp.content
    
    # 💡 防護一：起飛前安檢 (14MB 硬上限，防止 Base64 塞爆 API)
    file_size_mb = len(raw_bytes) / (1024 * 1024)
    if file_size_mb > 14.0:
        del raw_bytes; gc.collect() # 攔截成功，釋放記憶體
        raise Exception(f"越權攔截：壓縮後檔案仍達 {file_size_mb:.1f}MB，超越 REST API 載重極限，退回交接給重裝部隊。")

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
    else: 
        # 💡 防護二：墜機黑盒子 (擷取原生錯誤訊息，方便除錯)
        err_msg = ai_resp.text[:200] 
        raise Exception(f"Gemini API 拒絕存取 (HTTP {ai_resp.status_code}): {err_msg}")

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
