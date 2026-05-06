# ---------------------------------------------------------
# 程式碼：src/pod_scra_intel_techcore.py (V6.6 全軍通用：混合動力與零地雷版)
# 職責：1. [雷達] fetch_stt_tasks：依據 mem_tier 與 worker_id 進行動態三級分流。
#       2. [容錯] increment_soft_failure：處理失敗不墜機，打上標記交接重裝。
#       3. [火力] 封裝 Supabase 讀寫、手刻 REST API (Gemini/Groq) 呼叫。
# [V5.8.2 更新] 破除字元解析盲區！改用雙重 neq 排除空值，確保雷達 100% 鎖定實體檔案。
# 適用：全軍通用 (AUDIO_EAT, FLY, RENDER, KOYEB, ZEABUR, DBOS, HF)
# 修改，超級大檔透過VIEW圖，進行冷卻30分鐘以上採用，降低拒絕率。為了因應GEMINI 拒絕翻譯進行"超級大檔"交由AUDIO_EAT處理。
# [V5.9.5 換裝] 核心連線套件全面升級為 curl_cffi，統一全軍 HTTP 引擎。
# [V6.5 更新] 1. 拔除計分機制：固定回傳 0 分，消滅 Regex 解析崩潰點。
# [V6.5 更新] 2. TG 戰報升級：標題鑲嵌 [任務ID前8碼]，方便 HF 歸檔對位。
# [V6.5 更新] 3. 殭屍鎖救援：fetch_summary_tasks 自動回收逾時 60 分鐘的任務。
# [V6.5 更新] 4. 連線統一：改用 curl_cffi 的 multipart 協議，移除 httpx 依賴。
# [V6.6 更新] 1. 解決 _form 崩潰：GROQ 聽寫改用 httpx 引擎，避開 curl_cffi 的 multipart 陷阱。
# [V6.6 更新] 2. 金鑰全相容：智能偵測 GEMINI_API_KEY/GEMINI_KEY 等多種環境變數命名。
# [V6.6 更新] 3. 穩定計分：parse_intel_metrics 固定回傳 0，徹底消滅 Regex 崩毀。
# [V6.6 更新] 4. ID 鑲嵌支援：send_tg_report 完美支援 [任務ID] 鑲嵌顯示。
# ---------------------------------------------------------
import base64, re, gc, os, json
from datetime import datetime, timezone, timedelta
from curl_cffi import requests # 🚀 用於一般 REST API 與 TLS 偽裝
import httpx # 🚀 專門用於解決大型檔案上傳 GROQ 的相容性

# =========================================================
# 📡 戰略雷達 (Strategic Radar)
# =========================================================
def fetch_stt_tasks(sb, mem_tier, worker_id="UNKNOWN", fetch_limit=50):
    query = sb.table("vw_safe_mission_queue").select("*")
    if worker_id == "AUDIO_EAT":
        query = query.or_("assigned_troop.eq.AUDIO_EAT,assigned_troop.is.null,assigned_troop.eq.T2")
    else:
        query = query.or_("assigned_troop.neq.AUDIO_EAT,assigned_troop.is.null")

    if mem_tier < 512:
        query = query.gte("audio_size_mb", 0).ilike("r2_url", "%.opus").lt("audio_size_mb", 15).eq("soft_failure_count", 0).order("audio_size_mb")
    elif worker_id in ["HUGGINGFACE", "AUDIO_EAT", "RAILWAY"]:
        query = query.order("audio_size_mb", desc=True, nullsfirst=True)
    else:
        query = query.order("soft_failure_count", desc=False, nullsfirst=True).order("audio_size_mb", desc=True)
    return query.limit(fetch_limit).execute().data or []

def increment_soft_failure(sb, task_id):
    try:
        res = sb.table("mission_queue").select("soft_failure_count").eq("id", task_id).single().execute()
        current_count = res.data.get("soft_failure_count") or 0
        sb.table("mission_queue").update({"soft_failure_count": current_count + 1, "scrape_status": "success", "r2_url": None}).eq("id", task_id).execute()
        print(f"🚩 [容錯推進] 任務 {task_id[:8]} 失敗次數 +1 (目前: {current_count + 1}/6)")
    except Exception as e: print(f"⚠️ 容錯推進紀錄失敗: {e}")

# =========================================================
# 📊 資料庫軍械庫 (Database Armory)
# =========================================================
def fetch_summary_tasks(sb, fetch_limit=50):
    worker_id = os.environ.get("WORKER_ID", "UNKNOWN")
    dead_line = (datetime.now(timezone.utc) - timedelta(minutes=60)).isoformat()
    query = sb.table("mission_intel").select("*, mission_queue(episode_title, source_name, r2_url, audio_size_mb, soft_failure_count)")\
              .or_(f"intel_status.eq.Sum.-pre,and(intel_status.eq.Sum.-proc,updated_at.lt.{dead_line})")
    
    if worker_id not in ["HUGGINGFACE", "DBOS", "AUDIO_EAT", "RAILWAY"]:
        query = query.lte("mission_queue.audio_size_mb", 30)
        if worker_id == "FLY_LAX" or int(os.environ.get("MEM_TIER", 1024)) < 512:
            query = query.eq("mission_queue.soft_failure_count", 0)
    return query.order("created_at").limit(fetch_limit).execute().data or []

def upsert_intel_status(sb, task_id, status, provider=None, stt_text=None):
    payload = {"task_id": task_id, "intel_status": status}
    if provider: payload["ai_provider"] = provider
    if stt_text: payload["stt_text"] = stt_text
    sb.table("mission_intel").upsert(payload, on_conflict="task_id").execute()

def update_intel_success(sb, task_id, summary, score):
    sb.table("mission_intel").update({
        "summary_text": summary, "intel_status": "Sum.-sent",
        "report_date": datetime.now().strftime("%Y-%m-%d"), "total_score": score
    }).eq("task_id", task_id).execute()
    try: sb.table("mission_queue").update({"scrape_status": "completed"}).eq("id", task_id).execute()
    except: pass

def delete_intel_task(sb, task_id):
    try: sb.table("mission_intel").delete().eq("task_id", task_id).execute()
    except: pass

def parse_intel_metrics(text):
    """【零地雷解析】固定回傳 0 以防崩毀"""
    return {"score": 0, "evidence": 0}

# =========================================================
# 🧠 AI 火控與通訊 (AI & Comms)
# =========================================================

#--- # -----(定位線) 修改 call_groq_stt 改用 httpx 解決 _form 崩潰 ----
def call_groq_stt(secrets, r2_url_path):
    """【快速聽寫】使用 httpx 引擎處理檔案上傳，確保 multipart 100% 成功"""
    url = f"{secrets['R2_URL']}/{r2_url_path}"
    m_type = "audio/ogg" if ".opus" in url.lower() else "audio/mpeg"
    
    # 下載檔案維持用 curl_cffi (TLS 偽裝)
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    audio_data = resp.content
    
    g_api_key = secrets.get('GROQ_API_KEY', secrets.get('GROQ_KEY'))
    headers = {"Authorization": f"Bearer {g_api_key}"}
    files = {'file': (os.path.basename(r2_url_path), audio_data, m_type)}
    data = {'model': 'whisper-large-v3', 'response_format': 'text', 'language': 'en'}
    
    # 🚀 關鍵：使用 httpx 的標準 files 協定，避開所有 'no attribute _form' 報錯
    with httpx.Client(timeout=180.0) as client:
        stt_resp = client.post("https://api.groq.com/openai/v1/audio/transcriptions", 
                               headers=headers, files=files, data=data)

    del audio_data, resp; gc.collect()
    if stt_resp.status_code == 200: return stt_resp.text
    else: raise Exception(f"Groq API Error: HTTP {stt_resp.status_code} - {stt_resp.text}")
#--- # -----(定位線) 以上修改 ----

def call_gemini_summary(secrets, r2_url_path, sys_prompt):
    """【多模態摘要】呼造 Gemini API，支援大檔 SDK 模式"""
    gem_api_key = secrets.get('GEMINI_API_KEY', secrets.get('GEMINI_KEY'))
    gemini_model = "gemini-2.5-flash"
    
    if not r2_url_path or r2_url_path.lower() == 'null':
        g_url = f"https://generativelanguage.googleapis.com/v1beta/models/{gemini_model}:generateContent?key={gem_api_key}"
        ai_resp = requests.post(g_url, json={"contents": [{"parts": [{"text": sys_prompt}]}]}, timeout=180)
    else:
        url = f"{secrets['R2_URL']}/{r2_url_path}"
        m_type = "audio/ogg" if ".opus" in url.lower() or ".ogg" in url.lower() else "audio/mpeg"
        resp = requests.get(url, timeout=120); resp.raise_for_status()
        raw_bytes = resp.content
        file_size_mb = len(raw_bytes) / (1024 * 1024)

        if file_size_mb <= 14.0:
            b64_audio = base64.b64encode(raw_bytes).decode('utf-8')
            del raw_bytes; gc.collect()
            g_url = f"https://generativelanguage.googleapis.com/v1beta/models/{gemini_model}:generateContent?key={gem_api_key}"
            payload = {"contents": [{"parts": [{"text": sys_prompt}, {"inline_data": {"mime_type": m_type, "data": b64_audio}}]}]}
            ai_resp = requests.post(g_url, json=payload, timeout=180)
            del b64_audio; gc.collect()
        else:
            # 重裝部隊 SDK 模式
            if os.environ.get("WORKER_ID") not in ["HUGGINGFACE", "AUDIO_EAT", "RAILWAY"]:
                del raw_bytes; raise Exception(f"越權：檔案 {file_size_mb:.1f}MB 超出此機甲限制。")
            import tempfile, google.generativeai as genai
            genai.configure(api_key=gem_api_key)
            with tempfile.NamedTemporaryFile(delete=False, suffix=".opus") as tmp: tmp.write(raw_bytes); tmp_path = tmp.name
            try:
                uploaded_file = genai.upload_file(path=tmp_path, mime_type=m_type)
                model = genai.GenerativeModel(gemini_model)
                response = model.generate_content([sys_prompt, uploaded_file])
                genai.delete_file(uploaded_file.name); os.remove(tmp_path)
                return response.text
            except Exception as e:
                if os.path.exists(tmp_path): os.remove(tmp_path)
                raise Exception(f"Gemini SDK 失敗: {str(e)}")

    if ai_resp.status_code == 200:
        cands = ai_resp.json().get('candidates', [])
        return cands[0]['content']['parts'][0].get('text', "") if cands else ""
    else: raise Exception(f"Gemini API 拒絕 (HTTP {ai_resp.status_code})")

# =========================================================
# 📡 TG 通訊防禦網 (Telegram Comms)
# =========================================================
def send_tg_report(secrets, source, title, summary, sb=None, worker_id="UNKNOWN", provider="AI"):
    """發送戰報，標題已包含鑲嵌 ID (由 Core.py 預處理)"""
    safe_summary = summary[:3800] + ("...\n(截斷)" if len(summary) > 3800 else "")
    f_source = str(source).replace("_", "＿").replace("*", "＊").replace("[", "〔").replace("]", "〕")
    f_title = str(title).replace("_", "＿").replace("*", "＊").replace("[", "〔").replace("]", "〕")
    report_msg = f"🎙️ *{f_source}*\n📌 *{f_title}*\n🧠 *戰術核心*: {provider}\n\n{safe_summary}"
    
    url = f"https://api.telegram.org/bot{secrets['TG_TOKEN']}/sendMessage"
    payload = {"chat_id": secrets["TG_CHAT"], "text": report_msg, "parse_mode": "Markdown"}
    try:
        resp = requests.post(url, json=payload, timeout=15)
        if resp.status_code != 200:
            payload["parse_mode"] = None
            resp = requests.post(url, json=payload, timeout=15)
        return resp.status_code == 200
    except: return False
