# ---------------------------------------------------------
# src/pod_scra_intel_techcore.py v6.5 (T2 中型部隊專用：殭屍鎖回收與零地雷版)
# 職責：1. [雷達] fetch_stt_tasks：依據 mem_tier 與 worker_id 進行動態三級分流。
#       2. [容錯] increment_soft_failure：處理失敗不墜機，打上標記交接重裝。
#       3. [火力] 封裝 Supabase 讀寫、手刻 REST API (Gemini/Groq) 呼叫。
# [V5.8.2 更新] 破除字元解析盲區！改用雙重 neq 排除空值，確保雷達 100% 鎖定實體檔案。
# 適用：全軍通用 (AUDIO_EAT, FLY, RENDER, KOYEB, ZEABUR, DBOS, HF)
# 修改，超級大檔透過VIEW圖，進行冷卻30分鐘以上採用，降低拒絕率。為了因應GEMINI 拒絕翻譯進行"超級大檔"交由AUDIO_EAT處理。
# [V5.9.4 同步] 補齊 AUDIO_EAT 遺漏的 Gemini SDK (File API) 與靜默 TG 戰報防禦。
# [V5.9.5 換裝] 核心連線套件全面升級為 curl_cffi，統一全軍 HTTP 引擎。
# [V6.5 更新] 1. 拔除計分機制：固定回傳 0 分，消滅 Regex 解析崩潰點。
# [V6.5 更新] 2. TG 戰報升級：標題鑲嵌 [任務ID前8碼]，方便 HF 歸檔對位。
# [V6.5 更新] 3. 殭屍鎖救援：fetch_summary_tasks 自動回收逾時 60 分鐘的任務。
# [V6.5 更新] 4. 連線統一：改用 curl_cffi 的 multipart 協議，移除 httpx 依賴。
# ---------------------------------------------------------
import base64, re, gc, os
from datetime import datetime, timezone, timedelta
from curl_cffi import requests # 🚀 全軍統一：使用 curl_cffi 提升偽裝度

# =========================================================
# 📡 戰略雷達 (Strategic Radar)
# =========================================================

def fetch_stt_tasks(sb, mem_tier, worker_id="UNKNOWN", fetch_limit=50):
    """【動態分流】依據記憶體等級鎖定合適體量任務"""
    query = sb.table("vw_safe_mission_queue").select("*")
    # 🛡️ 隔離 AUDIO_EAT 專屬核彈
    query = query.or_("assigned_troop.neq.AUDIO_EAT,assigned_troop.is.null,assigned_troop.eq.T2")

    if mem_tier < 512:
        # 🏹 輕裝游擊隊 (FLY): 僅處理 15MB 以下且無失敗紀錄的 Opus 檔案
        query = query.gte("audio_size_mb", 0).ilike("r2_url", "%.opus") \
                     .lt("audio_size_mb", 15).eq("soft_failure_count", 0) \
                     .order("audio_size_mb", desc=False)
    elif worker_id in ["HUGGINGFACE", "AUDIO_EAT", "RAILWAY"]:
        query = query.order("audio_size_mb", desc=True, nullsfirst=True)
    else:
        # 🛡️ RENDER/KOYEB/ZEABUR: 優先處理低失敗、大體量檔案
        query = query.order("soft_failure_count", desc=False, nullsfirst=True) \
                     .order("audio_size_mb", desc=True, nullsfirst=True)
        
    return query.limit(fetch_limit).execute().data or []

def increment_soft_failure(sb, task_id):
    """【容錯機制】失敗計數 +1，並解除 R2 鎖定等待重啟"""
    try:
        res = sb.table("mission_queue").select("soft_failure_count").eq("id", task_id).single().execute()
        current_count = res.data.get("soft_failure_count") or 0
        sb.table("mission_queue").update({
            "soft_failure_count": current_count + 1,
            "scrape_status": "success", 
            "r2_url": None 
        }).eq("id", task_id).execute()
        print(f"🚩 [容錯推進] 任務 {task_id[:8]} 失敗次數 +1 (目前: {current_count + 1}/6)")
    except Exception as e: 
        print(f"⚠️ 容錯推進紀錄失敗: {e}")

# =========================================================
# 📊 資料庫軍械庫 (Database Armory)
# =========================================================

#--- # -----(定位線) 以下修改 fetch_summary_tasks 加入超時回收 ----
def fetch_summary_tasks(sb, fetch_limit=50):
    """【殭屍鎖回收】抓取準備中或已遺失心跳 (1小時) 的任務"""
    worker_id = os.environ.get("WORKER_ID", "UNKNOWN")
    # 定義 60 分鐘為判定死亡的界線
    dead_line = (datetime.now(timezone.utc) - timedelta(minutes=60)).isoformat()

    # 🚀 關鍵：抓取 Sum.-pre 或 (Sum.-proc 且 更新時間過早)
    query = sb.table("mission_intel").select("*, mission_queue(episode_title, source_name, r2_url, audio_size_mb, soft_failure_count)")\
              .or_(f"intel_status.eq.Sum.-pre,and(intel_status.eq.Sum.-proc,updated_at.lt.{dead_line})")
    
    if worker_id not in ["HUGGINGFACE", "DBOS", "AUDIO_EAT", "RAILWAY"]:
        # 🛡️ 非重裝機甲僅處理 30MB 以下摘要
        query = query.lte("mission_queue.audio_size_mb", 30)
        # FLY 與低配機甲不碰有失敗病史的任務
        if worker_id == "FLY_LAX" or int(os.environ.get("MEM_TIER", 1024)) < 512:
            query = query.eq("mission_queue.soft_failure_count", 0)

    return query.order("created_at").limit(fetch_limit).execute().data or []
#--- # -----(定位線) 以上修改 ----

def upsert_intel_status(sb, task_id, status, provider=None, stt_text=None):
    """【狀態預佔】寫入情報表狀態"""
    payload = {"task_id": task_id, "intel_status": status}
    if provider: payload["ai_provider"] = provider
    if stt_text: payload["stt_text"] = stt_text
    sb.table("mission_intel").upsert(payload, on_conflict="task_id").execute()

def update_intel_success(sb, task_id, summary, score):
    """【結案存檔】摘要成功入庫，同步更新母表狀態"""
    sb.table("mission_intel").update({
        "summary_text": summary, 
        "intel_status": "Sum.-sent",
        "report_date": datetime.now().strftime("%Y-%m-%d"), 
        "total_score": score # 目前固定填 0
    }).eq("task_id", task_id).execute()
    try: 
        sb.table("mission_queue").update({"scrape_status": "completed"}).eq("id", task_id).execute()
    except: pass

def delete_intel_task(sb, task_id):
    """【任務重置】清除情報紀錄"""
    try: sb.table("mission_intel").delete().eq("task_id", task_id).execute()
    except: pass

#--- # -----(定位線) 以下修改 parse_intel_metrics 改為固定回傳 0 ----
def parse_intel_metrics(text):
    """【零地雷解析】徹底拔除 Regex，保證不崩潰"""
    return {"score": 0, "evidence": 0}
#--- # -----(定位線) 以上修改 ----

# =========================================================
# 🧠 AI 火控與通訊 (AI & Comms)
# =========================================================

#--- # -----(定位線) 以下修改 call_groq_stt 適配 curl_cffi 協議 ----
def call_groq_stt(secrets, r2_url_path):
    """【快速聽寫】呼叫 GROQ API 並實裝 multipart 上傳"""
    url = f"{secrets['R2_URL']}/{r2_url_path}"
    m_type = "audio/ogg" if ".opus" in url.lower() else "audio/mpeg"
    
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    audio_data = resp.content
    
    headers = {"Authorization": f"Bearer {secrets.get('GROQ_API_KEY', secrets.get('GROQ_KEY'))}"}
    # 🚀 關鍵：curl_cffi 必須使用 multipart 參數
    mp_payload = {
        "file": (os.path.basename(r2_url_path), audio_data, m_type),
        "model": (None, "whisper-large-v3"),
        "response_format": (None, "text"),
        "language": (None, "en")
    }
    
    stt_resp = requests.post("https://api.groq.com/openai/v1/audio/transcriptions", headers=headers, multipart=mp_payload, timeout=120)

    del audio_data, mp_payload, resp; gc.collect()
    
    if stt_resp.status_code == 200: 
        return stt_resp.text
    else: 
        raise Exception(f"Groq API Error: HTTP {stt_resp.status_code} - {stt_resp.text}")
#--- # -----(定位線) 以上修改 ----

def call_gemini_summary(secrets, r2_url_path, sys_prompt):
    """【多模態摘要】呼叫 Gemini REST API 提煉情報"""
    gemini_model = "gemini-2.5-flash"
    g_url = f"https://generativelanguage.googleapis.com/v1beta/models/{gemini_model}:generateContent?key={secrets['GEMINI_KEY']}"
    
    if not r2_url_path or r2_url_path.lower() == 'null':
        payload = {"contents": [{"parts": [{"text": sys_prompt}]}]}
    else:
        url = f"{secrets['R2_URL']}/{r2_url_path}"
        m_type = "audio/ogg" if ".opus" in url.lower() or ".ogg" in url.lower() else "audio/mpeg"
        
        resp = requests.get(url, timeout=120)
        resp.raise_for_status()
        raw_bytes = resp.content
        
        file_size_mb = len(raw_bytes) / (1024 * 1024)
        if file_size_mb > 30.0: 
            del raw_bytes; gc.collect() 
            raise Exception(f"越權攔截：檔案達 {file_size_mb:.1f}MB，中型機甲無重裝權限。")

        b64_audio = base64.b64encode(raw_bytes).decode('utf-8')
        del raw_bytes; gc.collect() 
        
        payload = {"contents": [{"parts": [{"text": sys_prompt}, {"inline_data": {"mime_type": m_type, "data": b64_audio}}]}]}
    
    ai_resp = requests.post(g_url, json=payload, timeout=180)
    
    if 'b64_audio' in locals(): del b64_audio
    del payload; gc.collect() 
    
    if ai_resp.status_code == 200:
        cands = ai_resp.json().get('candidates', [])
        if cands and cands[0].get('content'): 
            return cands[0]['content']['parts'][0].get('text', "")
        return ""
    else: 
        err_msg = ai_resp.text[:200] 
        raise Exception(f"Gemini API 拒絕存取 (HTTP {ai_resp.status_code}): {err_msg}")

#--- # -----(定位線) 以下修改 send_tg_report 加入 ID 鑲嵌與 task_id 傳遞 ----
def send_tg_report(secrets, source, title, summary, task_id, sb=None, worker_id="UNKNOWN", provider="AUTO"):
    """【戰報發布】將任務 ID 鑲嵌至標題前方"""
    safe_summary = summary[:3800] + ("...\n(截斷)" if len(summary) > 3800 else "")
    f_source = str(source).replace("_", "＿").replace("*", "＊").replace("[", "〔").replace("]", "〕")
    f_title = str(title).replace("_", "＿").replace("*", "＊").replace("[", "〔").replace("]", "〕")
    
    # 🚀 修正：在標題前鑲嵌 [任務ID前8碼]
    short_id = str(task_id)[:8]
    report_msg = f"🎙️ *{f_source}*\n📌 *[{short_id}] {f_title}*\n🧠 *戰術核心*: {provider}\n\n{safe_summary}"
    
    url = f"https://api.telegram.org/bot{secrets['TG_TOKEN']}/sendMessage"
    payload = {"chat_id": secrets["TG_CHAT"], "text": report_msg, "parse_mode": "Markdown"}

    try:
        resp = requests.post(url, json=payload, timeout=15)
        if resp.status_code != 200:
            payload["parse_mode"] = None
            resp = requests.post(url, json=payload, timeout=15)
            
        if resp.status_code == 200: return True
        else: raise Exception(f"HTTP {resp.status_code}")
            
    except Exception as e: 
        print(f"[{worker_id}] TG 發報失敗: {str(e)[:100]}")
        if sb:
            try:
                sb.table("pod_scra_log").insert({
                    "worker_id": worker_id, "task_type": "TG_REPORT", "status": "ERROR",
                    "message": f"TG 發報失敗 | ID: {short_id} | Err: {str(e)[:50]}"
                }).execute()
            except: pass 
        return False
