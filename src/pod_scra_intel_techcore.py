# ---------------------------------------------------------
# 程式碼：src/pod_scra_intel_techcore.py (V5.9.5 全軍通用：核彈隔離與防彈雷達版)
# 職責：1. [雷達] fetch_stt_tasks：依據 mem_tier 與 worker_id 進行動態三級分流。
#       2. [容錯] increment_soft_failure：處理失敗不墜機，打上標記交接重裝。
#       3. [火力] 封裝 Supabase 讀寫、手刻 REST API (Gemini/Groq) 呼叫。
# [V5.8.2 更新] 破除字元解析盲區！改用雙重 neq 排除空值，確保雷達 100% 鎖定實體檔案。
# 適用：全軍通用 (AUDIO_EAT, FLY, RENDER, KOYEB, ZEABUR, DBOS, HF)
# 修改，超級大檔透過VIEW圖，進行冷卻30分鐘以上採用，降低拒絕率。為了因應GEMINI 拒絕翻譯進行"超級大檔"交由AUDIO_EAT處理。
# [V5.9.4 更新] 實裝「核彈隔離區」：全軍嚴禁觸碰指派給 AUDIO_EAT 的超大死檔。
# [V5.9.4 同步] 補齊 AUDIO_EAT 遺漏的 Gemini SDK (File API) 與靜默 TG 戰報防禦。
# [V5.9.5 換裝] 核心連線套件全面升級為 curl_cffi，統一全軍 HTTP 引擎。
# 適用：全軍通用 (AUDIO_EAT, FLY, RENDER, KOYEB, ZEABUR, HF, RAILWAY)
# ---------------------------------------------------------
import base64, re, gc
from datetime import datetime
from curl_cffi import requests # 🚀 換裝：使用 curl_cffi 替換原生 requests


# =========================================================
# 📡 戰略雷達 (Strategic Radar)
# =========================================================
def fetch_stt_tasks(sb, mem_tier, worker_id="UNKNOWN", fetch_limit=50):
    """【低耦合戰略閘道】依據 mem_tier 與兵牌進行動態分流"""
    
    query = sb.table("vw_safe_mission_queue").select("*")
    
    # 🛡️ 核彈隔離防線：防止其他機甲踩到 AUDIO_EAT 的 300MB 地雷
    # 注意：Supabase 的 neq 會過濾掉 NULL，所以必須用 or_ 把 NULL 加回來
    if worker_id == "AUDIO_EAT":
        # AUDIO_EAT 享有特權，可以接手專屬核彈，也可以撿一般大檔
        query = query.or_("assigned_troop.eq.AUDIO_EAT,assigned_troop.is.null,assigned_troop.eq.T2")
    else:
        # 其他所有機甲，絕對不准碰 AUDIO_EAT 的專屬檔案！
        query = query.or_("assigned_troop.neq.AUDIO_EAT,assigned_troop.is.null")

    # 🚀 動態三級分流
    if mem_tier < 512:
        # 🏹 輕裝游擊隊 (FLY): 安全第一
        query = query.gte("audio_size_mb", 0).ilike("r2_url", "%.opus").lt("audio_size_mb", 15) \
                     .order("audio_size_mb", desc=False)
                     
    elif worker_id in ["HUGGINGFACE", "AUDIO_EAT", "RAILWAY"]:
        # 🚜 重裝巨獸：無差別碾壓
        query = query.order("audio_size_mb", desc=True, nullsfirst=True)
                     
    else:
        # 🛡️ 中型部隊 (RENDER / KOYEB / ZEABUR)：穩健推進
        query = query.order("soft_failure_count", desc=False, nullsfirst=True) \
                     .order("audio_size_mb", desc=True, nullsfirst=True)
        
    return query.limit(fetch_limit).execute().data or []
    
def increment_soft_failure(sb, task_id):
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
def fetch_summary_tasks(sb, fetch_limit=50):
    import os
    from datetime import datetime, timezone, timedelta
    worker_id = os.environ.get("WORKER_ID", "UNKNOWN")
    
    dead_line = (datetime.now(timezone.utc) - timedelta(minutes=60)).isoformat()

    query = sb.table("mission_intel").select("*, mission_queue(episode_title, source_name, r2_url, audio_size_mb, soft_failure_count)")\
              .or_(f"intel_status.eq.Sum.-pre,and(intel_status.eq.Sum.-proc,updated_at.lt.{dead_line})")
    
    if worker_id not in ["HUGGINGFACE", "AUDIO_EAT", "RAILWAY"]:
        query = query.lte("mission_queue.audio_size_mb", 14)

    tasks = query.order("created_at").limit(fetch_limit).execute().data or []
    
    valid_tasks = []
    for t in tasks:
        if t["intel_status"] == "Sum.-pre":
            valid_tasks.append(t)
            continue
            
        q_data = t.get("mission_queue") or {}
        fails = q_data.get("soft_failure_count") or 0
        
        if fails < 4:
            valid_tasks.append(t)
            
    return valid_tasks

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
    
    file_size_mb = len(raw_bytes) / (1024 * 1024)
    worker_id = os.environ.get("WORKER_ID", "UNKNOWN")
    gemini_model = "gemini-2.5-flash"

    # ==========================================
    # 🟡 分流 A：輕中型部隊 (<= 14MB) 使用極速 REST Base64
    # ==========================================
    if file_size_mb <= 14.0:
        b64_audio = base64.b64encode(raw_bytes).decode('utf-8')
        del raw_bytes; gc.collect() 
        
        g_url = f"https://generativelanguage.googleapis.com/v1beta/models/{gemini_model}:generateContent?key={secrets['GEMINI_KEY']}"
        payload = {"contents": [{"parts": [{"text": sys_prompt}, {"inline_data": {"mime_type": m_type, "data": b64_audio}}]}]}
        
        ai_resp = requests.post(g_url, json=payload, timeout=180)
        del b64_audio, payload; gc.collect() 
        
        if ai_resp.status_code == 200:
            cands = ai_resp.json().get('candidates', [])
            if cands and cands[0].get('content'): return cands[0]['content']['parts'][0].get('text', "")
            return ""
        else: 
            err_msg = ai_resp.text[:200] 
            raise Exception(f"Gemini REST 拒絕存取 (HTTP {ai_resp.status_code}): {err_msg}")

    # ==========================================
    # 🔴 分流 B：重裝部隊 (> 14MB) 動態啟用官方 SDK File API
    # ==========================================
    else:
        if worker_id not in ["HUGGINGFACE", "AUDIO_EAT", "RAILWAY"]:
            del raw_bytes; gc.collect()
            raise Exception(f"越權攔截：檔案達 {file_size_mb:.1f}MB，此機甲無 SDK 權限，強制退回重試佇列。")
            
        print(f"🚀 [{worker_id}] 檔案達 {file_size_mb:.1f}MB！啟動 Gemini 官方 SDK (File API) 重裝火力...")
        import tempfile
        import google.generativeai as genai 
        
        genai.configure(api_key=secrets['GEMINI_KEY'])
        
        with tempfile.NamedTemporaryFile(delete=False, suffix=".opus") as tmp:
            tmp.write(raw_bytes)
            tmp_path = tmp.name
            
        del raw_bytes; gc.collect()

        try:
            print(f"⬆️ [{worker_id}] 正在將巨型檔案上傳至 Google 雲端...")
            uploaded_file = genai.upload_file(path=tmp_path, mime_type=m_type)
            
            print(f"🧠 [{worker_id}] 上傳完成，開始執行重裝分析...")
            model = genai.GenerativeModel(gemini_model)
            response = model.generate_content([sys_prompt, uploaded_file])
            
            genai.delete_file(uploaded_file.name)
            os.remove(tmp_path)
            
            return response.text
        except Exception as e:
            if os.path.exists(tmp_path): os.remove(tmp_path)
            raise Exception(f"Gemini SDK 重裝打擊失敗: {str(e)}")

# =========================================================
# 📡 TG 通訊防禦網 (Telegram Comms)
# =========================================================
def send_tg_report(secrets, source, title, summary, sb=None, worker_id="UNKNOWN"):
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
        else: raise Exception(f"HTTP {resp.status_code} - {resp.text}")
            
    except Exception as e:
        err_msg = f"⚠️ TG 戰報發送失敗: {str(e)[:150]}"
        print(f"[{worker_id}] {err_msg} (已轉紀錄至 S_LOG，主線任務繼續)")
        if sb:
            try:
                sb.table("pod_scra_log").insert({
                    "worker_id": worker_id, "task_type": "TG_REPORT", "status": "ERROR",
                    "message": f"TG 發報失敗 | Title: {safe_title[:30]} | Err: {str(e)[:100]}"
                }).execute()
            except: pass 
        return False
