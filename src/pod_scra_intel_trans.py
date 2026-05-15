# ---------------------------------------------------------
# 程式碼：src/pod_scra_intel_trans.py (V6.15 終極純血特權版)
# 任務： (物流下載引擎)
# [V6.15 升級]
# 1. AUDIO_EAT 特權化：正式列入 HEAVY_ARMORS，具備處理重裝任務權限。
# 2. 時間鎖隔離：AUDIO_EAT 執行時，自動無視 troop2_start_at 的限制。
# 3. 實體網址防呆：增加對 audio_url 遺失任務的紀錄與略過，防止下載崩潰。
# 4. 縮排校準：嚴格遵循 PEP 8 規範，確保所有區塊邏輯清晰。
# ---------------------------------------------------------

import os, time, random, gc, json, uuid
from curl_cffi import requests 
from urllib.parse import urlparse
from datetime import datetime, timezone, timedelta
from src.pod_scra_intel_r2 import get_s3_client 
from src.pod_scra_intel_camouflage import get_tactical_camouflage
from src.pod_scra_intel_control import get_tactical_panel

def execute_fortress_stages(sb, config, s_log_func):
    """【狀態機入口】控制下載、摘要、轉譯的節拍切換"""
    now_iso = datetime.now(timezone.utc).isoformat()
    worker_id = config.get("WORKER_ID", "UNKNOWN_NODE")
    
    panel = get_tactical_panel(worker_id)
    time.sleep(random.uniform(3.0, 8.0))
    
    # 讀取全局戰術面板
    t_res = sb.table("pod_scra_tactics").select("*").eq("id", 1).single().execute()
    if not t_res.data: return
    tactic = t_res.data
    
    is_duty_officer = (tactic.get("active_worker", "") == worker_id)
    w_status = tactic.get("worker_status", {})
    tick_key = f"{worker_id}_tick"
    current_tick = w_status.get(tick_key, 0) + 1
    
    max_ticks = panel.get("MAX_TICKS", 2) 
    if not is_duty_officer:
        gear_ratio = panel.get("IDLE_GEARBOX", 4.0) 
        max_ticks = int(max_ticks * gear_ratio)  
        
    if current_tick > max_ticks: 
        current_tick = 1
        
    role_name = "👑 值勤官" if is_duty_officer else "🛠️ 後勤兵"
    s_log_func(sb, "STATE_M", "INFO", f"⚙️ [戰略狀態機] 身分: {role_name} | 階段節拍: {current_tick} / {max_ticks}")

    # 更新心跳與節拍
    w_status[tick_key] = current_tick
    health = tactic.get('workers_health', {})
    health[worker_id] = now_iso
    sb.table("pod_scra_tactics").update({
        "last_heartbeat_at": now_iso, 
        "workers_health": health, 
        "worker_status": w_status
    }).eq("id", 1).execute()

    from src.pod_scra_intel_core import run_audio_to_stt_mission, run_stt_to_summary_mission

    # 執行任務分配
    if current_tick == 1:
        # 階段 1：物流引擎（下載）
        base_dl_limit = panel.get("DOWNLOAD_LIMIT", 2)
        dl_limit = base_dl_limit if (is_duty_officer or worker_id == "AUDIO_EAT") else 1
        max_same_domain = panel.get("MAX_SAME_DOMAIN", 1)
        
        s_log_func(sb, "STATE_M", "INFO", f"{role_name} 執行第 1 拍: 下載任務 (總量 {dl_limit})")
        
        rule_res = sb.table("pod_scra_rules").select("domain").in_("worker_id", [worker_id, "ALL"]).gte("expired_at", now_iso).execute()
        db_blacklist = [r['domain'] for r in rule_res.data] if rule_res.data else []
        combined_blacklist = list(set(db_blacklist + panel.get("GLOBAL_DOMAIN_BLACKLIST", [])))
        
        run_logistics_engine(sb, config, now_iso, s_log_func, combined_blacklist, dl_limit, max_same_domain, is_duty_officer) 
    
    elif current_tick % 2 != 0:
        # 單數拍：轉譯產線 (STT)
        s_log_func(sb, "STATE_M", "INFO", f"{role_name} 啟動轉譯產線")
        run_audio_to_stt_mission(sb) 
    else:
        # 雙數拍：摘要發報 (Summary)
        s_log_func(sb, "STATE_M", "INFO", f"{role_name} 啟動摘要產線")
        run_stt_to_summary_mission(sb) 

def run_logistics_engine(sb, config, now_iso, s_log_func, my_blacklist, dl_limit=2, max_same_domain=1, is_duty_officer=True):
    """【物流核心】執行真實的音檔抓取與 R2 存儲"""
    worker_id = config.get('WORKER_ID', 'UNKNOWN')
    
    # 🚀 [V6.15] 定位：AUDIO_EAT 與 GITHUB/HF 同級，均屬重裝部隊
    HEAVY_ARMORS = ["HUGGINGFACE", "GITHUB", "AUDIO_EAT"]
    allowed_statuses = ["success", "dl_heavy_only"] if worker_id in HEAVY_ARMORS else ["success"]

    # 📡 掃描目標
    query = sb.table("mission_queue").select("*, mission_program_master(*)").in_("scrape_status", allowed_statuses).is_("r2_url", "null")
    
    # 🚀 [V6.15] AUDIO_EAT 專屬特權：無視 troop2_start_at 時間鎖
    if worker_id != "AUDIO_EAT":
        query = query.lte("troop2_start_at", now_iso)
        
    tasks = query.order("created_at", desc=True).limit(50).execute().data or []
    if not tasks: 
        return
    
    s3 = get_s3_client()
    bucket = os.environ.get("R2_BUCKET_NAME")
    time.sleep(random.uniform(2.0, 5.0))
    
    available_domains = set([urlparse(t['audio_url']).netloc for t in tasks if t.get('audio_url')])
    dynamic_max_domain = 1 if len(available_domains) >= dl_limit else max_same_domain
    
    domain_counts = {} 
    downloaded_count = 0    
    
    for m in tasks:
        if downloaded_count >= dl_limit: break
            
        f_url = m.get('audio_url')
        # 🛡️ 防呆：確保 URL 存在
        if not f_url: 
            s_log_func(sb, "DOWNLOAD", "WARNING", f"⚠️ 任務 {m['id'][:8]} 遺失音檔網址，略過處理。")
            continue
            
        target_domain = urlparse(f_url).netloc
        if any(b in target_domain for b in my_blacklist): continue
        
        current_domain_usage = domain_counts.get(target_domain, 0)
        if current_domain_usage >= dynamic_max_domain: continue

        if downloaded_count > 0: time.sleep(random.uniform(5.0, 12.0))

        ext = os.path.splitext(urlparse(f_url).path)[1] or ".mp3"
        tmp_path = f"/tmp/dl_{m['id'][:8]}{ext}"
        current_dl_fails = m.get('dl_soft_failure_count', 0)

        try:
            camo_gear = get_tactical_camouflage(worker_id, is_duty_officer)
            dynamic_headers = camo_gear["headers"]
            tls_fingerprint = camo_gear["impersonate"]
            
            # 🍎 蘋果離線行為擬真
            if "AppleCoreMedia" in dynamic_headers.get("User-Agent", ""):
                dynamic_headers["X-Playback-Session-Id"] = str(uuid.uuid4()).upper()
            
            with requests.Session(impersonate=tls_fingerprint) as session:
                # 探測預熱 (針對有失敗紀錄的任務)
                if current_dl_fails == 1:
                    probe_headers = dynamic_headers.copy()
                    probe_headers["Range"] = "bytes=0-100" 
                    try:
                        probe_r = session.get(f_url, timeout=15, headers=probe_headers)
                        probe_r.close()
                        time.sleep(random.uniform(0.8, 2.0))
                    except: pass

                final_timeout = 300 if worker_id in HEAVY_ARMORS else 120
                dl_start_time = time.time()
                
                # 執行串流下載
                r = session.get(f_url, stream=True, timeout=final_timeout, headers=dynamic_headers)
                try:
                    r.raise_for_status()
                    with open(tmp_path, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=65536): 
                            if time.time() - dl_start_time > final_timeout:
                                raise TimeoutError("Download Timeout")
                            if chunk: f.write(chunk)
                finally:
                    r.close()
                    
            # 上傳 R2 並標記結案
            s3.upload_file(tmp_path, bucket, os.path.basename(tmp_path))
            sb.table("mission_queue").update({
                "scrape_status": "completed", 
                "r2_url": os.path.basename(tmp_path), 
                "dl_soft_failure_count": 0
            }).eq("id", m['id']).execute()
            
            s_log_func(sb, "DOWNLOAD", "SUCCESS", f"✅ 物資入庫: {m['id'][:8]}")
            downloaded_count += 1 
            domain_counts[target_domain] = current_domain_usage + 1

        except Exception as e: 
            err_str = str(e).lower()
            is_tarpit = any(kw in err_str for kw in ['timeout', 'timed out', 'connection closed', 'connection reset'])
            
            if is_tarpit:
                if current_dl_fails < 1:
                    s_log_func(sb, "DOWNLOAD", "WARNING", f"⚠️ 遭遇連線泥沼，標記重試: {m['id'][:8]}")
                    sb.table("mission_queue").update({"dl_soft_failure_count": 1}).eq("id", m['id']).execute()
                else:
                    s_log_func(sb, "DOWNLOAD", "WARNING", f"⚠️ 再次超時，移交 dl_heavy_only 模式。")
                    sb.table("mission_queue").update({"scrape_status": "dl_heavy_only"}).eq("id", m['id']).execute()
            else:
                s_log_func(sb, "DOWNLOAD", "ERROR", f"❌ 搬運失敗: {str(e)[:100]}")
        finally:
            if os.path.exists(tmp_path): os.remove(tmp_path)
            gc.collect()
