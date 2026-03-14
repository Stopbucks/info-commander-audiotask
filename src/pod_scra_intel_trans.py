# ---------------------------------------------------------
# 程式碼：src/pod_scra_intel_trans.py  (V5.1 全軍統一狀態機版)
# 任務：全軍統一 Tick 狀態機、外部物流下載、拋接異常
# ---------------------------------------------------------
import os, requests, time, random, gc, json
from urllib.parse import urlparse
from datetime import datetime, timezone, timedelta
from src.pod_scra_intel_r2 import get_s3_client 

def execute_fortress_stages(sb, config, s_log_func, trigger_intel_func, audio_officers):
    now_iso = datetime.now(timezone.utc).isoformat()
    worker_id = config.get("WORKER_ID", "UNKNOWN_NODE")
    
    t_res = sb.table("pod_scra_tactics").select("*").eq("id", 1).single().execute()
    if not t_res.data: return
    tactic = t_res.data
    
    is_duty_officer = (tactic.get("active_worker", "") == worker_id)
    w_status = tactic.get("worker_status", {})
    tick_key = f"{worker_id}_tick"
    current_tick = w_status.get(tick_key, 0) + 1
    
    max_ticks = 3 if is_duty_officer else 2
    if current_tick > max_ticks: current_tick = 1
        
    role_name = "👑 值勤官" if is_duty_officer else "🛠️ 後勤兵"
    s_log_func(sb, "STATE_M", "INFO", f"⚙️ [戰略狀態機] 身分: {role_name} | 階段節拍: {current_tick} / {max_ticks}")

    from src.pod_scra_intel_core import run_audio_to_stt_mission, run_stt_to_summary_mission
    PROCESS_LIMIT = 1 # 輕裝部隊保守設定為 1

    # 🚀 注意：這裡不寫 try...except，讓崩潰直接穿透到 app.py 去通報軟失敗！
    if is_duty_officer and current_tick == 1:
        s_log_func(sb, "STATE_M", "INFO", f"{role_name} 執行階段 1/3: 外部走私下載")
        rule_res = sb.table("pod_scra_rules").select("domain").in_("worker_id", [worker_id, "ALL"]).gte("expired_at", now_iso).execute()
        my_blacklist = [r['domain'] for r in rule_res.data] if rule_res.data else []
        run_logistics_engine(sb, config, now_iso, s_log_func, my_blacklist)
    
    elif current_tick % 2 != 0 or (not is_duty_officer and current_tick == 1):
        s_log_func(sb, "STATE_M", "INFO", f"{role_name} 啟動轉譯產線 (上限 {PROCESS_LIMIT} 件)")
        if worker_id in audio_officers:
            for i in range(PROCESS_LIMIT):
                print(f"🔄 執行第 {i+1}/{PROCESS_LIMIT} 筆轉譯任務...", flush=True)
                run_audio_to_stt_mission() 
                time.sleep(3)
    else:
        s_log_func(sb, "STATE_M", "INFO", f"{role_name} 啟動摘要發報 (上限 {PROCESS_LIMIT} 件)")
        for i in range(PROCESS_LIMIT):
            print(f"🔄 執行第 {i+1}/{PROCESS_LIMIT} 筆摘要任務...", flush=True)
            run_stt_to_summary_mission() 
            time.sleep(3)

    w_status[tick_key] = current_tick
    health = tactic.get('workers_health', {})
    health[worker_id] = now_iso
    sb.table("pod_scra_tactics").update({"last_heartbeat_at": now_iso, "workers_health": health, "worker_status": w_status}).eq("id", 1).execute()


def run_logistics_engine(sb, config, now_iso, s_log_func, my_blacklist):
    """【核心物流引擎】保留您原本的 403 冰封與建檔邏輯"""
    query = sb.table("mission_queue").select("*, mission_program_master(*)").eq("scrape_status", "success").is_("r2_url", "null").lte("troop2_start_at", now_iso).order("created_at", desc=True).limit(1)
    tasks = query.execute().data or []
    if not tasks: return
    
    s3 = get_s3_client()
    bucket = os.environ.get("R2_BUCKET_NAME")
    worker_id = config.get('WORKER_ID', 'UNKNOWN')
    
    UAS = ["Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"]
    
    for m in tasks:
        f_url = m.get('audio_url')
        if not f_url: continue
        target_domain = urlparse(f_url).netloc
        if any(b in target_domain for b in my_blacklist): continue

        ext = os.path.splitext(urlparse(f_url).path)[1] or ".mp3"
        tmp_path = f"/tmp/dl_{m['id'][:8]}{ext}"
        
        try:
            headers = {"User-Agent": random.choice(UAS), "Accept": "*/*"}
            with requests.get(f_url, stream=True, timeout=120, headers=headers) as r:
                r.raise_for_status()
                with open(tmp_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=1024*1024): f.write(chunk)
                    
            s3.upload_file(tmp_path, bucket, os.path.basename(tmp_path))
            sb.table("mission_queue").update({"scrape_status": "completed", "r2_url": os.path.basename(tmp_path)}).eq("id", m['id']).execute()
            s_log_func(sb, "DOWNLOAD", "SUCCESS", f"✅ 物資入庫: {m['id'][:8]}")
            
        except requests.exceptions.HTTPError as he:
            if he.response.status_code in [403, 401, 429]:
                s_log_func(sb, "DOWNLOAD", "ERROR", f"🚫 [{worker_id}] 遭封鎖 ({he.response.status_code})")
                victim_freeze = (datetime.now(timezone.utc) + timedelta(hours=24)).isoformat()
                ally_freeze = (datetime.now(timezone.utc) + timedelta(hours=12)).isoformat()
                
                sb.table("pod_scra_rules").insert([
                    {"worker_id": worker_id, "domain": target_domain, "rule_type": "AUTO_COOLDOWN", "expired_at": victim_freeze},
                    {"worker_id": "ALL", "domain": target_domain, "rule_type": "VIGILANCE", "expired_at": ally_freeze}
                ]).execute()
            else:
                s_log_func(sb, "DOWNLOAD", "ERROR", f"❌ 搬運異常: {he.response.status_code}")
        except Exception as e: 
            s_log_func(sb, "DOWNLOAD", "ERROR", f"❌ 搬運失敗: {str(e)}")
        finally:
            if os.path.exists(tmp_path): os.remove(tmp_path)
            gc.collect()