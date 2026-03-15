# ---------------------------------------------------------
# 程式碼：src/pod_scra_intel_trans.py  (V5.5 面板統御防崩潰版)
# [節拍] 狀態機邏輯：透過 MAX_TICKS 控制循環。若主將設為 3 拍，則依序執行 [1:下載, 2:摘要, 3:轉譯]。
# [節拍] 判斷公式：利用除以 2 的餘數 (current_tick % 2 != 0) 來動態交替分配任務型態。
# [節拍] 任務分配：單數拍 (1, 3, 5...) 執行轉譯 (STT)；雙數拍 (2, 4, 6...) 執行摘要 (Summary)。

# [主將範例] FLY 為主將 (MAX=12)：僅在「第 1 拍」出門抓音檔，第 2~12 拍交替做摘要與轉譯 (低頻進貨)。
# [主將範例] RENDER 為主將 (MAX=6)：同樣在「第 1 拍」抓音檔，第 2~6 拍做摘要與轉譯 (高頻進貨)。
# [後勤範例] 若身分為「後勤兵」：完全不管 MAX 是多少，【永遠不出門抓檔】，只專心交替做轉譯與摘要。
# [節拍總結] MAX_TICKS 的大小，實質上決定了「主將多久出門進貨一次」的冷卻週期。

# 修正：1. 徹底拔除 audio_officers 與冗餘的傳入參數，避免呼叫崩潰。
# 2. 將 max_ticks 交由 src.pod_scra_intel_control 面板動態管理，落實低耦合。
# ---------------------------------------------------------

import os, requests, time, random, gc, json
from urllib.parse import urlparse
from datetime import datetime, timezone, timedelta
from src.pod_scra_intel_r2 import get_s3_client 
from src.pod_scra_intel_control import get_tactical_panel # 🚀 引入控制面板

def execute_fortress_stages(sb, config, s_log_func):
    now_iso = datetime.now(timezone.utc).isoformat()
    worker_id = config.get("WORKER_ID", "UNKNOWN_NODE")
    
    # 向控制面板請求專屬裝備 (包含 MAX_TICKS)
    panel = get_tactical_panel(worker_id)
    
    # 全局初始 Jitter (模擬機器啟動延遲)
    time.sleep(random.uniform(3.0, 8.0))
    t_res = sb.table("pod_scra_tactics").select("*").eq("id", 1).single().execute()
    if not t_res.data: return
    tactic = t_res.data
    
    is_duty_officer = (tactic.get("active_worker", "") == worker_id)
    w_status = tactic.get("worker_status", {})
    tick_key = f"{worker_id}_tick"
    current_tick = w_status.get(tick_key, 0) + 1
    
    # 由面板決定這台機甲的循環長度
    max_ticks = panel.get("MAX_TICKS", 2) 
    if current_tick > max_ticks: current_tick = 1
        
    role_name = "👑 值勤官" if is_duty_officer else "🛠️ 後勤兵"
    s_log_func(sb, "STATE_M", "INFO", f"⚙️ [戰略狀態機] 身分: {role_name} | 階段節拍: {current_tick} / {max_ticks}")

    from src.pod_scra_intel_core import run_audio_to_stt_mission, run_stt_to_summary_mission

    if is_duty_officer and current_tick == 1:
        s_log_func(sb, "STATE_M", "INFO", f"{role_name} 執行階段 1/3: 外部走私下載")
        rule_res = sb.table("pod_scra_rules").select("domain").in_("worker_id", [worker_id, "ALL"]).gte("expired_at", now_iso).execute()
        my_blacklist = [r['domain'] for r in rule_res.data] if rule_res.data else []
        run_logistics_engine(sb, config, now_iso, s_log_func, my_blacklist)
    
    elif current_tick % 2 != 0 or (not is_duty_officer and current_tick == 1):
        s_log_func(sb, "STATE_M", "INFO", f"{role_name} 啟動轉譯產線 (由面板接管)")
        run_audio_to_stt_mission(sb) 
    else:
        s_log_func(sb, "STATE_M", "INFO", f"{role_name} 啟動摘要發報 (由面板接管)")
        run_stt_to_summary_mission(sb) 

    w_status[tick_key] = current_tick
    health = tactic.get('workers_health', {})
    health[worker_id] = now_iso
    sb.table("pod_scra_tactics").update({"last_heartbeat_at": now_iso, "workers_health": health, "worker_status": w_status}).eq("id", 1).execute()


def run_logistics_engine(sb, config, now_iso, s_log_func, my_blacklist):
    query = sb.table("mission_queue").select("*, mission_program_master(*)").eq("scrape_status", "success").is_("r2_url", "null").lte("troop2_start_at", now_iso).order("created_at", desc=True)\
        .limit(1)       # 伺服器下載數量更動
    tasks = query.execute().data or []
    if not tasks: return
    
    s3 = get_s3_client()
    bucket = os.environ.get("R2_BUCKET_NAME")
    worker_id = config.get('WORKER_ID', 'UNKNOWN')
    UAS = ["Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"]
    
    # 🚀 [Jitter 1] 進入外部伺服器前的初步擬人化延遲 (2~5秒)
    time.sleep(random.uniform(2.0, 5.0))
    
    for idx, m in enumerate(tasks):
        # 🚀 [Jitter 2] 若有多筆任務，在每一筆檔案下載之間加入延遲 (5~12秒)
        # 註：目前 query.limit(1) 不會觸發此段，但若未來放寬 limit，此處將自動生效！
        if idx > 0:
            time.sleep(random.uniform(5.0, 12.0))

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