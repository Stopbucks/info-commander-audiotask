# ---------------------------------------------------------
# 程式碼：src/pod_scra_intel_trans.py  (V5.9.9 GHA 終極擬態與防彈下載版)
# [節拍] 狀態機邏輯：透過 MAX_TICKS 控制循環。若主將設為 3 拍，則依序執行 [1:下載, 2:摘要, 3:轉譯]。
# [主將範例] RENDER 為主將 (MAX=6)：在「第 1 拍」抓音檔，第 2~6 拍做摘要與轉譯 (高頻進貨)。
# 修正：1. 徹底拔除 audio_officers 與冗餘的傳入參數，避免呼叫崩潰。
# 2. 將 max_ticks 交由 src.pod_scra_intel_control 面板動態管理，落實低耦合。
# [V5.9.8 升級] 全面掛載千面人迷彩模組 (Tier 1 絕對白名單)，掩護 GHA 高風險 IP。
# [V5.9.8 升級] 實裝 3MB 切片與 0.5s 擬人化緩衝，防止 CDN 掐斷資料中心極速下載。
# [隱蔽] 全面換裝 curl_cffi，實裝底層 TLS 指紋擬態 (Impersonate Safari)。
# [隱蔽] 導入 camouflage 千面人模組，透過機甲基因種子達成每日一致性偽裝。
# [防禦] 實裝 3MB 切片與 0.5s 擬人化緩衝，配合 Session 保持，破解跳轉陷阱與 403 封鎖。
# [V5.9.9 更新] 徹底拔除原生 requests，改用 curl_cffi 達成最高等級隱身下載。
# ---------------------------------------------------------

import os, time, random, gc, json
from curl_cffi import requests # 🚀 換裝：使用 curl_cffi 替換原生 requests
from urllib.parse import urlparse
from datetime import datetime, timezone, timedelta
from src.pod_scra_intel_r2 import get_s3_client 
from src.pod_scra_intel_control import get_tactical_panel 
from src.pod_scra_intel_camouflage import get_tactical_camouflage # 👈 更新動態HEADER名稱呼叫

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
        
        # 🚀 傳入 is_duty_officer 給物流引擎以換取正確的迷彩
        run_logistics_engine(sb, config, now_iso, s_log_func, my_blacklist, is_duty_officer)
    
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


def run_logistics_engine(sb, config, now_iso, s_log_func, my_blacklist, is_duty_officer=True):
    query = sb.table("mission_queue").select("*, mission_program_master(*)").eq("scrape_status", "success").is_("r2_url", "null").lte("troop2_start_at", now_iso).order("created_at", desc=True)\
        .limit(1)
    tasks = query.execute().data or []
    if not tasks: return
    
    s3 = get_s3_client()
    bucket = os.environ.get("R2_BUCKET_NAME")
    worker_id = config.get('WORKER_ID', 'UNKNOWN')
    
    # 🚀 [Jitter 1] 進入外部伺服器前的初步擬人化延遲
    time.sleep(random.uniform(2.0, 5.0))
    
    for idx, m in enumerate(tasks):
        if idx > 0:
            time.sleep(random.uniform(5.0, 12.0))

        f_url = m.get('audio_url')
        if not f_url: continue
        target_domain = urlparse(f_url).netloc
        if any(b in target_domain for b in my_blacklist): continue

        ext = os.path.splitext(urlparse(f_url).path)[1] or ".mp3"
        tmp_path = f"/tmp/dl_{m['id'][:8]}{ext}"
        
        try:

            # 🚀 核心擬態：向迷彩庫申請【成套】動態偽裝
            camo_gear = get_tactical_camouflage(worker_id, is_duty_officer)
            dynamic_headers = camo_gear["headers"]
            tls_fingerprint = camo_gear["impersonate"]
            
            # 🚀 戰術升級：使用動態配對的 TLS 指紋啟動 Session，達成表裡一致
            with requests.Session(impersonate=tls_fingerprint) as session:
                # 💡 拆除 __enter__ 炸彈：不使用 with，改為直接賦值
                r = session.get(f_url, stream=True, timeout=180, headers=dynamic_headers)
                try:
                    r.raise_for_status()
                    with open(tmp_path, 'wb') as f:
                        # 💡 3MB 分片下載，每片休息 0.5s，規避流量異常偵測
                        for chunk in r.iter_content(chunk_size=3 * 1024 * 1024): 
                            if chunk: 
                                f.write(chunk)
                                time.sleep(0.5) 
                finally:
                    # 💡 安全收尾：明確關閉連線
                    r.close()
                    
            s3.upload_file(tmp_path, bucket, os.path.basename(tmp_path))
            sb.table("mission_queue").update({"scrape_status": "completed", "r2_url": os.path.basename(tmp_path)}).eq("id", m['id']).execute()
            s_log_func(sb, "DOWNLOAD", "SUCCESS", f"✅ 物資入庫: {m['id'][:8]}")
            downloaded_count += 1  # 👈 🚨 關鍵修補：任務完成，計數器 +1！
            
        except requests.exceptions.HTTPError as he:
            status_code = getattr(he.response, 'status_code', 0)
            if status_code in [403, 401, 429]:
                s_log_func(sb, "DOWNLOAD", "ERROR", f"🚫 [{worker_id}] 遭封鎖 ({status_code})")
                victim_freeze = (datetime.now(timezone.utc) + timedelta(hours=24)).isoformat()
                ally_freeze = (datetime.now(timezone.utc) + timedelta(hours=12)).isoformat()
                sb.table("pod_scra_rules").insert([
                    {"worker_id": worker_id, "domain": target_domain, "rule_type": "AUTO_COOLDOWN", "expired_at": victim_freeze},
                    {"worker_id": "ALL", "domain": target_domain, "rule_type": "VIGILANCE", "expired_at": ally_freeze}
                ]).execute()
            else:
                s_log_func(sb, "DOWNLOAD", "ERROR", f"❌ 搬運異常: {status_code}")
        except Exception as e: 
            s_log_func(sb, "DOWNLOAD", "ERROR", f"❌ 搬運失敗: {str(e)}")
        finally:
            if os.path.exists(tmp_path): os.remove(tmp_path)
            gc.collect()
