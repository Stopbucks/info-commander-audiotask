# ---------------------------------------------------------
# 此程式碼： gha_stt_mission.py V6.1 (GHA 專用：終極純血搬運版)
# 任務： (GitHub Actions 專用 - 純壓縮搬運工)
# 代號：AUDIO_EAT
# [V6.1 升級] 戰術大破大立！
# 1. 徹底斷開與 core.py 及 techcore.py 的依賴。即便 AI 模組發生 SyntaxError，
#    也不會影響本機甲的啟動。
# 2. 直接對接 trans.py 的 run_logistics_engine 執行純粹的下載與壓縮。
# 3. 捨棄多餘的 MISSION_MODE 判斷，貫徹「鎖死第一拍」的清淤使命。
# ---------------------------------------------------------
import os, time, gc, random, traceback
from datetime import datetime, timezone
from supabase import create_client

# 🚀 [V6.1 關鍵] 拔除對 core.py 的依賴，直接載入物流與控制面板
from src.pod_scra_intel_trans import run_logistics_engine
from src.pod_scra_intel_control import get_tactical_panel

WORKER_ID = os.environ.get("WORKER_ID", "AUDIO_EAT")

def get_sb():
    return create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY"))

def db_jitter():
    """🛡️ 隨機微延遲避震：防止多台機甲同時寫入造成資料庫 Lock 或競合"""
    time.sleep(random.uniform(0.2, 1.0))

def report_soft_failure(sb, worker_id, error_msg):
    """🚨 [V6.0 升級] 致命錯誤面板回報"""
    print(f"🔥 [致命崩潰] 機甲發生未預期異常: {error_msg}", flush=True)
    try:
        db_jitter() 
        res = sb.table("pod_scra_tactics").select("active_worker, consecutive_soft_failures, worker_status").eq("id", 1).single().execute()
        if not res.data: return
        tactic = res.data
        
        db_jitter()
        if worker_id == tactic.get("active_worker"):
            sb.table("pod_scra_tactics").update({
                "consecutive_soft_failures": tactic.get("consecutive_soft_failures", 0) + 1,
                "last_error_type": f"🚨 [主將] {worker_id} 崩潰: {error_msg}"[:200]
            }).eq("id", 1).execute()
        else:
            w_status = tactic.get("worker_status", {})
            w_status[f"{worker_id}_last_err"] = str(error_msg)[:100]
            sb.table("pod_scra_tactics").update({
                "worker_status": w_status,
                "last_error_type": f"⚠️ [吞噬特遣] {worker_id} 異常: {error_msg}"[:200]
            }).eq("id", 1).execute()
    except: pass

def s_log(sb, task_type, status, message, err_stack=None):
    """簡易版 S_LOG，供 trans.py 呼叫"""
    try:
        print(f"[{task_type}][{status}] {message}", flush=True)
        if status in ["SUCCESS", "ERROR"]:
            db_jitter()
            sb.table("mission_logs").insert({
                "worker_id": WORKER_ID, "task_type": task_type,
                "status": status, "message": message, "traceback": err_stack
            }).execute()
    except: pass

def run_gha_assault():
    sb = get_sb()
    now_iso = datetime.now(timezone.utc).isoformat()

    print(f"🚀 [{WORKER_ID} V6.1] 純血搬運特遣隊上線！(已斷開所有 AI 武器包)")

    try:
        # --- 步驟 0: 戰情室報到 (微延遲避震) ---
        db_jitter()
        t_res = sb.table("pod_scra_tactics").select("*").eq("id", 1).single().execute()
        if t_res.data:
            tactic = t_res.data
            w_status = tactic.get("worker_status") or {}
            w_health = tactic.get("workers_health") or {}
            tick_key = f"{WORKER_ID}_tick"
            w_status[tick_key] = 1  # 永遠鎖死在第一拍
            w_health[WORKER_ID] = now_iso
            
            db_jitter()
            sb.table("pod_scra_tactics").update({"worker_status": w_status, "workers_health": w_health, "last_heartbeat_at": now_iso}).eq("id", 1).execute()

        # =========================================================
        # 🚀 直攻核心：直接啟動物流引擎進行清淤
        # =========================================================
        print(f"\n--- 📥 啟動極限物流搬運引擎 ---")
        
        # 取得專屬的 AUDIO_EAT 面板裝備
        panel = get_tactical_panel(WORKER_ID)
        dl_limit = panel.get("DOWNLOAD_LIMIT", 5)
        max_same_domain = panel.get("MAX_SAME_DOMAIN", 2)
        panel_blacklist = panel.get("GLOBAL_DOMAIN_BLACKLIST", [])
        
        # 獲取資料庫黑名單
        rule_res = sb.table("pod_scra_rules").select("domain").in_("worker_id", [WORKER_ID, "ALL"]).gte("expired_at", now_iso).execute()
        db_blacklist = [r['domain'] for r in rule_res.data] if rule_res.data else []
        combined_blacklist = list(set(db_blacklist + panel_blacklist))
        
        # 建立一個假的 config 物件傳給 trans.py
        fake_config = {"WORKER_ID": WORKER_ID}
        
        # 執行下載與壓縮
        run_logistics_engine(sb, fake_config, now_iso, s_log, combined_blacklist, dl_limit, max_same_domain, is_duty_officer=False)
        
        gc.collect()

        db_jitter()
        sb.table("mission_logs").insert({"worker_id": WORKER_ID, "task_type": "GHA_ASSAULT", "status": "SUCCESS", "message": f"🚀 [{WORKER_ID}] 吞噬排程執行完畢。"}).execute()
        print("\n🏁 任務圓滿結束，特遣隊撤退。")

    except Exception as e:
        err_msg = str(e)
        print(traceback.format_exc())
        report_soft_failure(sb, WORKER_ID, err_msg) 
        
        try:
            db_jitter()
            sb.table("mission_logs").insert({"worker_id": WORKER_ID, "task_type": "GHA_ASSAULT", "status": "ERROR", "message": f"💥 戰場崩潰: {err_msg}"}).execute()
        except: pass
        
        raise e 

if __name__ == "__main__":
    run_gha_assault()
