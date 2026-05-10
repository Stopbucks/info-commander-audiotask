# ---------------------------------------------------------
# 此程式碼： gha_stt_mission.py V6.0 (GHA 專用：內部防死鎖與致命追蹤版)
# 任務： (GitHub Actions 專用 - 支援音檔救援與摘要發報)
# 代號：AUDIO_EAT
# [V6.0 升級] 融合一班部隊的防禦機制：
# 1. 導入 db_jitter 微延遲避震，防止多台 GHA 併發寫入時造成 Lock。
# 2. 解除致命崩潰消音器，確保 OOM 等底層錯誤能正確寫入 DB 面板。
# 3. 捨棄 Flask 與 APScheduler (GHA 不需要)，維持「執行完即撤退」的刺客本色。
# ---------------------------------------------------------
import os, time, gc, random, traceback
from datetime import datetime, timezone
from supabase import create_client

from src.pod_scra_intel_core import run_audio_to_stt_mission, run_stt_to_summary_mission

# =========================================================
# ⚙️ 戰術參數設定區
# =========================================================
MISSION_MODE = os.environ.get("MISSION_MODE", "ALL").upper()
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

def run_gha_assault():
    sb = get_sb()
    now_iso = datetime.now(timezone.utc).isoformat()

    print(f"🚀 [{WORKER_ID} V6.0] 吞噬特遣隊上線！目前模式: [{MISSION_MODE}]")

    try:
        # --- 步驟 0: 戰情室報到 (微延遲避震) ---
        db_jitter()
        t_res = sb.table("pod_scra_tactics").select("*").eq("id", 1).single().execute()
        if t_res.data:
            tactic = t_res.data
            w_status = tactic.get("worker_status") or {}
            w_health = tactic.get("workers_health") or {}
            tick_key = f"{WORKER_ID}_tick"
            w_status[tick_key] = w_status.get(tick_key, 0) + 1  
            w_health[WORKER_ID] = now_iso
            
            db_jitter()
            sb.table("pod_scra_tactics").update({"worker_status": w_status, "workers_health": w_health, "last_heartbeat_at": now_iso}).eq("id", 1).execute()

        # =========================================================
        # 模式 1: 音檔救援 (壓縮 + STT)
        # =========================================================
        if MISSION_MODE in ["AUDIO", "ALL"]:
            print(f"\n--- 🎤 階段 A: 執行壓縮與 STT 轉譯任務 ---")
            run_audio_to_stt_mission(sb)
            gc.collect()

        # =========================================================
        # 模式 2: 文字摘要與 TG 發報
        # =========================================================
        if MISSION_MODE in ["SUMMARY", "ALL"]:
            print(f"\n--- 📝 階段 B: 執行摘要與 TG 發報 ---")
            run_stt_to_summary_mission(sb)
            gc.collect()

        db_jitter()
        sb.table("mission_logs").insert({"worker_id": WORKER_ID, "task_type": "GHA_ASSAULT", "status": "SUCCESS", "message": f"🚀 [{WORKER_ID}] 吞噬排程執行完畢。"}).execute()
        print("\n🏁 任務圓滿結束，特遣隊撤退。")

    except Exception as e:
        err_msg = str(e)
        print(traceback.format_exc())
        report_soft_failure(sb, WORKER_ID, err_msg) # 🚀 呼叫 V6.0 致命回報
        
        try:
            db_jitter()
            sb.table("mission_logs").insert({"worker_id": WORKER_ID, "task_type": "GHA_ASSAULT", "status": "ERROR", "message": f"💥 戰場崩潰: {err_msg}"}).execute()
        except: pass
        
        # 故意引發錯誤讓 GHA 亮紅燈，方便您在 GitHub 上查看
        raise e 

if __name__ == "__main__":
    run_gha_assault()
