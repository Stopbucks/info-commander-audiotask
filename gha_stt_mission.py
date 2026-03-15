# ---------------------------------------------------------
# 此程式碼： gha_stt_mission.py V5.3 (絕對綁定與避災升級版)
# 任務： (GitHub Actions 專用 - 支援音檔救援與摘要發報)
# 代號：AUDIO_EAT
# 修正：拔除自建的 LIMIT 迴圈與繁瑣呼叫，直接對接 core.py V5.3 火控中心
# ---------------------------------------------------------
import os, time, gc, traceback
from datetime import datetime, timezone
from supabase import create_client

from src.pod_scra_intel_core import run_audio_to_stt_mission, run_stt_to_summary_mission

# =========================================================
# ⚙️ 戰術參數設定區
# =========================================================
# 💡 模式切換：可設為 "AUDIO" (第一棒), "SUMMARY" (第二棒), 或是 "ALL" (全餐)
MISSION_MODE = os.environ.get("MISSION_MODE", "ALL").upper()
# =========================================================

def get_sb():
    return create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY"))

def run_gha_assault():
    sb = get_sb()
    worker_id = os.environ.get("WORKER_ID", "AUDIO_EAT")
    now_iso = datetime.now(timezone.utc).isoformat()

    print(f"🚀 [{worker_id}] 吞噬特遣隊上線！目前模式: [{MISSION_MODE}]")

    try:
        # --- 步驟 0: 戰情室報到 ---
        t_res = sb.table("pod_scra_tactics").select("*").eq("id", 1).single().execute()
        if t_res.data:
            tactic = t_res.data
            w_status = tactic.get("worker_status") or {}
            w_health = tactic.get("workers_health") or {}
            tick_key = f"{worker_id}_tick"
            w_status[tick_key] = w_status.get(tick_key, 0) + 1  
            w_health[worker_id] = now_iso
            sb.table("pod_scra_tactics").update({"worker_status": w_status, "workers_health": w_health, "last_heartbeat_at": now_iso}).eq("id", 1).execute()

        # =========================================================
        # 模式 1: 音檔救援 (壓縮 + STT)
        # =========================================================
        if MISSION_MODE in ["AUDIO", "ALL"]:
            print(f"\n--- 🎤 階段 A: 執行壓縮與 STT 轉譯任務 ---")
            # 🚀 V5.3 核心：直接呼叫 core.py 的標準流程，由 core.py 面板控制產能與撤退時間！
            run_audio_to_stt_mission(sb)
            gc.collect()

        # =========================================================
        # 模式 2: 文字摘要與 TG 發報
        # =========================================================
        if MISSION_MODE in ["SUMMARY", "ALL"]:
            print(f"\n--- 📝 階段 B: 執行摘要與 TG 發報 ---")
            # 🚀 V5.3 核心：直接呼叫 core.py 的標準流程
            run_stt_to_summary_mission(sb)
            gc.collect()

        sb.table("mission_logs").insert({"worker_id": worker_id, "task_type": "GHA_ASSAULT", "status": "SUCCESS", "message": f"🚀 [{worker_id}] 吞噬排程執行完畢。"}).execute()
        print("\n🏁 任務圓滿結束。")

    except Exception as e:
        err_msg = str(e)
        print(f"💥 [{worker_id}] 全域崩潰: {err_msg}")
        print(traceback.format_exc())
        try:
            sb.table("mission_logs").insert({"worker_id": worker_id, "task_type": "GHA_ASSAULT", "status": "ERROR", "message": f"💥 戰場崩潰: {err_msg}"}).execute()
            sb.table("pod_scra_tactics").update({"last_error_type": f"⚠️ [吞噬特遣] {worker_id} 異常: {err_msg}"[:200]}).eq("id", 1).execute()
        except: pass
        raise e 

if __name__ == "__main__":
    run_gha_assault()