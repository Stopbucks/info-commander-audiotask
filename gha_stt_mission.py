# 
# ---------------------------------------------------------
# 此程式碼： gha_stt_mission.py V1.4
# 任務： (GitHub Actions 專用 - 專職消化第一棒)
# 代號：AUDIO_EAT
# ---------------------------------------------------------
# gha_stt_mission.py (GitHub Actions 專用 - AUDIO_EAT 吞噬特遣隊 v3.5)
import os, time, random, gc
from datetime import datetime, timezone
from supabase import create_client


# 引入全軍統一的軍火庫 (修正導入檔名：移除 _intel 以對齊實體檔案)
from src.pod_scra_r2 import compress_task_to_opus 
from src.pod_scra_intel_techcore import (
    upsert_intel_status, delete_intel_task, call_groq_stt
)

# =========================================================
# ⚙️ 戰術參數設定區 (指揮官請在此調整數量)
# =========================================================
COMPRESS_LIMIT = 1   # 🚀 每次排程壓縮數量
STT_LIMIT = 1        # 🚀 每次排程 AI 轉譯數量
SAFE_DURATION_SECONDS = 1800  # 🛡️ 撤離防線：30 分鐘 (1800秒)
# =========================================================

def get_sb():
    return create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY"))

def run_gha_assault():
    start_time = time.time()  # ⏳ 記錄起始時間
    sb = get_sb()
    worker_id = os.environ.get("WORKER_ID", "AUDIO_EAT")
    now_iso = datetime.now(timezone.utc).isoformat()
    
    secrets = {
        "GROQ_KEY": os.environ.get("GROQ_API_KEY"), "GEMINI_KEY": os.environ.get("GEMINI_API_KEY"),
        "R2_URL": os.environ.get("R2_PUBLIC_URL")
    }

    print(f"🚀 [{worker_id}] 吞噬特遣隊上線！(防線設定: {SAFE_DURATION_SECONDS/60} 分鐘)")

    try:
        # --- 步驟 0: 戰情室報到 ---
        t_res = sb.table("pod_scra_tactics").select("*").eq("id", 1).single().execute()
        if t_res.data:
            tactic = t_res.data
            # 🚀 強化防護：避免資料庫欄位為 NULL 時導致程式崩潰
            w_status = tactic.get("worker_status") or {}
            w_health = tactic.get("workers_health") or {}
            
            tick_key = f"{worker_id}_tick"
            # 🚀 確保 w_status 一定是字典，現在 .get() 可以安全執行了
            w_status[tick_key] = w_status.get(tick_key, 0) + 1  
            w_health[worker_id] = now_iso
            
            sb.table("pod_scra_tactics").update({
                "worker_status": w_status, 
                "workers_health": w_health, 
                "last_heartbeat_at": now_iso
            }).eq("id", 1).execute()
            print(f"📡 [{worker_id}] 戰情室簽到成功！目前累積出勤次數 (Tick): {w_status[tick_key]}")


        # ---------------------------------------------------------
        # 📦 階段 A：壓縮任務 (加上時間防線)
        # ---------------------------------------------------------
        print(f"\n--- 📦 階段 A: 執行壓縮任務 ---")
        c_query = sb.table("view_worker_task_inbox").select("*")\
                    .not_.ilike("r2_url", "%.opus").not_.ilike("r2_url", "%.ogg")\
                    .not_.is_("r2_url", "null").limit(COMPRESS_LIMIT).execute()
        
        for t in (c_query.data or []):
            # 🛡️ 撤離檢查：如果執行已超過 30 分鐘，停止領取新任務
            if time.time() - start_time > SAFE_DURATION_SECONDS:
                print("⚠️ [撤離警報] 已達 30 分鐘限制，停止領取壓縮任務！")
                break 

            t_id, r2_url = t['id'], t.get('r2_url', '')
            print(f"⚙️ [壓縮鎖定] {t.get('source_name')}")
            success, new_url = compress_task_to_opus(t_id, r2_url)
            if success:
                sb.table("mission_queue").update({"r2_url": new_url, "audio_ext": ".opus", "audio_size_mb": 5}).eq("id", t_id).execute()
            gc.collect()

        # ---------------------------------------------------------
        # 🎤 階段 B：STT 轉譯 (加上時間防線)
        # ---------------------------------------------------------
        print(f"\n--- 🎤 階段 B: 執行 STT 轉譯任務 ---")
        s_query = sb.table("view_worker_task_inbox").select("*")\
                    .ilike("r2_url", "%.opus").order("audio_size_mb", desc=True)\
                    .limit(STT_LIMIT).execute()
        
        for t in (s_query.data or []):
            # 🛡️ 撤離檢查：進入耗時最長的 STT 前，必須確認剩餘戰鬥時間
            if time.time() - start_time > SAFE_DURATION_SECONDS:
                print("⚠️ [撤離警報] 已達 30 分鐘限制，停止領取轉譯任務！")
                break 

            t_id, r2_url = t['id'], t.get('r2_url', '')
            # --- STT 執行邏輯 (略) ---
            try:
                chosen_provider = random.choice(["GROQ", "GEMINI"])
                upsert_intel_status(sb, t_id, "Sum.-proc", chosen_provider)
                if chosen_provider == "GROQ":
                    stt_text = call_groq_stt(secrets, r2_url)
                    upsert_intel_status(sb, t_id, "Sum.-pre", stt_text=stt_text)
                else:
                    upsert_intel_status(sb, t_id, "Sum.-pre", stt_text="[GEMINI_2.5_NATIVE_STREAM]")
            except Exception as e:
                # 處理衝突與錯誤...
                delete_intel_task(sb, t_id)
            finally:
                gc.collect()

        # --- 步驟 C: 任務成功回報 (略) ---
        sb.table("mission_logs").insert({
            "worker_id": worker_id, "task_type": "GHA_ASSAULT",
            "status": "SUCCESS", "message": f"🚀 [{worker_id}] 吞噬排程執行完畢。"
        }).execute()
        print("\n🏁 任務圓滿結束。")

    except Exception as e:
        # 錯誤處理 (略)
        raise e 

if __name__ == "__main__":
    run_gha_assault()