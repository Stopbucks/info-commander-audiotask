# 
# ---------------------------------------------------------
# 此程式碼： gha_stt_mission.py V3.5 (滿血聲納版)
# 任務： (GitHub Actions 專用 - 專職消化第一棒)
# 代號：AUDIO_EAT
# ---------------------------------------------------------
import os, time, random, gc, traceback
from datetime import datetime, timezone
from supabase import create_client

from src.pod_scra_r2 import compress_task_to_opus 
from src.pod_scra_intel_techcore import (
    upsert_intel_status, delete_intel_task, call_groq_stt
)

# =========================================================
# ⚙️ 戰術參數設定區 (指揮官請在此調整數量)
# =========================================================
COMPRESS_LIMIT = 3   # 🚀 每次排程壓縮數量
STT_LIMIT = 3        # 🚀 每次排程 AI 轉譯數量
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
            w_status = tactic.get("worker_status") or {}
            w_health = tactic.get("workers_health") or {}
            
            tick_key = f"{worker_id}_tick"
            w_status[tick_key] = w_status.get(tick_key, 0) + 1  
            w_health[worker_id] = now_iso
            
            sb.table("pod_scra_tactics").update({
                "worker_status": w_status, 
                "workers_health": w_health, 
                "last_heartbeat_at": now_iso
            }).eq("id", 1).execute()
            print(f"📡 [{worker_id}] 戰情室簽到成功！目前累積出勤次數 (Tick): {w_status[tick_key]}")

        # ---------------------------------------------------------
        # 📦 階段 A：壓縮任務 (加上時間防線 & 聲納網)
        # ---------------------------------------------------------
        print(f"\n--- 📦 階段 A: 執行壓縮任務 ---")
        c_query = sb.table("view_worker_task_inbox").select("*")\
                    .not_.ilike("r2_url", "%.opus").not_.ilike("r2_url", "%.ogg")\
                    .not_.is_("r2_url", "null").limit(COMPRESS_LIMIT).execute()
        
        for t in (c_query.data or []):
            if time.time() - start_time > SAFE_DURATION_SECONDS:
                print("⚠️ [撤離警報] 已達 30 分鐘限制，停止領取壓縮任務！")
                break 

            t_id, r2_url = t['id'], t.get('r2_url', '')
            print(f"⚙️ [壓縮鎖定] {t.get('source_name')}")
            
            # 🚨 關鍵聲納：捕捉壓縮過程中的任何異常，防止無聲閃退
            try:
                success, new_url = compress_task_to_opus(t_id, r2_url)
                if success:
                    sb.table("mission_queue").update({"r2_url": new_url, "audio_ext": ".opus", "audio_size_mb": 5}).eq("id", t_id).execute()
                    print(f"✅ 壓縮成功: {new_url}")
                else:
                    print(f"❌ 壓縮回傳失敗 (可能檔案損毀或引擎異常)。")
            except Exception as comp_e:
                print(f"🔥 [致命錯誤] 壓縮引擎崩潰: {str(comp_e)}")
                print(traceback.format_exc()) 
            finally:
                gc.collect()

        # ---------------------------------------------------------
        # 🎤 階段 B：STT 轉譯 (加上時間防線)
        # ---------------------------------------------------------
        print(f"\n--- 🎤 階段 B: 執行 STT 轉譯任務 ---")
        s_query = sb.table("view_worker_task_inbox").select("*")\
                    .ilike("r2_url", "%.opus").order("audio_size_mb", desc=True)\
                    .limit(STT_LIMIT).execute()
        
        for t in (s_query.data or []):
            if time.time() - start_time > SAFE_DURATION_SECONDS:
                print("⚠️ [撤離警報] 已達 30 分鐘限制，停止領取轉譯任務！")
                break 

            t_id, r2_url = t['id'], t.get('r2_url', '')
            check = sb.table("mission_intel").select("intel_status").eq("task_id", t_id).execute()
            if check.data:
                print(f"⏩ 任務已存在，跳過。")
                continue 

            print(f"🎯 [STT 鎖定] {t.get('source_name')} ({t.get('audio_size_mb')}MB)")
            
            try:
                chosen_provider = random.choice(["GROQ", "GEMINI"])
                upsert_intel_status(sb, t_id, "Sum.-proc", chosen_provider)
                if chosen_provider == "GROQ":
                    stt_text = call_groq_stt(secrets, r2_url)
                    upsert_intel_status(sb, t_id, "Sum.-pre", stt_text=stt_text)
                    print(f"✅ GROQ 轉譯成功")
                else:
                    upsert_intel_status(sb, t_id, "Sum.-pre", stt_text="[GEMINI_2.5_NATIVE_STREAM]")
                    print(f"✅ GEMINI 鎖定原生流")
            except Exception as e:
                err_str = str(e)
                if '23505' in err_str or 'duplicate key' in err_str.lower():
                    print(f"🤝 競態攔截！")
                else:
                    print(f"💥 STT 打擊失敗: {err_str}")
                    delete_intel_task(sb, t_id)
                    if '404' in err_str and 'Not Found' in err_str:
                        print(f"🕳️ 踩到 404 炸彈！退回物流！")
                        sb.table("mission_queue").update({"r2_url": None, "scrape_status": "pending"}).eq("id", t_id).execute()
            finally:
                gc.collect()

        # --- 步驟 C: 任務成功回報 ---
        sb.table("mission_logs").insert({
            "worker_id": worker_id, "task_type": "GHA_ASSAULT",
            "status": "SUCCESS", "message": f"🚀 [{worker_id}] 吞噬排程執行完畢。"
        }).execute()
        print("\n🏁 任務圓滿結束。")

    except Exception as e:
        err_msg = str(e)
        print(f"💥 [{worker_id}] 全域崩潰: {err_msg}")
        print(traceback.format_exc())
        try:
            sb.table("mission_logs").insert({
                "worker_id": worker_id, "task_type": "GHA_ASSAULT",
                "status": "ERROR", "message": f"💥 戰場崩潰: {err_msg}"
            }).execute()
            sb.table("pod_scra_tactics").update({
                "last_error_type": f"⚠️ [吞噬特遣] {worker_id} 異常: {err_msg}"[:200]
            }).eq("id", 1).execute()
        except: pass
        raise e 

if __name__ == "__main__":
    run_gha_assault()