# ---------------------------------------------------------
# 此程式碼： gha_stt_mission.py V4.1 (絕對綁定與避災升級版)
# 任務： (GitHub Actions 專用 - 支援音檔救援與摘要發報)
# 代號：AUDIO_EAT
# ---------------------------------------------------------
import os, time, random, gc, traceback
from datetime import datetime, timezone
from supabase import create_client

from src.pod_scra_r2 import compress_task_to_opus 
from src.pod_scra_intel_groqcore import GroqFallbackAgent
from src.pod_scra_intel_techcore import (
    upsert_intel_status, delete_intel_task, call_groq_stt,
    fetch_summary_tasks, call_gemini_summary, parse_intel_metrics, update_intel_success, send_tg_report
)

# =========================================================
# ⚙️ 戰術參數設定區
# =========================================================
COMPRESS_LIMIT = 3   # 📦 每次排程壓縮數量
STT_LIMIT = 2        # 🎤 每次排程 AI 轉譯數量
SUMMARY_LIMIT = 1    # 📝 每次排程摘要與 TG 發報數量
SAFE_DURATION_SECONDS = 1800  # 🛡️ 撤離防線：30 分鐘 (1800秒)

# 💡 模式切換：可設為 "AUDIO" (第一棒), "SUMMARY" (第二棒), 或是 "ALL" (全餐)
MISSION_MODE = os.environ.get("MISSION_MODE", "ALL").upper()
# =========================================================

def get_sb():
    return create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY"))

def run_gha_assault():
    start_time = time.time()
    sb = get_sb()
    worker_id = os.environ.get("WORKER_ID", "AUDIO_EAT")
    now_iso = datetime.now(timezone.utc).isoformat()
    
    # 🚨 必須補上 TG 的金鑰，否則無法發報
    secrets = {
        "GROQ_KEY": os.environ.get("GROQ_API_KEY"), 
        "GEMINI_KEY": os.environ.get("GEMINI_API_KEY"),
        "TG_TOKEN": os.environ.get("TELEGRAM_BOT_TOKEN"), 
        "TG_CHAT": os.environ.get("TELEGRAM_CHAT_ID"),
        "R2_URL": os.environ.get("R2_PUBLIC_URL")
    }

    print(f"🚀 [{worker_id}] 特遣隊上線！目前模式: [{MISSION_MODE}] (防線: 30分鐘)")

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
            print(f"\n--- 📦 階段 A: 執行壓縮任務 ---")
            c_query = sb.table("view_worker_task_inbox").select("*").not_.ilike("r2_url", "%.opus").not_.ilike("r2_url", "%.ogg").not_.is_("r2_url", "null").limit(COMPRESS_LIMIT).execute()
            for t in (c_query.data or []):
                if time.time() - start_time > SAFE_DURATION_SECONDS: break 
                t_id, r2_url = t['id'], t.get('r2_url', '')
                print(f"⚙️ [壓縮鎖定] {t.get('source_name')}")
                try:
                    success, new_url = compress_task_to_opus(t_id, r2_url)
                    if success:
                        sb.table("mission_queue").update({"r2_url": new_url, "audio_ext": ".opus", "audio_size_mb": 5}).eq("id", t_id).execute()
                        print(f"✅ 壓縮成功: {new_url}")
                except Exception as comp_e:
                    print(f"🔥 [致命錯誤] 壓縮引擎崩潰: {str(comp_e)}")
                    print(traceback.format_exc()) 
                finally: gc.collect()

            print(f"\n--- 🎤 階段 B: 執行 STT 轉譯任務 ---")
            s_query = sb.table("view_worker_task_inbox").select("*").ilike("r2_url", "%.opus").order("audio_size_mb", desc=True).limit(STT_LIMIT).execute()
            for t in (s_query.data or []):
                if time.time() - start_time > SAFE_DURATION_SECONDS: break 
                t_id, r2_url = t['id'], t.get('r2_url', '')
                check = sb.table("mission_intel").select("intel_status").eq("task_id", t_id).execute()
                if check.data: continue 

                print(f"🎯 [STT 鎖定] {t.get('source_name')}")
                try:
                    # 🚨 戰場防禦：Groq 503 當機中，強制拔除擲骰子，全軍切換 GEMINI！
                    chosen_provider = "GEMINI"
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
                    if '23505' in err_str or 'duplicate key' in err_str.lower(): print(f"🤝 競態攔截！")
                    else:
                        print(f"💥 STT 打擊失敗: {err_str}")
                        delete_intel_task(sb, t_id)
                        if '404' in err_str and 'Not Found' in err_str: sb.table("mission_queue").update({"r2_url": None, "scrape_status": "pending"}).eq("id", t_id).execute()
                finally: gc.collect()

        # =========================================================
        # 模式 2: 文字摘要與 TG 發報
        # =========================================================
        if MISSION_MODE in ["SUMMARY", "ALL"]:
            print(f"\n--- 📝 階段 C: 執行摘要與 TG 發報 ---")
            tasks = fetch_summary_tasks(sb)
            tasks_to_process = tasks[:SUMMARY_LIMIT] if tasks else []
            
            if not tasks_to_process:
                print("✅ 目前沒有需要摘要與發報的任務。")
            else:
                for intel in tasks_to_process:
                    if time.time() - start_time > SAFE_DURATION_SECONDS: break

                    t_id = intel['task_id']; provider = intel['ai_provider']
                    q_data = intel.get('mission_queue') or {}; r2_file = str(q_data.get('r2_url') or '').lower()
                    if not any(ext in r2_file for ext in ['.opus', '.ogg']): continue

                    print(f"✍️ [摘要鎖定] {provider} | 任務: {q_data.get('episode_title', '')[:15]}...")
                    p_res = sb.table("pod_scra_metadata").select("content").eq("key_name", "PROMPT_FALLBACK").single().execute()
                    sys_prompt = p_res.data['content'] if p_res.data else "請分析情報。"

                    try:
                        summary = ""
                        if provider == "GROQ":
                            groq_agent = GroqFallbackAgent()
                            summary = groq_agent.generate_summary(intel['stt_text'], sys_prompt)
                        elif provider == "GEMINI":
                            summary = call_gemini_summary(secrets, q_data['r2_url'], sys_prompt)

                        if summary:
                            metrics = parse_intel_metrics(summary)
                            
                            # 🚀 絕對綁定：先發送 Telegram！如果失敗會直接跳到 except，阻斷結案！
                            print(f"📡 準備發送 TG 戰報...")
                            send_tg_report(secrets, q_data.get('source_name', '未知'), q_data.get('episode_title', '未知'), summary)
                            
                            # 🚀 發送成功後，才允許更改資料庫狀態為「已結案」
                            update_intel_success(sb, t_id, summary, metrics["score"])
                            print(f"💾 戰報送達，摘要已安全結案！")
                        else:
                            print(f"⚠️ 摘要生成結果為空。")

                    except Exception as e:
                        err_str = str(e)
                        print(f"❌ 摘要/發報崩潰: {err_str}")
                        print(traceback.format_exc()) # 🚨 捕捉摘要或 TG 發送失敗的真兇
                        if '404' in err_str and 'Not Found' in err_str:
                            delete_intel_task(sb, t_id)
                            sb.table("mission_queue").update({"r2_url": None, "scrape_status": "pending"}).eq("id", t_id).execute()
                    finally:
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