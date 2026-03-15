# ---------------------------------------------------------
# src/pod_scra_intel_core.py v5.5 (全軍單一純粹流程版)
# 適用部隊：ALL (FLY, RENDER, KOYEB, ZEABUR, DBOS, HF)
# 任務：專注於 STT 與 Summary 的核心戰鬥流程。
# 架構突破：完全剝離控制面板與連線設定，移至 src.pod_scra_intel_control
# ---------------------------------------------------------
import os, time, random, gc
from src.pod_scra_intel_control import get_tactical_panel, get_sb, get_secrets # 🚀 引入外部指揮所
from src.pod_scra_intel_r2 import compress_task_to_opus  
from src.pod_scra_intel_groqcore import GroqFallbackAgent
from src.pod_scra_intel_techcore import (
    fetch_stt_tasks, fetch_summary_tasks, upsert_intel_status, 
    update_intel_success, delete_intel_task, call_groq_stt, 
    call_gemini_summary, parse_intel_metrics, send_tg_report,
    increment_soft_failure
)

# =========================================================
# 🎤 第一棒：Audio to STT 
# =========================================================
def run_audio_to_stt_mission(sb=None):
    start_time = time.time()
    worker_id = os.environ.get("WORKER_ID", "UNKNOWN_NODE")
    
    # 🚀 向指揮所申請專屬戰術面板
    panel = get_tactical_panel(worker_id)
    
    if panel["STT_LIMIT"] <= 0:
        print(f"⏸️ [{worker_id}] 面板指示：不參與 STT 轉譯產線。")
        return

    time.sleep(random.uniform(3.0, 8.0))
    if not sb: sb = get_sb()
    s = get_secrets()
    
    print(f"🔍 [{worker_id}] 啟動 STT 雷達 (戰力: {panel['MEM_TIER']}MB | 掃描: {panel['RADAR_FETCH_LIMIT']}筆)...")
    
    tasks = fetch_stt_tasks(sb, panel["MEM_TIER"], worker_id, fetch_limit=panel["RADAR_FETCH_LIMIT"])
    if not tasks: 
        print(f"🛌 [{worker_id}] 目前無適合體量之任務。")
        return

    actual_processed = 0 
    
    for task in tasks:
        if actual_processed >= panel["STT_LIMIT"]: 
            print(f"🏁 [{worker_id}] 第一棒已達目標產能 ({panel['STT_LIMIT']} 件)，準備交接。")
            break 
        if time.time() - start_time > panel["SAFE_DURATION_SECONDS"]: 
            print(f"⏱️ [{worker_id}] 巡邏逼近安全極限 ({panel['SAFE_DURATION_SECONDS']}s)，強制撤退！")
            break

        task_id = task['id']
        r2_url = str(task.get('r2_url') or '').lower()
        
        check = sb.table("mission_intel").select("intel_status").eq("task_id", task_id).execute()
        if check.data:
            print(f"⏩ 任務 {task.get('source_name')} 已存在，尋找下一筆...")
            continue 

        print(f"🎯 [{worker_id}] 鎖定目標: {task.get('source_name')} (大小: {task.get('audio_size_mb')}MB)")

        try:
            # 🚀 根據面板權限決定是否壓縮
            if panel["CAN_COMPRESS"] and (r2_url.endswith('.mp3') or r2_url.endswith('.m4a')):
                print(f"⚙️ [{worker_id}] 面板授權壓縮！啟動 FFmpeg 引擎...")
                success, new_url = compress_task_to_opus(task_id, task['r2_url'])
                if success:
                    print(f"✅ [{worker_id}] 壓縮成功: {new_url}，接續進入 AI 轉譯！")
                    sb.table("mission_queue").update({"r2_url": new_url, "audio_ext": ".opus", "audio_size_mb": 5}).eq("id", task_id).execute()
                    r2_url = new_url.lower()
                    task['r2_url'] = new_url
                else:
                    print(f"❌ [{worker_id}] 壓縮失敗，觸發容錯推進！")
                    increment_soft_failure(sb, task_id)
                    continue 
            elif not panel["CAN_COMPRESS"] and (r2_url.endswith('.mp3') or r2_url.endswith('.m4a')):
                print(f"⛔ [{worker_id}] 權限不足：禁止執行壓縮。跳過此大檔案。")
                continue

            chosen_provider = "GROQ" if panel["SCOUT_MODE"] else "GEMINI"

            print(f"🎲 [{worker_id}] 戰術分流 -> [{chosen_provider}]")
            upsert_intel_status(sb, task_id, "Sum.-proc", chosen_provider)

            if chosen_provider == "GROQ":
                stt_text = call_groq_stt(s, r2_url)
                upsert_intel_status(sb, task_id, "Sum.-pre", stt_text=stt_text)
                print(f"✅ [{worker_id}] GROQ 轉譯成功")
            else:
                upsert_intel_status(sb, task_id, "Sum.-pre", stt_text="[GEMINI_2.5_NATIVE_STREAM]")
                print(f"✅ [{worker_id}] GEMINI 鎖定原生流")

            actual_processed += 1 

        except Exception as e:
            err_str = str(e)
            if '23505' in err_str or 'duplicate key' in err_str.lower():
                print(f"🤝 [{worker_id}] 競態攔截：任務已被友軍接管。")
            else:
                print(f"💥 [{worker_id}] 第一棒打擊失敗: {err_str}")
                delete_intel_task(sb, task_id)
                if '404' in err_str and 'Not Found' in err_str:
                    print(f"🕳️ [{worker_id}] 踩到 404 炸彈！退回物流佇列！")
                    sb.table("mission_queue").update({"r2_url": None, "scrape_status": "pending"}).eq("id", task_id).execute()
                else:
                    increment_soft_failure(sb, task_id)
            
        finally:
            gc.collect()

# =========================================================
# ✍️ 第二棒：STT to Summary 
# =========================================================
def run_stt_to_summary_mission(sb=None):
    start_time = time.time()
    worker_id = os.environ.get("WORKER_ID", "UNKNOWN_NODE")
    
    # 🚀 向指揮所申請專屬戰術面板
    panel = get_tactical_panel(worker_id)
    
    if panel["SUMMARY_LIMIT"] <= 0:
        print(f"⏸️ [{worker_id}] 面板指示：不參與摘要產線。")
        return

    time.sleep(random.uniform(3.0, 8.0))
    if not sb: sb = get_sb()
    s = get_secrets()
    
    tasks = fetch_summary_tasks(sb, fetch_limit=panel["RADAR_FETCH_LIMIT"])
    actual_processed = 0
    
    for intel in tasks:
        if actual_processed >= panel["SUMMARY_LIMIT"]: 
            print(f"🏁 [{worker_id}] 第二棒已達目標產能 ({panel['SUMMARY_LIMIT']} 件)，交接。")
            break
        if time.time() - start_time > panel["SAFE_DURATION_SECONDS"]:
            print(f"⏱️ [{worker_id}] 摘要產線逼近安全極限 ({panel['SAFE_DURATION_SECONDS']}s)，強制撤退！")
            break
            
        task_id = intel['task_id']
        provider = intel['ai_provider']
        q_data = intel.get('mission_queue') or {}
        r2_file = str(q_data.get('r2_url') or '').lower()
        
        if not r2_file or r2_file == 'null': continue 

        print(f"✍️ [{worker_id}] 啟動摘要產線: {provider} | 任務: {q_data.get('episode_title', '')[:15]}...")
        p_res = sb.table("pod_scra_metadata").select("content").eq("key_name", "PROMPT_FALLBACK").single().execute()
        sys_prompt = p_res.data['content'] if p_res.data else "請分析情報。"

        try:
            summary = ""
            if provider == "GROQ":
                groq_agent = GroqFallbackAgent()
                summary = groq_agent.generate_summary(intel['stt_text'], sys_prompt)
            elif provider == "GEMINI":
                summary = call_gemini_summary(s, q_data['r2_url'], sys_prompt)

            if summary:
                metrics = parse_intel_metrics(summary)
                send_tg_report(s, q_data.get('source_name', '未知'), q_data.get('episode_title', '未知'), summary)
                update_intel_success(sb, task_id, summary, metrics["score"])
                print(f"🎉 [{worker_id}] 戰報發送成功，摘要已安全結案！")
                actual_processed += 1 

        except Exception as e:
            err_str = str(e)
            print(f"❌ [{worker_id}] 第二棒崩潰: {err_str}")
            if '429' in err_str: print(f"⚠️ [{worker_id}] API Rate Limit，任務退回。")
            elif '404' in err_str and 'Not Found' in err_str:
                delete_intel_task(sb, task_id)
                sb.table("mission_queue").update({"r2_url": None, "scrape_status": "pending"}).eq("id", task_id).execute()
        
        finally:
            gc.collect()