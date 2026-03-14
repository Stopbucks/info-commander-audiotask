# ---------------------------------------------------------
# src/pod_scra_intel_core.py (全軍統一 V5.2 拆彈裝甲版)
# 任務：1. 自動切換重裝/輕裝 2. 攔截 23505 友軍搶單
# 3. 🚨 偵測 404 黑洞：自動抹除 r2_url 並退回重抓
# ---------------------------------------------------------
import os, time, random, gc
from supabase import create_client

from src.pod_scra_intel_r2 import compress_task_to_opus 
from src.pod_scra_intel_groqcore import GroqFallbackAgent
from src.pod_scra_intel_techcore import (
    fetch_stt_tasks, fetch_summary_tasks, upsert_intel_status, 
    update_intel_success, delete_intel_task, call_groq_stt, 
    call_gemini_summary, parse_intel_metrics, send_tg_report
)

def get_secrets():
    return {
        "SB_URL": os.environ.get("SUPABASE_URL"), "SB_KEY": os.environ.get("SUPABASE_KEY"),
        "GROQ_KEY": os.environ.get("GROQ_API_KEY"), "GEMINI_KEY": os.environ.get("GEMINI_API_KEY"),
        "TG_TOKEN": os.environ.get("TELEGRAM_BOT_TOKEN"), "TG_CHAT": os.environ.get("TELEGRAM_CHAT_ID"),
        "R2_URL": os.environ.get("R2_PUBLIC_URL")
    }

def get_sb(): return create_client(get_secrets()["SB_URL"], get_secrets()["SB_KEY"])

def run_audio_to_stt_mission(sb=None):
    time.sleep(random.uniform(3.0, 8.0))
    if not sb: sb = get_sb()
    s = get_secrets()
    worker_id = os.environ.get("WORKER_ID", "UNKNOWN_NODE")
    mem_tier = int(os.environ.get("MEMORY_TIER", 256))
    
    print(f"🔍 [{worker_id}] 啟動 STT 決策雷達 (戰力: {mem_tier}MB)...")
    
    tasks = fetch_stt_tasks(sb, mem_tier)
    if not tasks: 
        print(f"🛌 [{worker_id}] 目前無適合 {mem_tier}MB 體量之任務。")
        return

    processed = 0 
    for task in tasks:
        if processed >= 1: break 
        
        task_id = task['id']; r2_url = str(task.get('r2_url') or '').lower()
        
        check = sb.table("mission_intel").select("intel_status").eq("task_id", task_id).execute()
        if check.data:
            print(f"⏩ 任務 {task.get('source_name')} 已存在，尋找下一筆...")
            continue 

        print(f"🎯 [{worker_id}] 鎖定目標: {task.get('source_name')} (大小: {task.get('audio_size_mb')}MB)")
        processed += 1 

        try:
            if mem_tier >= 512 and (r2_url.endswith('.mp3') or r2_url.endswith('.m4a')):
                print(f"⚙️ [{worker_id}] 啟動 FFmpeg 壓縮引擎...")
                success, new_url = compress_task_to_opus(task_id, task['r2_url'])
                if success:
                    sb.table("mission_queue").update({"r2_url": new_url, "audio_ext": ".opus", "audio_size_mb": 5}).eq("id", task_id).execute()
                    r2_url = new_url.lower()
                else:
                    raise Exception("FFmpeg Compression Failed")

            chosen_provider = random.choice(["GROQ", "GEMINI"])
            print(f"🎲 [{worker_id}] 戰術分流 -> [{chosen_provider}]")
            upsert_intel_status(sb, task_id, "Sum.-proc", chosen_provider)

            if chosen_provider == "GROQ":
                stt_text = call_groq_stt(s, r2_url)
                upsert_intel_status(sb, task_id, "Sum.-pre", stt_text=stt_text)
                print(f"✅ [{worker_id}] GROQ 轉譯成功")
            else:
                upsert_intel_status(sb, task_id, "Sum.-pre", stt_text="[GEMINI_2.5_NATIVE_STREAM]")
                print(f"✅ [{worker_id}] GEMINI 鎖定原生流")

        except Exception as e:
            err_str = str(e)
            if '23505' in err_str or 'duplicate key' in err_str.lower():
                print(f"🤝 [{worker_id}] 競態攔截：任務已被友軍先行接管，自動撤退！")
            else:
                print(f"💥 [{worker_id}] 第一棒打擊失敗: {err_str}")
                delete_intel_task(sb, task_id)
                
                # 🚀 V5.2 拆彈剪刀：偵測 404 黑洞
                if '404' in err_str and 'Not Found' in err_str:
                    print(f"🕳️ [{worker_id}] 踩到 404 炸彈！抹除 R2 連結，退回物流佇列重新下載！")
                    sb.table("mission_queue").update({"r2_url": None, "scrape_status": "pending"}).eq("id", task_id).execute()
                else:
                    raise e # 只有非 404 的真實崩潰才觸發軟失敗
        finally:
            gc.collect()

def run_stt_to_summary_mission(sb=None):
    time.sleep(random.uniform(3.0, 8.0))
    if not sb: sb = get_sb()
    s = get_secrets()
    worker_id = os.environ.get("WORKER_ID", "UNKNOWN_NODE")
    
    tasks = fetch_summary_tasks(sb)
    processed_count = 0
    for intel in tasks:
        if processed_count >= 1: break
        
        task_id = intel['task_id']; provider = intel['ai_provider']
        q_data = intel.get('mission_queue') or {}; r2_file = str(q_data.get('r2_url') or '').lower()
        if not any(ext in r2_file for ext in ['.opus', '.ogg']): continue

        print(f"✍️ [{worker_id}] 啟動摘要產線: {provider} | 任務: {q_data.get('episode_title', '')[:15]}...")
        processed_count += 1
        
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
                update_intel_success(sb, task_id, summary, metrics["score"])
                print(f"💾 [{worker_id}] 摘要已安全結案。")
                send_tg_report(s, q_data.get('source_name', '未知'), q_data.get('episode_title', '未知'), summary)

        except Exception as e:
            err_str = str(e)
            print(f"❌ [{worker_id}] 第二棒崩潰: {err_str}")
            
            # 🚀 V5.2 拆彈剪刀 (摘要階段也有可能踩到 404，特別是 Gemini 原生流)
            if '404' in err_str and 'Not Found' in err_str:
                print(f"🕳️ [{worker_id}] 摘要時踩到 404 炸彈！抹除 R2 連結與殘存情報，退回物流佇列！")
                delete_intel_task(sb, task_id)
                sb.table("mission_queue").update({"r2_url": None, "scrape_status": "pending"}).eq("id", task_id).execute()
            else:
                raise e 
        finally:
            gc.collect()