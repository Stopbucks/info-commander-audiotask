# ---------------------------------------------------------
# src/pod_scra_intel_core.py v5.1 (全軍統一絕對防禦版)
# 適用部隊：FLY, RENDER, KOYEB, ZEABUR (256MB) | HF, DBOS (512MB)
# 任務：1. 依據戰力自動切換「開路壓縮」或「輕量轉譯」
# 2. 完全委託 techcore 處理網路與資料庫，實現極致防爆
# 3. 視線穿透與幽靈輾壓，徹底根除產線堵塞
# 4. 【V5.1 新增】通訊絕對綁定：TG 發送失敗絕對不允許結案！
# ---------------------------------------------------------
import os, time, random, gc, traceback
from supabase import create_client

# 🚀 引入特種兵與軍械兵
from src.pod_scra_intel_r2 import compress_task_to_opus  
from src.pod_scra_intel_groqcore import GroqFallbackAgent
from src.pod_scra_intel_techcore import (
    fetch_stt_tasks, fetch_summary_tasks, upsert_intel_status, 
    update_intel_success, delete_intel_task, call_groq_stt, 
    call_gemini_summary, parse_intel_metrics, send_tg_report
)

def get_secrets():
    """集中管理所有外部金鑰與環境變數"""
    return {
        "SB_URL": os.environ.get("SUPABASE_URL"), "SB_KEY": os.environ.get("SUPABASE_KEY"),
        "GROQ_KEY": os.environ.get("GROQ_API_KEY"), "GEMINI_KEY": os.environ.get("GEMINI_API_KEY"),
        "TG_TOKEN": os.environ.get("TELEGRAM_BOT_TOKEN"), "TG_CHAT": os.environ.get("TELEGRAM_CHAT_ID"),
        "R2_URL": os.environ.get("R2_PUBLIC_URL")
    }

def get_sb():
    s = get_secrets()
    return create_client(s["SB_URL"], s["SB_KEY"])

# =========================================================
# 🎤 第一棒：Audio to STT (視線穿透與自動壓縮版)
# =========================================================
def run_audio_to_stt_mission(sb=None):
    time.sleep(random.uniform(3.0, 8.0))
    if not sb: sb = get_sb()
    s = get_secrets()
    worker_id = os.environ.get("WORKER_ID", "UNKNOWN_NODE")
    mem_tier = int(os.environ.get("MEMORY_TIER", 256))
    
    print(f"🔍 [{worker_id}] 啟動 STT 決策雷達 (戰力: {mem_tier}MB)...")
    
    # 1. 委託軍械兵取得合適任務 (內含 limit=10 視線穿透)
    tasks = fetch_stt_tasks(sb, mem_tier)
    if not tasks: 
        print(f"🛌 [{worker_id}] 目前無適合 {mem_tier}MB 體量之任務。")
        return

    processed = 0 
    
    for task in tasks:
        if processed >= 1: break # 安全限制：一次只處理一筆
        
        task_id = task['id']
        r2_url = str(task.get('r2_url') or '').lower()
        
        # 🚀 關鍵防禦：檢查是否已存在，若是殘留幽靈則跳過找下一筆！
        check = sb.table("mission_intel").select("intel_status").eq("task_id", task_id).execute()
        if check.data:
            print(f"⏩ 任務 {task.get('source_name')} 已存在(狀態:{check.data[0].get('intel_status')})，尋找下一筆...")
            continue 

        print(f"🎯 [{worker_id}] 鎖定目標: {task.get('source_name')} (大小: {task.get('audio_size_mb')}MB)")
        processed += 1 

        try:
            # --- 🛠️ 階段 A：512MB 重裝部隊專屬壓縮 ---
            if mem_tier >= 512 and (r2_url.endswith('.mp3') or r2_url.endswith('.m4a')):
                print(f"⚙️ [{worker_id}] 重裝戰力偵測！啟動 FFmpeg 壓縮引擎...")
                success, new_url = compress_task_to_opus(task_id, task['r2_url'])
                if success:
                    print(f"✅ [{worker_id}] 壓縮成功: {new_url}，接續進入 AI 轉譯！")
                    sb.table("mission_queue").update({"r2_url": new_url, "audio_ext": ".opus", "audio_size_mb": 5}).eq("id", task_id).execute()
                    r2_url = new_url.lower()
                    task['r2_url'] = new_url
                else:
                    print(f"❌ [{worker_id}] 壓縮失敗，放棄此任務。")
                    continue # 壓縮失敗則不進入轉譯，直接結束本回合

            # --- 🛠️ 階段 B：分流與幽靈輾壓 ---
            #chosen_provider = random.choice(["GROQ", "GEMINI"])
            chosen_provider = "GEMINI"  # 🚨 戰術強制：避開 Groq 503 災區
            print(f"🎲 [{worker_id}] 戰術分流 -> [{chosen_provider}]")
            upsert_intel_status(sb, task_id, "Sum.-proc", chosen_provider)

            # --- 🛠️ 階段 C：火力打擊 ---
            if chosen_provider == "GROQ":
                print(f"📤 [{worker_id}] 呼叫 Groq 砲火支援...")
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
                # 🚀 404 黑洞偵測
                if '404' in err_str and 'Not Found' in err_str:
                    print(f"🕳️ [{worker_id}] 踩到 404 炸彈！退回物流佇列重新下載！")
                    sb.table("mission_queue").update({"r2_url": None, "scrape_status": "pending"}).eq("id", task_id).execute()
            
        gc.collect()

# =========================================================
# ✍️ 第二棒：STT to Summary (指揮官決策層 - 絕對綁定版)
# =========================================================
def run_stt_to_summary_mission(sb=None):
    time.sleep(random.uniform(3.0, 8.0))
    if not sb: sb = get_sb()
    s = get_secrets()
    worker_id = os.environ.get("WORKER_ID", "UNKNOWN_NODE")
    
    # 1. 委託軍械兵視線穿透，取得前 10 筆任務
    tasks = fetch_summary_tasks(sb)
    processed_count = 0
    
    for intel in tasks:
        if processed_count >= 1: break
        
        task_id = intel['task_id']
        provider = intel['ai_provider']
        q_data = intel.get('mission_queue') or {}
        r2_file = str(q_data.get('r2_url') or '').lower()
        
        # 🛡️ 防禦：只吃 .opus 或 .ogg，過濾異常狀態
        if not any(ext in r2_file for ext in ['.opus', '.ogg']): 
            continue

        print(f"✍️ [{worker_id}] 啟動摘要產線: {provider} | 任務: {q_data.get('episode_title', '')[:15]}...")
        processed_count += 1
        
        p_res = sb.table("pod_scra_metadata").select("content").eq("key_name", "PROMPT_FALLBACK").single().execute()
        sys_prompt = p_res.data['content'] if p_res.data else "請分析情報。"

        try:
            summary = ""
            if provider == "GROQ":
                print(f"🛡️ [{worker_id}] 呼叫 GroqFallback 特種兵...")
                groq_agent = GroqFallbackAgent()
                summary = groq_agent.generate_summary(intel['stt_text'], sys_prompt)

            elif provider == "GEMINI":
                print(f"📤 [{worker_id}] 呼叫 Gemini 砲火支援...")
                summary = call_gemini_summary(s, q_data['r2_url'], sys_prompt)

            # 3. 處理戰果 (絕對綁定防禦：先發報，後結案！)
            if summary:
                metrics = parse_intel_metrics(summary)
                
                print(f"📡 [{worker_id}] 摘要完成，準備發送 TG 戰報...")
                # 🚀 這裡會呼叫 techcore 的 send_tg_report。如果失敗，techcore 會拋出 Exception，直接跳到 except 區塊！
                send_tg_report(s, q_data.get('source_name', '未知'), q_data.get('episode_title', '未知'), summary)
                
                # 🚀 只有上面的發報沒爆炸，程式才會走到這行進行資料庫結案
                update_intel_success(sb, task_id, summary, metrics["score"])
                print(f"🎉 [{worker_id}] 戰報發送成功，摘要已安全結案！")

        except Exception as e:
            err_str = str(e)
            print(f"❌ [{worker_id}] 第二棒(摘要或發報)崩潰: {err_str}")
            print(traceback.format_exc()) # 印出詳細錯誤，方便追查是否為 TG 報錯
            
            # 🚀 404 炸彈拆除 (摘要階段也有可能踩到)
            if '404' in err_str and 'Not Found' in err_str:
                print(f"🕳️ [{worker_id}] 摘要時踩到 404 炸彈！抹除 R2 連結，退回物流佇列！")
                delete_intel_task(sb, task_id)
                sb.table("mission_queue").update({"r2_url": None, "scrape_status": "pending"}).eq("id", task_id).execute()
        
        finally:
            gc.collect()