# ---------------------------------------------------------
# src/pod_scra_intel_core.py (V5.3 主力 512MB 面板統御與真實戰果版)
# 適用：RENDER, KOYEB, ZEABUR, DBOS, HF, GHA | 規格：512MB (MEMORY_TIER="512")
# [任務] 1. 執行 FFmpeg 音檔壓縮 2. 呼叫 AI 轉譯與摘要 3. 發送 TG 戰報結案
# [機制] 產能與雷達深度完全由頂端「⚙️ 戰術控制面板」統一統御，拔除硬編碼迴圈。
# [守衛] 實裝 25 分鐘 (1500s)「軟撤退」防線，確保情報優雅結案，防平台硬超時中斷。
# [修改] 1. 徹底拔除副檔名防護罩，不再跳過 .mp3，僅略過 r2_url=null 的空包彈。
# [修改] 2. 導入「真實計數器」，真正完成 API 呼叫與資料庫寫入後，產能才算 +1。
# [修改] 3. 實裝「偵察兵機制」(GROQ_SCOUT_ID)，主力死守 Gemini，指派單兵測試 Groq。
# ---------------------------------------------------------
import os, time, random, gc, traceback
from supabase import create_client

from src.pod_scra_intel_r2 import compress_task_to_opus  
from src.pod_scra_intel_groqcore import GroqFallbackAgent
from src.pod_scra_intel_techcore import (
    fetch_stt_tasks, fetch_summary_tasks, upsert_intel_status, 
    update_intel_success, delete_intel_task, call_groq_stt, 
    call_gemini_summary, parse_intel_metrics, send_tg_report
)

# =========================================================
# ⚙️ 戰術參數控制面板 (Control Panel)
# =========================================================
RADAR_FETCH_LIMIT = 100        # 📡 雷達掃描深度：看穿舊任務塞子 (建議 50~100)
# 🚀 由於 GHA 是久久出巡一次的「吞噬特遣隊」，我們將它的產能極限拉高！
STT_LIMIT = 3                  # 🎤 第一棒 (轉譯) 每次排程處理上限 (GHA 專用加量版)
SUMMARY_LIMIT = 3              # 📝 第二棒 (摘要與TG) 每次排程處理上限 (GHA 專用加量版)
SAFE_DURATION_SECONDS = 1500   # 🛡️ 撤離防線：25 分鐘 (1500秒)。確保 GHA 安全下班
# =========================================================

def get_secrets():
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
# 🎤 第一棒：Audio to STT (V5.3 包含 Groq 偵察兵測試機制)
# =========================================================
def run_audio_to_stt_mission(sb=None):
    start_time = time.time()
    time.sleep(random.uniform(3.0, 8.0))
    if not sb: sb = get_sb()
    s = get_secrets()
    worker_id = os.environ.get("WORKER_ID", "UNKNOWN_NODE")
    
    # 🚀 若未指定記憶體，GHA 預設比照 512MB 重裝部隊火力全開！
    mem_tier = int(os.environ.get("MEMORY_TIER", 512)) 
    
    print(f"🔍 [{worker_id}] 啟動 STT 決策雷達 (戰力: {mem_tier}MB | 掃描: {RADAR_FETCH_LIMIT}筆)...")
    
    tasks = fetch_stt_tasks(sb, mem_tier, fetch_limit=RADAR_FETCH_LIMIT)
    if not tasks: 
        print(f"🛌 [{worker_id}] 目前無適合 {mem_tier}MB 體量之任務。")
        return

    actual_processed = 0 
    
    for task in tasks:
        # ⏱️ 戰術防線：數量達標，或逼近撤離時間，立刻停止接單！
        if actual_processed >= STT_LIMIT: 
            print(f"🏁 [{worker_id}] 第一棒已達目標產能 ({STT_LIMIT} 件)，準備交接。")
            break 
        if time.time() - start_time > SAFE_DURATION_SECONDS: 
            print(f"⏱️ [{worker_id}] 巡邏逼近安全極限 ({SAFE_DURATION_SECONDS}s)，強制撤退！")
            break

        task_id = task['id']
        r2_url = str(task.get('r2_url') or '').lower()
        
        check = sb.table("mission_intel").select("intel_status").eq("task_id", task_id).execute()
        if check.data:
            print(f"⏩ 任務 {task.get('source_name')} 已存在(狀態:{check.data[0].get('intel_status')})，尋找下一筆...")
            continue 

        print(f"🎯 [{worker_id}] 鎖定目標: {task.get('source_name')} (大小: {task.get('audio_size_mb')}MB)")

        try:
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
                    continue 

            # ==========================================
            # --- 🛠️ 階段 B：分流與偵察兵測試 ---
            # ==========================================
            GROQ_SCOUT_ID = "ZEABUR"  # 🎯 指定測試 GROQ 的機甲代號
            
            if worker_id == GROQ_SCOUT_ID:
                print(f"🕵️ [{worker_id}] 擔任偵察兵，嘗試切換至 GROQ 進行歸隊測試...")
                chosen_provider = "GROQ" 
            else:
                chosen_provider = "GEMINI" 

            print(f"🎲 [{worker_id}] 戰術分流 -> [{chosen_provider}]")
            upsert_intel_status(sb, task_id, "Sum.-proc", chosen_provider)

            if chosen_provider == "GROQ":
                print(f"📤 [{worker_id}] 呼叫 Groq 砲火支援...")
                stt_text = call_groq_stt(s, r2_url)
                upsert_intel_status(sb, task_id, "Sum.-pre", stt_text=stt_text)
                print(f"✅ [{worker_id}] GROQ 轉譯成功 (歸隊測試通過！)")
            else:
                upsert_intel_status(sb, task_id, "Sum.-pre", stt_text="[GEMINI_2.5_NATIVE_STREAM]")
                print(f"✅ [{worker_id}] GEMINI 鎖定原生流")

            actual_processed += 1 # 🏆 真實完成！

        except Exception as e:
            err_str = str(e)
            if '23505' in err_str or 'duplicate key' in err_str.lower():
                print(f"🤝 [{worker_id}] 競態攔截：任務已被友軍先行接管，繼續巡邏。")
            else:
                print(f"💥 [{worker_id}] 第一棒打擊失敗: {err_str}")
                delete_intel_task(sb, task_id)
                if '404' in err_str and 'Not Found' in err_str:
                    print(f"🕳️ [{worker_id}] 踩到 404 炸彈！退回物流佇列重新下載！")
                    sb.table("mission_queue").update({"r2_url": None, "scrape_status": "pending"}).eq("id", task_id).execute()
            
        finally:
            gc.collect()

# =========================================================
# ✍️ 第二棒：STT to Summary 
# =========================================================
def run_stt_to_summary_mission(sb=None):
    start_time = time.time()
    time.sleep(random.uniform(3.0, 8.0))
    if not sb: sb = get_sb()
    s = get_secrets()
    worker_id = os.environ.get("WORKER_ID", "UNKNOWN_NODE")
    
    tasks = fetch_summary_tasks(sb, fetch_limit=RADAR_FETCH_LIMIT)
    actual_processed = 0
    
    for intel in tasks:
        if actual_processed >= SUMMARY_LIMIT: 
            print(f"🏁 [{worker_id}] 第二棒已達目標產能 ({SUMMARY_LIMIT} 件)，準備交接。")
            break
        if time.time() - start_time > SAFE_DURATION_SECONDS:
            print(f"⏱️ [{worker_id}] 摘要產線逼近安全極限 ({SAFE_DURATION_SECONDS}s)，強制撤退！")
            break
            
        task_id = intel['task_id']
        provider = intel['ai_provider']
        q_data = intel.get('mission_queue') or {}
        r2_file = str(q_data.get('r2_url') or '').lower()
        
        # 🚀 只跳過空包彈，允許 .mp3 等格式進入 Gemini 摘要！
        if not r2_file or r2_file == 'null':
            print(f"⏩ 任務 {q_data.get('episode_title', '')[:10]} 缺乏音檔 (空包彈)，尋找下一筆...")
            continue 

        print(f"✍️ [{worker_id}] 啟動摘要產線: {provider} | 任務: {q_data.get('episode_title', '')[:15]}...")
        
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

            if summary:
                metrics = parse_intel_metrics(summary)
                print(f"📡 [{worker_id}] 摘要完成，準備發送 TG 戰報...")
                
                send_tg_report(s, q_data.get('source_name', '未知'), q_data.get('episode_title', '未知'), summary)
                update_intel_success(sb, task_id, summary, metrics["score"])
                
                print(f"🎉 [{worker_id}] 戰報發送成功，摘要已安全結案！")
                actual_processed += 1 # 🏆 真實發送戰報！

        except Exception as e:
            err_str = str(e)
            print(f"❌ [{worker_id}] 第二棒(摘要或發報)崩潰: {err_str}")
            
            if '429' in err_str:
                print(f"⚠️ [{worker_id}] API 請求過於頻繁 (Rate Limit)，此任務退回等候區。")
            elif '404' in err_str and 'Not Found' in err_str:
                print(f"🕳️ [{worker_id}] 摘要時踩到 404 炸彈！退回物流佇列！")
                delete_intel_task(sb, task_id)
                sb.table("mission_queue").update({"r2_url": None, "scrape_status": "pending"}).eq("id", task_id).execute()
        
        finally:
            gc.collect()