# ---------------------------------------------------------
# src/pod_scra_intel_control.py (V5.5 GHA 吞噬特遣隊專用版)
# [特遣] 代號：AUDIO_EAT
# [面板] 專為 GitHub Actions 打造的重裝面板，享有最高產能與 FFmpeg 權限。
# [節拍] MAX_TICKS: 2。以最短的 2 拍節奏，高頻交替執行轉譯與摘要，極致榨乾 GHA 效能。
# ---------------------------------------------------------
import os
from supabase import create_client

# =========================================================
# ⚙️ GHA 專屬戰術控制面板 (AUDIO_EAT Exclusive)
# =========================================================
def get_tactical_panel(worker_id):
    """專屬特遣隊裝備發放，無論身分預設皆給予重裝火力"""
    
    # 🚜 重裝巨獸模板 (適用於 GHA 高效能且限時的環境)
    audio_eat_panel = {
        "MEM_TIER": 512,
        "RADAR_FETCH_LIMIT": 100,
        "STT_LIMIT": 2,               # 🎤 一次狂吞 2  個音檔轉譯
        "SUMMARY_LIMIT": 2,           # 📝 一次高量產出 2 篇摘要
        "SAFE_DURATION_SECONDS": 1500,# 🛡️ 25 分鐘安全撤退線
        "CAN_COMPRESS": True,         # ⚙️ 允許 FFmpeg 降噪壓縮
        "SCOUT_MODE": False,
        "MAX_TICKS": 2                # ⏱️ 極限 2 拍：轉譯與摘要快速切換 (每 12 小時 1 拍)
    }

    panels = {
        "AUDIO_EAT": audio_eat_panel
    }
    
    # 就算萬一 WORKER_ID 沒抓到，在 GHA 環境下也直接套用重裝面板
    return panels.get(worker_id, audio_eat_panel)

# =========================================================
# 🔑 機密與連線中樞 (Secrets & Connections)
# =========================================================
def get_secrets():
    return {
        "SB_URL": os.environ.get("SUPABASE_URL"), 
        "SB_KEY": os.environ.get("SUPABASE_KEY"),
        "GROQ_KEY": os.environ.get("GROQ_API_KEY"), 
        "GEMINI_KEY": os.environ.get("GEMINI_API_KEY"),
        "TG_TOKEN": os.environ.get("TELEGRAM_BOT_TOKEN"), 
        "TG_CHAT": os.environ.get("TELEGRAM_CHAT_ID"),
        "R2_URL": os.environ.get("R2_PUBLIC_URL")
    }

def get_sb():
    s = get_secrets()
    return create_client(s["SB_URL"], s["SB_KEY"])