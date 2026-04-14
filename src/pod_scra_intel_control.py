# ---------------------------------------------------------
# src/pod_scra_intel_control.py (V5.8 GHA 吞噬特遣隊_替補DBOS位置：專用版)
# [特遣] 代號：AUDIO_EAT
# [面板] 專為 GitHub Actions 打造的重裝面板，享有最高產能與 FFmpeg 權限。
# [節拍] MAX_TICKS: 2。以最短的 2 拍節奏，高頻交替執行轉譯與摘要，極致榨乾 GHA 效能。
# [修正] 配合修正進行壓縮檔案
# ---------------------------------------------------------
import os
from supabase import create_client

#---前面程式碼相同---#
# -----(定位線)以下修改----

# =========================================================
# ⚙️ GHA 專屬戰術控制面板 (AUDIO_EAT Exclusive)
# =========================================================
def get_tactical_panel(worker_id):
    """專屬特遣隊裝備發放，接替 DBOS 執行極限重裝壓縮"""
    
    # 🚜 重裝吞噬者模板 (化身純壓縮農場)
    audio_eat_panel = {
        "MEM_TIER": 1024,             # 🚀 升級巨獸記憶體，吃下 DBOS 遺留的死檔
        "RADAR_FETCH_LIMIT": 100,
        "STT_LIMIT": 10,               # 🎤 每次極限吞噬 10 個大怪獸進行壓縮
        "SUMMARY_LIMIT": 0,           # 🛑 拔除摘要武器，專心做壓縮搬運工
        "SAFE_DURATION_SECONDS": 4200,# 🛡️ 70 分鐘安全撤退線 (確保在 GHA 90分死線前優雅結案)
        "CAN_COMPRESS": True,         # ⚙️ 啟用 FFmpeg 降噪壓縮
        "COMPRESS_ONLY": True,        # 🛑 關鍵：設為兵工廠模式，壓完就跑，不耗費 API！
        "SCOUT_MODE": False,
        "MAX_TICKS": 2 
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
