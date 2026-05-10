# ---------------------------------------------------------
# src/pod_scra_intel_control.py (V5.9 GHA 吞噬特遣隊_乾淨合併版)
# [特遣] 代號：AUDIO_EAT
# [修正] V5.9: 解決重複定義函數導致的 NameError。
#        將 base_blacklist 移至全域變數，確保面板能正確讀取。
#        設定為「純搬運清淤專家」，準備配合 V6.1 的斷開戰術執行極限下載。
# ---------------------------------------------------------
import os
from supabase import create_client

# =========================================================
# 🛡️ 基地黑名單 (Base Blacklist) 定義區
# 宣告為全域變數，確保所有面板都能讀取到
# =========================================================
base_blacklist = [
    "example-malicious.com", 
    "broken-audio-server.net",
    "youtube.com", 
    "youtu.be"
]

# =========================================================
# ⚙️ 戰術控制面板 (Tactical Panel)
# =========================================================
def get_tactical_panel(worker_id):
    """專屬特遣隊裝備發放，接替 DBOS 執行極限重裝壓縮"""
    
    # 🚜 重裝吞噬者模板 (化身極限物流搬運工)
    audio_eat_panel = {
        "MEM_TIER": 1024,             # 🚀 維持巨獸記憶體
        "RADAR_FETCH_LIMIT": 100,
        "DOWNLOAD_LIMIT": 5,          # 📥 [提升] 每次最高可掃蕩 5 個檔案
        "MAX_SAME_DOMAIN": 1,         # 🛡️ 同網域安全併發數
        "STT_LIMIT": 5,               # 🎤 雖然斷開連結用不到，但保留設定
        "SUMMARY_LIMIT": 0,           # 🛑 專心做搬運工
        "SAFE_DURATION_SECONDS": 4200,
        "CAN_COMPRESS": True,         # ⚙️ 啟用 FFmpeg 降噪壓縮
        "COMPRESS_ONLY": False,       # 
        "SCOUT_MODE": False,
        "MAX_TICKS": 4,               # 留作日後若需回歸常規部隊時的節拍設定
        "IDLE_GEARBOX": 1.0,          # ⚙️ GHA 啟動就是全力衝刺搬運，不降速
        "GLOBAL_DOMAIN_BLACKLIST": base_blacklist 
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
