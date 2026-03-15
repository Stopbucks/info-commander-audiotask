# ---------------------------------------------------------
# src/pod_scra_intel_r2.py (V5.3 兵工廠與倉儲模組 - 絕對防爆版)
# 適用：RENDER, KOYEB, ZEABUR, HF, DBOS | 規格：通用
# [任務] 1. R2 儲存端對端連線 2. 檔案上下傳 3. FFmpeg 強化轉檔
# [機制] 實裝 600 秒硬性超時，防止損壞音檔導致機甲永久卡死。
# [修改] 1. 導入精準的 subprocess 錯誤捕捉，打破除錯黑洞。
# [修改] 2. 保持無裝飾器狀態，確保在 Serverless 與實體 Container 間完美兼容。
# ---------------------------------------------------------

import os, requests, gc, subprocess, boto3
import imageio_ffmpeg   

def get_s3_client():
    """【基礎建設】建立並回傳 R2/S3 連線物件"""
    return boto3.client('s3', endpoint_url=os.environ.get("R2_ENDPOINT_URL"),
                        aws_access_key_id=os.environ.get("R2_ACCESS_KEY_ID"),
                        aws_secret_access_key=os.environ.get("R2_SECRET_ACCESS_KEY"), region_name="auto")

def upload_to_r2(local_path, filename):
    """【倉儲物流】將本機物資上傳至 R2"""
    s3 = get_s3_client()
    s3.upload_file(local_path, os.environ.get("R2_BUCKET_NAME"), filename)

def download_from_r2(filename, local_path):
    """【倉儲物流】提領 R2 物資至本機暫存區"""
    url = f"{os.environ.get('R2_PUBLIC_URL')}/{filename}"
    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        with open(local_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024*1024): f.write(chunk)

def compress_task_to_opus(task_id, raw_filename):
    """【兵工廠改裝】泛用轉檔步驟，具備超時守衛與損毀防禦"""
    ext = os.path.splitext(raw_filename)[1].lower() or '.mp3'
    tmp_in = f"/tmp/in_{task_id[:8]}{ext}"
    tmp_out = f"/tmp/out_{task_id[:8]}.opus"
    new_name = f"opt_{task_id[:8]}.opus"
    
    try:
        # 1. 提領物資
        download_from_r2(raw_filename, tmp_in)
        
        # 2. 獲取 FFmpeg 並配置指令
        ffmpeg_exe = imageio_ffmpeg.get_ffmpeg_exe()
        cmd = [ffmpeg_exe, '-y', '-i', tmp_in, '-ar', '16000', '-ac', '1', 
               '-c:a', 'libopus', '-b:a', '16k', '-vbr', 'off', 
               '-compression_level', '0', '-preset', 'superfast', tmp_out]
        
        # 🚀 強化防禦：加入 timeout=600 (10分鐘)，即便音檔損壞導致 FFmpeg 卡住也能強制斬斷
        subprocess.run(cmd, check=True, capture_output=True, timeout=600)
        
        # 3. 回傳新物資
        upload_to_r2(tmp_out, new_name)
        return True, new_name
        
    except subprocess.TimeoutExpired:
        print(f"❌ [改裝超時]: FFmpeg 轉檔耗時過長，疑似損壞檔案，強制終止任務。")
        return False, None
    except subprocess.CalledProcessError as e:
        # 🚀 強化除錯：捕捉並印出 FFmpeg 真正的憤怒（錯誤訊息）
        err_msg = e.stderr.decode('utf-8', errors='ignore') if e.stderr else str(e)
        print(f"❌ [FFmpeg 崩潰]: {err_msg[:200]}")
        return False, None
    except Exception as e:
        print(f"❌ [改裝失敗]: 系統異常 -> {e}")
        return False, None
    finally:
        # 🧹【內存守衛】強制清理暫存檔與回收記憶體
        for f in [tmp_in, tmp_out]:
            if os.path.exists(f): 
                try: os.remove(f)
                except: pass
        gc.collect()