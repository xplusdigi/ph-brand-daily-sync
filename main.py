import os
import sys
import asyncio
import mimetypes
import logging
import time
import traceback
import tempfile
from datetime import datetime, timedelta, timezone
import httpx
from telethon import TelegramClient
from telethon.sessions import StringSession
from supabase import create_client

# 日志配置优化
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    stream=sys.stdout 
)
logger = logging.getLogger(__name__)

# 优先加载报警配置
N8N_WEBHOOK_URL = os.environ.get('N8N_WEBHOOK_URL')
N8N_AUTH_TOKEN = os.environ.get('N8N_AUTH_TOKEN')

# 核心功能函数
async def send_alert(message, level="Critical"):
    """
    通用报警发送函数 - 发送至 n8n，由 n8n 路由至 Global Error Handler
    """
    logger.error(f"🚨 Sending Alert to n8n: {message}")
    if not N8N_WEBHOOK_URL:
        logger.error("❌ Cannot send alert: N8N_WEBHOOK_URL is missing.")
        return

    try:
        async with httpx.AsyncClient() as http_client:
            await http_client.post(
                N8N_WEBHOOK_URL,
                json={
                    "brand": "System_Alert",
                    "content": f"🚨 Python脚本报警 [{level}]: {message}",
                    "message_id": "error_alert",
                    "date": datetime.now().isoformat()
                },
                headers={'Authorization': N8N_AUTH_TOKEN} if N8N_AUTH_TOKEN else {},
                timeout=15 
            )
        logger.info("✅ Error alert sent to n8n.")
    except Exception as e:
        logger.error(f"⚠️ Failed to send error alert: {e}")

def upload_to_supabase_with_retry(supabase_client, bucket_name, file_path, folder_name, max_retries=3):
    """
    Supabase 上传函数 (同步版)
    运行在独立线程中，避免阻塞主线程心跳
    """
    file_name = os.path.basename(file_path)
    # 简单的文件名防止覆盖
    remote_path = f"{folder_name}/{int(datetime.now().timestamp())}_{file_name}"
    mime_type = mimetypes.guess_type(file_path)[0] or "application/octet-stream"
    
    for attempt in range(max_retries):
        try:
            with open(file_path, 'rb') as f:
                supabase_client.storage.from_(bucket_name).upload(
                    path=remote_path,
                    file=f,
                    file_options={"content-type": mime_type}
                )
            public_url = supabase_client.storage.from_(bucket_name).get_public_url(remote_path)
            # 返回 URL 和 Path (用于回滚)
            return public_url, remote_path
            
        except Exception as e:
            logger.warning(f"⚠️ Upload attempt {attempt+1}/{max_retries} failed: {e}")
            time.sleep(2) 
    
    logger.error(f"❌ Failed to upload {file_name} after {max_retries} attempts")
    return None, None

def delete_from_supabase(supabase_client, bucket_name, paths):
    """批量删除 Supabase 文件 (用于回滚)"""
    if not paths: return
    try:
        supabase_client.storage.from_(bucket_name).remove(paths)
        logger.info(f"🧹 Rolled back (deleted) {len(paths)} orphaned files.")
    except Exception as e:
        logger.error(f"⚠️ Failed to clean up orphaned files: {e}")

async def main_logic():
    """主逻辑封装"""
    start_time = time.time()
    
    # 加载环境变量 (Fail Fast)
    try:
        api_id = int(os.environ['TG_API_ID'])
        api_hash = os.environ['TG_API_HASH']
        session_string = os.environ['TG_SESSION_STRING']
        supabase_url = os.environ['SUPABASE_URL']
        supabase_key = os.environ['SUPABASE_KEY']
        target_channels_env = os.environ['TARGET_CHANNELS']
    except KeyError as e:
        error_msg = f"Missing environment variable: {e}"
        await send_alert(error_msg, level="Config_Error")
        raise ValueError(error_msg)

    # 解析频道映射
    raw_targets = target_channels_env.split(',')
    channel_map = {}
    for item in raw_targets:
        if ':' in item:
            parts = item.strip().split(':')
            if len(parts) == 2:
                key = parts[0].strip()
                val = parts[1].strip()
                channel_map[key] = val if val else "Uncategorized"
        elif item.strip():
            channel_map[item.strip()] = "Uncategorized"

    logger.info("🚀 Daily Service Script Started...")
    logger.info(f"📂 Brand Mapping: {channel_map}") 

    # 初始化 Telegram Client
    client = TelegramClient(
        StringSession(session_string), 
        api_id, 
        api_hash,
        connection_retries=5, 
        auto_reconnect=True,
        request_retries=3,
        device_model="N8N_Worker_Server", 
        system_version="Linux_Railway_Env",
        app_version="2.0.0"
    )
    
    supabase = create_client(supabase_url, supabase_key)
    BUCKET_NAME = "daily_post_assets"

    # 连接 Telegram
    try:
        logger.info("📡 Connecting to Telegram...")
        await client.connect()
        if not await client.is_user_authorized():
            await send_alert("❌ Session Invalid/Expired. Please update TG_SESSION_STRING.", level="Fatal")
            os._exit(1)
        logger.info("✅ Connected & Authorized.")
    except Exception as e:
        await send_alert(f"🔥 Connection Failed: {str(e)}", level="Fatal")
        raise e 

    # 配置加载 (时间窗口)
    manila_tz = timezone(timedelta(hours=8))
    now_manila = datetime.now(manila_tz)
    
    try:
        fetch_hours = int(os.environ.get('FETCH_HOURS', 26))
    except ValueError:
        fetch_hours = 26

    try:
        fetch_limit = int(os.environ.get('FETCH_LIMIT', 200))
    except ValueError:
        fetch_limit = 200

    cutoff_time = now_manila - timedelta(hours=fetch_hours)
    logger.info(f"⚙️ Config: Lookback={fetch_hours}h (Cutoff: {cutoff_time}), Limit={fetch_limit}")

    processed_groups = set()
    payloads = []

    # 临时目录管理
    with tempfile.TemporaryDirectory() as temp_dir:
        
        # 遍历频道
        for channel, brand_folder in channel_map.items():
            logger.info(f"🔍 Checking channel: {channel} --> {brand_folder}")
            
            # 查重逻辑 (批量预加载)
            existing_ids_set = set()
            try:
                db_check_limit = max(fetch_limit * 2, 1000)
                
                existing_data = supabase.table('daily_post_archive') \
                    .select('message_id') \
                    .eq('brand', brand_folder) \
                    .eq('source_channel', channel) \
                    .order('inserted_at', desc=True) \
                    .limit(db_check_limit) \
                    .execute()
                
                existing_ids_set = {row['message_id'] for row in existing_data.data}
                logger.info(f"📚 Loaded {len(existing_ids_set)} existing IDs for cache.")
            except Exception as e:
                logger.error(f"⚠️ Batch Check Error: {e}")
                # 继续执行，依靠后续逻辑

            try:
                # 抓取消息
                async for message in client.iter_messages(channel, offset_date=cutoff_time, reverse=True, limit=fetch_limit):
                    
                    if message.action: continue 
                    if not message.text and not message.media: continue
                    
                    # 内存查重
                    if str(message.id) in existing_ids_set:
                        continue

                    # 数据准备
                    media_urls = []
                    media_type = "text"
                    final_text = message.text or ""
                    final_msg_id = str(message.id)
                    is_payload_valid = True 

                    # 分支 A: 媒体组 (Album)
                    if message.grouped_id:
                        if message.grouped_id in processed_groups: continue 
                        processed_groups.add(message.grouped_id)
                        media_type = "album"
                        
                        group_msgs = await client.get_messages(channel, ids=list(range(message.id, message.id + 9)))
                        real_group = [m for m in group_msgs if m and m.grouped_id == message.grouped_id]
                        if not real_group: real_group = [message]

                        # 记录本次相册上传的所有 path，用于回滚
                        album_uploaded_paths = []

                        for m in real_group:
                            if m.media:
                                path = None
                                try:
                                    path = await m.download_media(file=temp_dir)
                                    if path:
                                        # 上传
                                        url, remote_path = await asyncio.to_thread(
                                            upload_to_supabase_with_retry, 
                                            supabase, BUCKET_NAME, path, brand_folder
                                        )

                                        if url: 
                                            media_urls.append(url)
                                            album_uploaded_paths.append(remote_path)
                                        else:
                                            # 上传失败 -> 触发回滚
                                            error_msg = f"Supabase Upload Failed mid-album (Msg ID: {message.id})"
                                            logger.error(error_msg)
                                            await send_alert(error_msg, level="Upload_Error")
                                            is_payload_valid = False
                                            
                                            # 执行回滚：删除这个相册之前已经上传成功的图片
                                            if album_uploaded_paths:
                                                await asyncio.to_thread(
                                                    delete_from_supabase,
                                                    supabase, BUCKET_NAME, album_uploaded_paths
                                                )
                                            break
                                finally:
                                    if path and os.path.exists(path):
                                        try: os.remove(path)
                                        except: pass
                            
                            # 即使中断，也要继续检查文本更新
                            if m.text and len(m.text) > len(final_text):
                                final_text = m.text
                    
                    # 分支 B: 单媒体 (Photo/Video)
                    elif message.media:
                        media_type = "photo" if message.photo else "video"
                        path = None
                        try:
                            path = await message.download_media(file=temp_dir)
                            if path:
                                url, _ = await asyncio.to_thread(
                                    upload_to_supabase_with_retry, 
                                    supabase, BUCKET_NAME, path, brand_folder
                                )
                                
                                if url: 
                                    media_urls.append(url)
                                else:
                                    error_msg = f"Supabase Upload Failed (Msg ID: {message.id})"
                                    logger.error(error_msg)
                                    await send_alert(error_msg, level="Upload_Error")
                                    is_payload_valid = False
                        finally:
                            if path and os.path.exists(path):
                                try: os.remove(path)
                                except: pass
                    
                    # 分支 C: 纯文本
                    else:
                        media_type = "text"

                    # 构建 Payload
                    if is_payload_valid:
                        payload = {
                            "source_channel": channel,
                            "brand": brand_folder,
                            "content": final_text,
                            "media_urls": media_urls, 
                            "media_type": media_type,
                            "message_id": final_msg_id,
                            "date": message.date.astimezone(manila_tz).isoformat()
                        }
                        payloads.append(payload)
                        logger.info(f"✅ Prepared payload: {final_msg_id} ({media_type})")
                    else:
                        logger.warning(f"⚠️ Skipping Payload ID {final_msg_id} due to upload failure.")

            except Exception as e:
                err_msg = f"❌ Error scraping channel {channel}: {e}"
                logger.error(err_msg)
                await send_alert(err_msg, level="Channel_Scrape_Error")
                continue
    
    # 推送 n8n (串行模式 - 保持稳健)
    if payloads:
        logger.info(f"🚀 Pushing {len(payloads)} items to n8n...")
        headers = {'Authorization': N8N_AUTH_TOKEN} if N8N_AUTH_TOKEN else {}
        success_count = 0
        fail_count = 0

        async with httpx.AsyncClient(timeout=5.0) as http_client:
            for p in payloads:
                try:
                    r = await http_client.post(N8N_WEBHOOK_URL, json=p, headers=headers)
                    if r.status_code == 200:
                        logger.info(f"✅ Sent ID {p['message_id']} to n8n")
                        success_count += 1
                    else:
                        logger.warning(f"⚠️ Webhook Failed {r.status_code} for ID {p['message_id']}")
                        fail_count += 1
                    await asyncio.sleep(1) 
                except Exception as e:
                    logger.error(f"⚠️ Webhook Connection Error: {e}")
                    fail_count += 1
        
        summary_msg = f"📊 Job Summary: Scraped {len(payloads)}, Sent {success_count}, Failed {fail_count}."
        logger.info(summary_msg)
        
        if fail_count > 0:
             await send_alert(f"⚠️ Some items failed to push to n8n. {fail_count} failures.", level="Webhook_Warning")

    else:
        logger.info("💤 No new content found (or all skipped).")

    try:
        await client.disconnect()
    except: pass
    
    logger.info(f"👋 Job Complete. Duration: {time.time() - start_time:.2f}s")

# 全局异常捕获 (遗言机制)
if __name__ == '__main__':
    try:
        asyncio.run(main_logic())
    except Exception as e:
        # 立即打印日志，确保在控制台可见
        error_msg = f"🔥 CRITICAL SCRIPT CRASH: {str(e)}\n\n{traceback.format_exc()}"
        logger.critical(error_msg)
        
        # 尝试发送遗言到 n8n (同步阻塞等待)
        try:
            print("🚨 Attempting to send death rattle to n8n...", file=sys.stderr)
            asyncio.run(send_alert(error_msg, level="CRITICAL_CRASH"))
            print("✅ Death rattle sent.", file=sys.stderr)
        except Exception as alert_error:
            # 即使报警失败，也要打印到控制台，以便查阅 Railway 日志
            print(f"❌ Failed to send crash alert: {alert_error}", file=sys.stderr)

        # 暴力退出 (防止 Telegram 线程卡死 Railway)
        print("💀 Executing os._exit(1) to kill zombie threads...", file=sys.stderr)
        os._exit(1)
