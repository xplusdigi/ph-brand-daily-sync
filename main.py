import os
import sys
import asyncio
import mimetypes
import random
from datetime import datetime, timedelta, timezone
import requests
from telethon import TelegramClient
from telethon.sessions import StringSession
from supabase import create_client

# --- 配置加载与解析 ---
try:
    api_id = int(os.environ['TG_API_ID'])
    api_hash = os.environ['TG_API_HASH']
    session_string = os.environ['TG_SESSION_STRING']
    n8n_webhook = os.environ['N8N_WEBHOOK_URL']
    supabase_url = os.environ['SUPABASE_URL']
    supabase_key = os.environ['SUPABASE_KEY']
    target_channels_env = os.environ['TARGET_CHANNELS']
except KeyError as e:
    print(f"❌ Critical Error: Missing environment variable {e}")
    sys.exit(1)

# 解析 TARGET_CHANNELS
raw_targets = target_channels_env.split(',')
channel_map = {}
for item in raw_targets:
    if ':' in item:
        parts = item.strip().split(':')
        channel_map[parts[0].strip()] = parts[1].strip()
    else:
        channel_map[item.strip()] = "Uncategorized"

# --- 初始化客户端 ---
client = TelegramClient(
    StringSession(session_string), 
    api_id, 
    api_hash,
    connection_retries=None,
    auto_reconnect=False
)
supabase = create_client(supabase_url, supabase_key)
BUCKET_NAME = "daily_post_assets"

async def upload_to_supabase(file_path, folder_name):
    """上传文件到 Supabase Storage"""
    file_name = os.path.basename(file_path)
    remote_path = f"{folder_name}/{int(datetime.now().timestamp())}_{file_name}"
    
    with open(file_path, 'rb') as f:
        try:
            mime_type = mimetypes.guess_type(file_path)[0]
            response = supabase.storage.from_(BUCKET_NAME).upload(
                path=remote_path,
                file=f,
                file_options={"content-type": mime_type if mime_type else "application/octet-stream"}
            )
            public_url = supabase.storage.from_(BUCKET_NAME).get_public_url(remote_path)
            return public_url
        except Exception as e:
            print(f"Upload failed for {remote_path}: {e}")
            return None

async def main():
    print("🚀 Script Started (Manila Timezone Configured)...")
    print(f"📂 Brand Mapping: {channel_map}") 
    
    # --- 🛡️ 连接逻辑 (保持重试机制) ---
    max_retries = 5
    for i in range(max_retries):
        try:
            wait_time = random.uniform(2, 6)
            print(f"⏳ Sleeping for {wait_time:.2f}s before connecting...")
            await asyncio.sleep(wait_time)

            print(f"📡 Connecting to Telegram (Attempt {i+1}/{max_retries})...")
            await client.connect()
            
            if await client.is_user_authorized():
                print("✅ Login Success! Session Valid.")
                break 
            else:
                print("❌ Critical: Session Invalid (Requires Code). Exiting.")
                sys.exit(1)
                
        except (ConnectionError, OSError) as e:
            print(f"⚠️ Connection Reset/Refused (IP Blocked?): {e}")
            if i < max_retries - 1:
                print("⏳ Disconnecting and waiting 10s...")
                try: await client.disconnect() 
                except: pass
                await asyncio.sleep(10)
            else:
                print("🔥 Max retries reached. GitHub IP is too dirty.")
                sys.exit(1)
        except Exception as e:
            print(f"🔥 Unknown Connection Error: {e}")
            sys.exit(1)
    
    manila_tz = timezone(timedelta(hours=8))
    now_manila = datetime.now(manila_tz)
    # 设定过去 65 分钟
    cutoff_time = now_manila - timedelta(minutes=65)
    
    print(f"⏰ Looking for messages after (Manila Time): {cutoff_time.strftime('%Y-%m-%d %H:%M:%S')}")

    processed_groups = set()
    payloads = []

    for channel, brand_folder in channel_map.items():
        print(f"🔍 Checking channel: {channel} (Target Folder: {brand_folder})")
        try:
            # 这里的逻辑是核心
            async for message in client.iter_messages(channel, offset_date=cutoff_time, reverse=True):
                
                if message.action: continue 
                if not message.text and not message.media: continue

                media_urls = []
                media_type = "text"
                
                if message.grouped_id:
                    if message.grouped_id in processed_groups: continue 
                    
                    print(f"📦 Found Album in {channel}")
                    processed_groups.add(message.grouped_id)
                    media_type = "album"
                    
                    # 限制获取数量防止卡死
                    group_msgs = await client.get_messages(channel, ids=list(range(message.id, message.id + 10)))
                    real_group = [m for m in group_msgs if m and m.grouped_id == message.grouped_id]
                    
                    for m in real_group:
                        if m.media:
                            os.makedirs("/tmp/", exist_ok=True)
                            path = await m.download_media(file=f"/tmp/")
                            if path:
                                url = await upload_to_supabase(path, brand_folder)
                                if url: media_urls.append(url)
                                os.remove(path)
                    
                    final_text = message.text or real_group[0].text or ""
                    final_msg_id = str(message.id)

                elif message.media:
                    print(f"📸 Found Single Media in {channel}")
                    media_type = "photo" if message.photo else "video"
                    os.makedirs("/tmp/", exist_ok=True)
                    path = await message.download_media(file=f"/tmp/")
                    if path:
                        url = await upload_to_supabase(path, brand_folder)
                        if url: media_urls.append(url)
                        os.remove(path)
                    final_text = message.text or ""
                    final_msg_id = str(message.id)

                else:
                    print(f"📝 Found Text in {channel}")
                    media_type = "text"
                    final_text = message.text
                    final_msg_id = str(message.id)

                payload = {
                    "source_channel": channel,
                    "brand": brand_folder,
                    "content": final_text,
                    "media_urls": media_urls, 
                    "media_type": media_type,
                    "message_id": final_msg_id,
                    "date": message.date.isoformat()
                }
                payloads.append(payload)

        except Exception as e:
            print(f"❌ Error checking {channel}: {e}")
            # 不退出，继续检查下一个频道
            continue

    if not payloads:
        print("💤 No new messages found. Silent exit.")
    else:
        print(f"🚀 Sending {len(payloads)} items to n8n...")
        for p in payloads:
            try:
                r = requests.post(n8n_webhook, json=p, timeout=30)
                print(f"✅ Sent ID {p['message_id']} (Brand: {p['brand']}): {r.status_code}")
                await asyncio.sleep(1) 
            except Exception as e:
                print(f"⚠️ Webhook failed: {e}")

    try:
        await client.disconnect()
    except:
        pass
    print("👋 Script finished successfully.")

if __name__ == '__main__':
    # 禁用 uvloop 防止兼容性问题
    asyncio.run(main())
