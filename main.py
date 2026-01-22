import os
import sys
import asyncio
import mimetypes
from datetime import datetime, timedelta, timezone
import requests
from telethon import TelegramClient
from telethon.sessions import StringSession
from supabase import create_client

# --- 配置加载 ---
try:
    api_id = int(os.environ['TG_API_ID'])
    api_hash = os.environ['TG_API_HASH']
    session_string = os.environ['TG_SESSION_STRING']
    n8n_webhook = os.environ['N8N_WEBHOOK_URL']
    n8n_auth_token = os.environ['N8N_AUTH_TOKEN']
    supabase_url = os.environ['SUPABASE_URL']
    supabase_key = os.environ['SUPABASE_KEY']
    target_channels_env = os.environ['TARGET_CHANNELS']
except KeyError as e:
    print(f"❌ Critical Error: Missing environment variable {e}")
    sys.exit(1)

# 解析频道映射
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
    connection_retries=5, 
    auto_reconnect=True
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
            supabase.storage.from_(BUCKET_NAME).upload(
                path=remote_path,
                file=f,
                file_options={"content-type": mime_type if mime_type else "application/octet-stream"}
            )
            public_url = supabase.storage.from_(BUCKET_NAME).get_public_url(remote_path)
            return public_url
        except Exception as e:
            print(f"⚠️ Upload failed for {remote_path}: {e}")
            return None

async def main():
    print("🚀 Service Script Started...")
    print(f"📂 Brand Mapping: {channel_map}") 
    
    # --- 连接逻辑 ---
    try:
        print("📡 Connecting to Telegram...")
        await client.connect()
        if not await client.is_user_authorized():
            print("❌ Critical: Session Invalid. Server IP might have changed or Session revoked.")
            sys.exit(1)
        print("✅ Connected & Authorized.")
    except Exception as e:
        print(f"🔥 Connection Failed: {e}")
        sys.exit(1)
    
    # --- 业务逻辑 ---
    manila_tz = timezone(timedelta(hours=8))
    now_manila = datetime.now(manila_tz)
    cutoff_time = now_manila - timedelta(minutes=65)
    
    print(f"⏰ Fetching messages after (Manila Time): {cutoff_time.strftime('%Y-%m-%d %H:%M:%S')}")

    processed_groups = set()
    payloads = []

    for channel, brand_folder in channel_map.items():
        print(f"🔍 Checking channel: {channel} --> {brand_folder}")
        try:
            async for message in client.iter_messages(channel, offset_date=cutoff_time, reverse=True):
                
                if message.action: continue 
                if not message.text and not message.media: continue

                media_urls = []
                media_type = "text"
                
                # 处理相册 (Album)
                if message.grouped_id:
                    if message.grouped_id in processed_groups: continue 
                    print(f"📦 Processing Album in {channel}")
                    processed_groups.add(message.grouped_id)
                    media_type = "album"
                    group_msgs = await client.get_messages(channel, ids=list(range(message.id, message.id + 10)))
                    real_group = [m for m in group_msgs if m and m.grouped_id == message.grouped_id]
                    for m in real_group:
                        if m.media:
                            os.makedirs("/tmp/", exist_ok=True)
                            path = await m.download_media(file="/tmp/")
                            if path:
                                url = await upload_to_supabase(path, brand_folder)
                                if url: media_urls.append(url)
                                os.remove(path)
                    final_text = message.text or real_group[0].text or ""
                    final_msg_id = str(message.id)

                # 处理单图/视频
                elif message.media:
                    print(f"📸 Processing Media in {channel}")
                    media_type = "photo" if message.photo else "video"
                    os.makedirs("/tmp/", exist_ok=True)
                    path = await message.download_media(file="/tmp/")
                    if path:
                        url = await upload_to_supabase(path, brand_folder)
                        if url: media_urls.append(url)
                        os.remove(path)
                    final_text = message.text or ""
                    final_msg_id = str(message.id)

                # 处理纯文本
                else:
                    print(f"📝 Processing Text in {channel}")
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
                    "date": message.date.astimezone(manila_tz).isoformat()
                }
                payloads.append(payload)

        except Exception as e:
            print(f"❌ Error in channel {channel}: {e}")
            continue

    # --- 推送数据 ---
    if not payloads:
        print("💤 No new content found.")
    else:
        print(f"🚀 Pushing {len(payloads)} items to n8n...")
        
        headers = {'Authorization': n8n_auth_token}

        for p in payloads:
            try:
                r = requests.post(
                    n8n_webhook, 
                    json=p, 
                    timeout=30,
                    headers=headers
                )
                print(f"✅ Sent ID {p['message_id']} ({p['brand']}): Status {r.status_code}")
                await asyncio.sleep(0.5) 
            except Exception as e:
                print(f"⚠️ Webhook Error: {e}")

    try:
        await client.disconnect()
    except:
        pass
    print("👋 Job Complete.")

if __name__ == '__main__':
    asyncio.run(main())
