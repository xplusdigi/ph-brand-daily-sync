import os
import asyncio
import mimetypes
from datetime import datetime, timedelta, timezone
import requests
from telethon import TelegramClient
from telethon.sessions import StringSession
from supabase import create_client

# --- 配置加载与解析 ---
api_id = int(os.environ['TG_API_ID'])
api_hash = os.environ['TG_API_HASH']
session_string = os.environ['TG_SESSION_STRING']
n8n_webhook = os.environ['N8N_WEBHOOK_URL']
supabase_url = os.environ['SUPABASE_URL']
supabase_key = os.environ['SUPABASE_KEY']

# 解析 TARGET_CHANNELS (格式: channel_id:folder_name,channel2:folder2)
raw_targets = os.environ['TARGET_CHANNELS'].split(',')
channel_map = {}
for item in raw_targets:
    if ':' in item:
        parts = item.strip().split(':')
        channel_map[parts[0].strip()] = parts[1].strip()
    else:
        # 容错：如果用户忘记写冒号，默认放入 'Uncategorized' 文件夹
        channel_map[item.strip()] = "Uncategorized"

# --- 初始化客户端 ---
client = TelegramClient(StringSession(session_string), api_id, api_hash)
supabase = create_client(supabase_url, supabase_key)
BUCKET_NAME = "daily_post_assets"

async def upload_to_supabase(file_path, folder_name):
    """上传文件到 Supabase Storage 指定文件夹并返回 Public URL"""
    file_name = os.path.basename(file_path)
    
    # 架构优化：路径加入文件夹前缀 (例如: folder2/17000000_image.jpg)
    remote_path = f"{folder_name}/{int(datetime.now().timestamp())}_{file_name}"
    
    with open(file_path, 'rb') as f:
        try:
            mime_type = mimetypes.guess_type(file_path)[0]
            # Supabase 会自动处理文件夹层级，无需预先创建
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
    print("🚀 Script Started...")
    print(f"📂 Brand Mapping: {channel_map}") # 打印映射关系以供调试
    
    await client.connect()
    
    # 设定时间窗口：过去 65 分钟
    cutoff_time = datetime.now(timezone.utc) - timedelta(minutes=65)
    print(f"⏰ Looking for messages after: {cutoff_time}")

    processed_groups = set()
    payloads = []

    # 遍历字典：channel 是频道ID， brand_folder 是对应的文件夹名
    for channel, brand_folder in channel_map.items():
        print(f"🔍 Checking channel: {channel} (Target Folder: {brand_folder})")
        try:
            async for message in client.iter_messages(channel, offset_date=cutoff_time, reverse=True):
                
                # 1. 过滤逻辑
                if message.action: continue 
                if not message.text and not message.media: continue

                # 2. 相册处理逻辑
                media_urls = []
                media_type = "text"
                
                if message.grouped_id:
                    if message.grouped_id in processed_groups:
                        continue 
                    
                    print(f"📦 Found Album in {channel}")
                    processed_groups.add(message.grouped_id)
                    media_type = "album"
                    
                    group_msgs = await client.get_messages(channel, ids=list(range(message.id, message.id + 10)))
                    real_group = [m for m in group_msgs if m and m.grouped_id == message.grouped_id]
                    
                    for m in real_group:
                        if m.media:
                            path = await m.download_media(file=f"/tmp/")
                            if path:
                                # 传入 brand_folder
                                url = await upload_to_supabase(path, brand_folder)
                                if url: media_urls.append(url)
                                os.remove(path)
                    
                    final_text = message.text or real_group[0].text or ""
                    final_msg_id = str(message.id)

                elif message.media:
                    print(f"📸 Found Single Media in {channel}")
                    media_type = "photo" if message.photo else "video"
                    path = await message.download_media(file=f"/tmp/")
                    if path:
                        # 传入 brand_folder
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

                # 3. 构造 Payload (新增 brand 字段)
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

    # 4. 发送给 n8n
    if not payloads:
        print("💤 No new messages found. Silent exit.")
    else:
        print(f"🚀 Sending {len(payloads)} items to n8n...")
        for p in payloads:
            try:
                r = requests.post(n8n_webhook, json=p)
                print(f"✅ Sent ID {p['message_id']} (Brand: {p['brand']}): {r.status_code}")
                await asyncio.sleep(1) 
            except Exception as e:
                print(f"⚠️ Webhook failed: {e}")

    await client.disconnect()

if __name__ == '__main__':
    asyncio.run(main())
