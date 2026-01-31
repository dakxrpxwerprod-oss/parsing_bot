import asyncio
import json
import os
import random
import string
import logging
import re
from google.cloud import storage
from google.oauth2 import service_account
import gspread
from telethon import TelegramClient, events, errors
from telethon.sessions import StringSession
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest
from telethon.errors import ChannelPrivateError, InviteHashExpiredError, UserAlreadyParticipantError
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
# Константы (hardcoded на основе ваших уточнений)
SHEET_ID = '1J0sqjlhZ4uSLuo8sA48sEcEXRD4NZDwDMabu6wz0kKo'
BUCKET_NAME = 'maneralabservice_source'
TIMEOUT = 60 # сек на ввод
# Инициализация Google clients (GOOGLE_JSON из env)
credentials = service_account.Credentials.from_service_account_info(
    json.loads(os.environ['GOOGLE_JSON']),
    scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/cloud-platform']
)
gs_client = gspread.authorize(credentials)
gcs_client = storage.Client(credentials=credentials)
bucket = gcs_client.bucket(BUCKET_NAME)
# Доступ к Sheets
sheet = gs_client.open_by_key(SHEET_ID)
accounts_sheet = sheet.worksheet('accounts')
posts_sheet = sheet.worksheet('Pars_posts')
# State: user_id -> {'state': str, 'data': dict, 'waiter': asyncio.Event}
states = {}
# Helper: Получить данные аккаунта из строки 3
def get_account_data():
    row = accounts_sheet.row_values(3)
    phone_raw = row[1] if len(row) > 1 else None # B3
    phone = '+' + phone_raw if phone_raw and not phone_raw.startswith('+') else phone_raw # Добавляем + если отсутствует
    api_id = int(row[3]) if len(row) > 3 else None # D3
    api_hash = row[4] if len(row) > 4 else None # E3
    session_path = row[5] if len(row) > 5 else None # F3
    logger.debug(f"Loaded account data: phone={phone}, api_id={api_id}, api_hash={api_hash[:5]}..., session_path={session_path}")
    return phone, api_id, api_hash, session_path
# Helper: Рандомное имя сессии (16 цифр .session)
def random_session_name():
    return ''.join(random.choices(string.digits, k=16)) + '.session'
# Helper: Загрузка в GCS
def upload_to_gcs(local_path, gcs_name):
    blob = bucket.blob(gcs_name)
    blob.upload_from_filename(local_path)
    return f'{BUCKET_NAME}/{gcs_name}'
# Helper: Рандомное имя медиа (7 букв _ номер .ext)
def random_media_name(number, ext):
    letters = ''.join(random.choices(string.ascii_lowercase, k=7))
    return f'{letters}_{number}.{ext}'
# Telethon client (bot)
BOT_TOKEN = os.environ['BOT_TOKEN']
phone, api_id, api_hash, _ = get_account_data()
client = TelegramClient(None, api_id, api_hash)
@client.on(events.NewMessage(pattern='/start'))
async def handle_start(event):
    logger.debug(f"Received /start from user {event.sender_id}")
    await event.reply('Привет! Я готов к работе. Используйте /newpars для парсинга.')
@client.on(events.NewMessage(pattern='/newpars'))
async def start_newpars(event):
    user_id = event.sender_id
    logger.debug(f"Received /newpars from user {user_id}")
    states[user_id] = {'state': 'waiting_link', 'data': {}, 'waiter': asyncio.Event()}
    await event.reply('Введите ссылку на Telegram канал.')
@client.on(events.NewMessage)
async def handle_message(event):
    user_id = event.sender_id
    if user_id not in states:
        return
    state = states[user_id]['state']
    data = states[user_id]['data']
    waiter = states[user_id]['waiter']
    logger.debug(f"Handling message in state {state} for user {user_id}")
    try:
        if state == 'waiting_link':
            link = event.message.text.strip()
            if not link.startswith('https://t.me/'):
                await event.reply('Ссылка должна начинаться с https://t.me/. Попробуйте снова.')
                return
            data['channel_link'] = link
            await event.reply('Начинаю проверку каналов.')
            states[user_id]['state'] = 'auth'
            await authorize_account(event, data)
        elif state == 'waiting_code':
            data['code'] = event.message.text.strip()
            waiter.set()
        elif state == 'waiting_2fa':
            data['password'] = event.message.text.strip()
            waiter.set()
    except Exception as e:
        logger.error(f"Error handling message: {str(e)}")
        await event.reply(f'Ошибка: {str(e)}')
        del states[user_id]
async def authorize_account(event, data):
    phone, api_id, api_hash, session_path = get_account_data()
    session_str = None
    if session_path:
        try:
            blob_name = session_path.split('/', 1)[-1] if '/' in session_path else session_path
            blob = bucket.blob(blob_name)
            session_str = blob.download_as_string().decode('utf-8')
            logger.info("Loaded session from GCS")
        except Exception as e:
            logger.error(f"Error loading session: {str(e)}")
            session_path = None # Fallback to new
    client_session = TelegramClient(StringSession(session_str), api_id, api_hash) if session_str else TelegramClient(StringSession(), api_id, api_hash)
    try:
        await client_session.connect()
        logger.info("Client session connected")
        if not await client_session.is_user_authorized():
            logger.info("Sending code request")
            await client_session.send_code_request(phone)
            states[event.sender_id]['state'] = 'waiting_code'
            await event.reply('Введите код для авторизации.')
            waiter = states[event.sender_id]['waiter']
            try:
                await asyncio.wait_for(waiter.wait(), timeout=TIMEOUT)
            except asyncio.TimeoutError:
                await event.reply('Время истекло.')
                del states[event.sender_id]
                return
            code = data.get('code')
            try:
                await client_session.sign_in(phone, code)
            except errors.SessionPasswordNeededError:
                states[event.sender_id]['state'] = 'waiting_2fa'
                await event.reply('Введите 2FA пароль.')
                waiter.clear()
                try:
                    await asyncio.wait_for(waiter.wait(), timeout=TIMEOUT)
                except asyncio.TimeoutError:
                    await event.reply('Время истекло.')
                    del states[event.sender_id]
                    return
                password = data.get('password')
                await client_session.sign_in(password=password)
            except Exception:
                await event.reply('Неверный код. Введите код заново.')
                states[event.sender_id]['state'] = 'waiting_code'
                waiter.clear()
                try:
                    await asyncio.wait_for(waiter.wait(), timeout=TIMEOUT)
                except asyncio.TimeoutError:
                    await event.reply('Время истекло.')
                    del states[event.sender_id]
                    return
                code = data.get('code')
                await client_session.sign_in(phone, code)
        if not session_path:
            session_name = random_session_name()
            gcs_name = f'sessions/{session_name}'
            with open('/tmp/session.temp', 'w') as f:
                f.write(client_session.session.save())
            full_path = upload_to_gcs('/tmp/session.temp', gcs_name)
            os.remove('/tmp/session.temp')
            accounts_sheet.update_cell(3, 6, full_path)
            logger.info(f"Saved new session to {full_path}")
        await event.reply('Аккаунт авторизован. Приступаю к работе.')
        data['client'] = client_session
        states[event.sender_id]['state'] = 'join_channel'
        await join_and_parse(event, data)
    except Exception as e:
        logger.error(f"Auth error: {str(e)}")
        await event.reply(f'Ошибка: {str(e)}. Создаю новую сессию.')
        accounts_sheet.update_cell(3, 6, '') # Очистить F3
        await authorize_account(event, data) # Retry
async def join_and_parse(event, data):
    client_session = data['client']
    channel_link = data['channel_link']
    try:
        try:
            channel = await client_session.get_entity(channel_link)
        except ValueError as ve:
            if "No user has" in str(ve) or "The username is not occupied" in str(ve) or "not part of" in str(ve):
                hash_ = channel_link.split('/')[-1].lstrip('+')
                result = await client_session(ImportChatInviteRequest(hash=hash_))
                channel = result.chats[0]
                logger.info(f"Joined private channel via hash: {hash_}")
            else:
                raise
        # Join if not already
        try:
            await client_session(JoinChannelRequest(channel))
        except UserAlreadyParticipantError:
            pass
        await event.reply('Вступил в канал, начинаю парсить последние 5 постов с задержкой 10-15 сек')
        logger.info(f"Joined channel {channel_link}")
        posts = []
        async for message in client_session.iter_messages(channel, limit=50, reverse=False):
            if message.reply_markup: # Skip posts with buttons
                continue
            if not message.text and not message.photo and not message.video:
                continue
            if message.grouped_id:
                group_text = []
                group_entities = []
                group_media = []
                async for msg in client_session.iter_messages(channel, ids=message.id, add_offset=-10, limit=20):
                    if msg.grouped_id == message.grouped_id:
                        if msg.text:
                            group_text.append(msg.text)
                            if msg.entities:
                                # Adjust offsets for joined text
                                offset_adjust = sum(len(t) + 1 for t in group_text[:-1]) # +1 for \n
                                for ent in msg.entities:
                                    ent.offset += offset_adjust
                                group_entities.extend(msg.entities)
                        if msg.photo or msg.video:
                            group_media.append(msg)
                original_text = '\n'.join(group_text)
                if not original_text:
                    continue
                cleaned_text = clean_text(original_text, group_entities) # Clean links
                posts.append({'message': message, 'original_text': original_text, 'cleaned_text': cleaned_text, 'media': group_media})
            else:
                if not message.text:
                    continue
                media = [message] if (message.photo or message.video) and not message.document else []
                if not media and message.document:
                    continue # Пропустить если только документ
                original_text = message.text
                cleaned_text = clean_text(original_text, message.entities) # Clean links
                posts.append({'message': message, 'original_text': original_text, 'cleaned_text': cleaned_text, 'media': media})
            if len(posts) >= 5:
                break
            await asyncio.sleep(random.uniform(10, 15))
        last_row = len(posts_sheet.col_values(1)) + 1
        if last_row < 3:
            last_row = 3
        for post in posts:
            msg = post['message']
            channel_username = channel.username if channel.username else f'c/{str(channel.id)[4:]}'
            post_link = f'https://t.me/{channel_username}/{msg.id}'
            original_text = post['original_text']
            cleaned_text = post['cleaned_text']
            media_paths = []
            for i, media_msg in enumerate(post['media'], 1):
                ext = 'mp4' if media_msg.video else 'jpg'
                local_path = f'/tmp/media_{i}.{ext}'
                await media_msg.download_media(file=local_path)
                gcs_name = f'media/{random_media_name(i, ext)}'
                gcs_path = upload_to_gcs(local_path, gcs_name)
                media_paths.append(gcs_path)
                os.remove(local_path)
                await asyncio.sleep(random.uniform(10, 15))
            row_data = [channel_link, post_link, original_text, cleaned_text] + [''] * 10 # A-D + E-N (10)
            for j, path in enumerate(media_paths[:10]):
                row_data[4 + j] = path # E= index 4 (0-based)
            posts_sheet.append_row(row_data, table_range=f'A{last_row}')
            last_row += 1
        num_posts = len(posts)
        await event.reply(f'{num_posts} постов успешно сохранены в таблицу' if num_posts != 1 else '1 пост успешно сохранен в таблицу')
    except InviteHashExpiredError:
        await event.reply('Ошибка: Ссылка-приглашение истекла.')
    except Exception as e:
        logger.error(f"Join/parse error: {str(e)}")
        await event.reply(f'Ошибка: {str(e)}')
    finally:
        del states[event.sender_id]
        await client_session.disconnect()
def clean_text(text, entities):
    if not entities:
        # Fallback regex for plain links
        text = re.sub(r'(?:https?://)?(?:t\.me/|telegram\.me/|@)[a-zA-Z0-9_+/]+', '', text)
        return text.strip()
    # Rebuild text without internal links (remove substring)
    offset_shift = 0
    for entity in sorted(entities, key=lambda e: e.offset):
        if hasattr(entity, 'url') and (entity.url.startswith('https://t.me/') or entity.url.startswith('t.me/') or entity.url.startswith('telegram.me/') or entity.url.startswith('@')):
            start = entity.offset - offset_shift
            end = start + entity.length
            text = text[:start] + text[end:]
            offset_shift += entity.length
    return text.strip()
async def main():
    logger.info("Starting bot...")
    await client.start(bot_token=BOT_TOKEN)
    logger.info("Bot started successfully")
    await client.run_until_disconnected()
asyncio.run(main())
