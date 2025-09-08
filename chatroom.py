import aiohttp
import asyncio
import logging
from datetime import datetime
from db import is_already_sent, add_sent_id, bulk_add_sent_ids
from typing import List, Dict, Tuple
from aiogram import types

CHATROOM_URL = "https://api.meeff.com/chatroom/dashboard/v1"
MORE_CHATROOMS_URL = "https://api.meeff.com/chatroom/more/v1"
SEND_MESSAGE_URL = "https://api.meeff.com/chat/send/v2"
HEADERS = {
    'User-Agent': "okhttp/4.12.0",
    'Accept-Encoding': "gzip",
    'content-type': "application/json; charset=utf-8",
    'X-Device-Info': "iPhone15Pro-iOS17.5.1-6.6.2"
}

async def fetch_chatrooms(session, token, from_date=None):
    params = {'locale': "en"}
    if from_date:
        params['fromDate'] = from_date
    headers = HEADERS.copy()
    headers['meeff-access-token'] = token
    try:
        async with session.get(CHATROOM_URL, params=params, headers=headers, timeout=10) as response:
            if response.status != 200:
                logging.error(f"Failed to fetch chatrooms: {response.status}")
                return [], None
            data = await response.json()
            return data.get("rooms", []), data.get("next")
    except Exception as e:
        logging.error(f"Error fetching chatrooms: {str(e)}")
        return [], None

async def fetch_more_chatrooms(session, token, from_date):
    headers = HEADERS.copy()
    headers['meeff-access-token'] = token
    payload = {"fromDate": from_date, "locale": "en"}
    try:
        async with session.post(MORE_CHATROOMS_URL, json=payload, headers=headers, timeout=10) as response:
            if response.status != 200:
                logging.error(f"Failed to fetch more chatrooms: {response.status}")
                return [], None
            data = await response.json()
            return data.get("rooms", []), data.get("next")
    except Exception as e:
        logging.error(f"Error fetching more chatrooms: {str(e)}")
        return [], None

async def send_message(session, token, chatroom_id, message):
    headers = HEADERS.copy()
    headers['meeff-access-token'] = token
    payload = {"chatRoomId": chatroom_id, "message": message, "locale": "en"}
    try:
        async with session.post(SEND_MESSAGE_URL, json=payload, headers=headers, timeout=10) as response:
            if response.status != 200:
                logging.error(f"Failed to send message to {chatroom_id}: {response.status}")
                return None
            return await response.json()
    except Exception as e:
        logging.error(f"Error sending message to {chatroom_id}: {str(e)}")
        return None

async def process_chatroom_batch(session, token, chatrooms, message, chat_id, spam_enabled, sent_ids=None, sent_ids_lock=None):
    sent_count = 0
    filtered_count = 0
    filtered_rooms = []
    if spam_enabled:
        room_ids = [room.get('_id') for room in chatrooms]
        if sent_ids is not None:
            async with sent_ids_lock:
                filtered_rooms = [room for room in chatrooms if room.get('_id') not in sent_ids]
            filtered_count = len(chatrooms) - len(filtered_rooms)
        else:
            existing_ids = await is_already_sent(chat_id, "chatroom", room_ids, bulk=True)
            filtered_rooms = [room for room in chatrooms if room.get('_id') not in existing_ids]
            filtered_count = len(chatrooms) - len(filtered_rooms)
    else:
        filtered_rooms = chatrooms
    tasks = [send_message(session, token, room.get('_id'), message) for room in filtered_rooms]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    sent_count = sum(1 for result in results if result is not None)
    if spam_enabled and filtered_rooms:
        sent_ids_batch = [room.get('_id') for room in filtered_rooms]
        await bulk_add_sent_ids(chat_id, "chatroom", sent_ids_batch)
        if sent_ids is not None:
            async with sent_ids_lock:
                sent_ids.update(sent_ids_batch)
    return len(chatrooms), sent_count, filtered_count

async def send_message_to_everyone(
    token, message, chat_id=None, spam_enabled=True, 
    sent_ids=None, sent_ids_lock=None, status_entry=None):
    
    sent_count = 0
    total_chatrooms = 0
    filtered_count = 0
    from_date = None
    async with aiohttp.ClientSession() as session:
        while True:
            rooms, next_from = await (fetch_chatrooms(session, token, from_date)
                                    if from_date is None else
                                    fetch_more_chatrooms(session, token, from_date))
            if not rooms:
                break
            batch_total, batch_sent, batch_filtered = await process_chatroom_batch(
                session, token, rooms, message, chat_id, spam_enabled, sent_ids, sent_ids_lock
            )
            total_chatrooms += batch_total
            sent_count += batch_sent
            filtered_count += batch_filtered
            
            if status_entry is not None:
                status_entry['rooms'] = total_chatrooms
                status_entry['sent'] = sent_count
                status_entry['filtered'] = filtered_count
                status_entry['status'] = "Processing"

            if not next_from:
                break
            from_date = next_from
    return total_chatrooms, sent_count, filtered_count

async def send_message_to_everyone_all_tokens(
    tokens: List[str], message: str, status_message: types.Message = None, bot=None,
    chat_id: int = None, spam_enabled: bool = True, token_names: Dict[str, str] = None,
    use_in_memory_deduplication: bool = False) -> None:
    
    # MODIFIED: Use the robust token-as-key data structure
    token_status: Dict[str, Dict] = {}
    sent_ids = set() if use_in_memory_deduplication else None
    sent_ids_lock = asyncio.Lock() if use_in_memory_deduplication else None

    async def _worker(token: str, idx: int):
        # The key for updates is the unique token
        status_entry = token_status[token]
        try:
            rooms, sent, filtered = await send_message_to_everyone(
                token, message, chat_id=chat_id, spam_enabled=spam_enabled, 
                sent_ids=sent_ids, sent_ids_lock=sent_ids_lock,
                status_entry=status_entry
            )
            status_entry['status'] = "Done"
            return True
        except Exception as e:
            logging.error(f"[Token {idx}/{len(tokens)}] failed: {str(e)}")
            status_entry['status'] = f"Failed: {str(e)[:20]}..."
            return False

    async def _refresh_ui():
        last_message = ""
        while any(d['status'] in ["Processing", "Queued"] for d in token_status.values()):
            table_header = "Account         │Rooms │Sent │Filter │Status"
            table_rows = []
            for status_dict in token_status.values():
                name = status_dict['name']
                rooms = status_dict['rooms']
                sent = status_dict['sent']
                filtered = status_dict['filtered']
                status = status_dict['status']
                table_rows.append(f"{name:<15} │{rooms:>5} │{sent:>4} │{filtered:>6} │{status}")
            
            table_content = "\n".join([table_header] + table_rows)
            current_message = f"🔄 <b>Chatroom AIO Status</b>\n<pre>{table_content}</pre>"

            if current_message != last_message and bot and chat_id and status_message:
                try:
                    await bot.edit_message_text(
                        chat_id=chat_id, message_id=status_message.message_id,
                        text=current_message, parse_mode="HTML"
                    )
                    last_message = current_message
                except Exception as e:
                    if "message is not modified" not in str(e):
                        logging.error(f"Error updating status: {e}")
            await asyncio.sleep(0.5)

    # MODIFIED: Initialize the new data structure
    for token in tokens:
        name = token_names.get(token, f"Token...{token[-4:]}") if token_names else f"Token...{token[-4:]}"
        token_status[token] = {'name': name, 'rooms': 0, 'sent': 0, 'filtered': 0, 'status': 'Queued'}

    ui_task = asyncio.create_task(_refresh_ui()) if bot and chat_id and status_message else None
    worker_tasks = [_worker(token, idx) for idx, token in enumerate(tokens, start=1)]
    await asyncio.gather(*worker_tasks)
    if ui_task:
        await ui_task

    successful_tokens = sum(1 for d in token_status.values() if d['status'] == "Done")
    
    if bot and chat_id and status_message:
        success_rate = (successful_tokens / len(tokens)) * 100 if len(tokens) > 0 else 0
        success_emoji = "✅" if success_rate > 90 else "⚠️" if success_rate > 70 else "❌"
        
        table_header = "Account         │Rooms │Sent │Filter │Status"
        table_rows = []
        for status_dict in token_status.values():
            name = status_dict['name']
            rooms = status_dict['rooms']
            sent = status_dict['sent']
            filtered = status_dict['filtered']
            status = status_dict['status']
            table_rows.append(f"{name:<15} │{rooms:>5} │{sent:>4} │{filtered:>6} │{status}")
        
        table_content = "\n".join([table_header] + table_rows)
        final_message = (
            f"{success_emoji} <b>Chatroom AIO Completed</b> - {successful_tokens}/{len(tokens)} tokens ({success_rate:.1f}%)\n"
            f"<pre>{table_content}</pre>"
        )

        try:
            await bot.edit_message_text(
                chat_id=chat_id, message_id=status_message.message_id,
                text=final_message, parse_mode="HTML"
            )
        except Exception as e:
            if "message is not modified" not in str(e):
                logging.error(f"Error in final status update: {e}")
