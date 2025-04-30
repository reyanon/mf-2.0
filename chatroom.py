import aiohttp
import asyncio
import logging
from datetime import datetime
from db import is_already_sent, add_sent_id, bulk_add_sent_ids
from typing import List, Dict
from aiogram import types

CHATROOM_URL = "https://api.meeff.com/chatroom/dashboard/v1"
MORE_CHATROOMS_URL = "https://api.meeff.com/chatroom/more/v1"
SEND_MESSAGE_URL = "https://api.meeff.com/chat/send/v2"
HEADERS = {
    'User-Agent': "okhttp/4.12.0",
    'Accept-Encoding': "gzip",
    'content-type': "application/json; charset=utf-8"
}

async def fetch_chatrooms(session, token, from_date=None):
    """Optimized chatroom fetching based on Code 2's approach"""
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
    """Optimized more chatrooms fetching"""
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
    """Optimized message sending with better error handling"""
    headers = HEADERS.copy()
    headers['meeff-access-token'] = token
    payload = {
        "chatRoomId": chatroom_id,
        "message": message,
        "locale": "en"
    }

    try:
        async with session.post(SEND_MESSAGE_URL, json=payload, headers=headers, timeout=10) as response:
            if response.status != 200:
                logging.error(f"Failed to send message to {chatroom_id}: {response.status}")
                return None
            return await response.json()
    except Exception as e:
        logging.error(f"Error sending message to {chatroom_id}: {str(e)}")
        return None

async def process_chatroom_batch(session, token, chatrooms, message, chat_id, spam_enabled):
    """Process a batch of chatrooms concurrently"""
    sent_count = 0
    filtered_rooms = []
    
    # First filter out already sent rooms if spam filter is enabled
    if spam_enabled:
        room_ids = [room.get('_id') for room in chatrooms]
        existing_ids = await is_already_sent(chat_id, "chatroom", room_ids, bulk=True)
        filtered_rooms = [room for room in chatrooms if room.get('_id') not in existing_ids]
    else:
        filtered_rooms = chatrooms
    
    # Process all filtered rooms concurrently
    tasks = []
    for room in filtered_rooms:
        room_id = room.get('_id')
        tasks.append(send_message(session, token, room_id, message))
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    sent_count = sum(1 for result in results if result is not None)
    
    # Bulk add sent IDs to database if spam filter is enabled
    if spam_enabled and filtered_rooms:
        sent_ids = [room.get('_id') for room in filtered_rooms]
        await bulk_add_sent_ids(chat_id, "chatroom", sent_ids)
    
    return len(chatrooms), sent_count

async def send_message_to_everyone(token, message, status_message=None, bot=None, chat_id=None, spam_enabled=False):
    """
    Optimized version that processes chatrooms in batches concurrently.
    Returns a tuple: (total_chatrooms, messages_sent).
    """
    sent_count = 0
    total_chatrooms = 0
    from_date = None

    async with aiohttp.ClientSession() as session:
        while True:
            # fetch next page of chatrooms
            rooms, next_from = await (fetch_chatrooms(session, token, from_date)
                                    if from_date is None else
                                    fetch_more_chatrooms(session, token, from_date))
            if not rooms:
                break

            # process the batch of rooms
            batch_total, batch_sent = await process_chatroom_batch(
                session, token, rooms, message, chat_id, spam_enabled
            )
            total_chatrooms += batch_total
            sent_count += batch_sent

            # update status in Telegram
            if bot and chat_id and status_message:
                try:
                    await bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=status_message.message_id,
                        text=f"Chatrooms: {total_chatrooms} Messages sent: {sent_count}",
                    )
                except Exception as e:
                    logging.error(f"Error updating status message: {str(e)}")

            logging.info(f"Sent messages to {sent_count}/{total_chatrooms} chatrooms.")

            if not next_from:
                break
            from_date = next_from

    # final update
    if bot and chat_id and status_message:
        try:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=status_message.message_id,
                text=f"Finished sending messages. Total Chatrooms: {total_chatrooms}, Messages sent: {sent_count}"
            )
        except Exception as e:
            logging.error(f"Error updating final status: {str(e)}")

    logging.info(f"Finished sending messages. Total Chatrooms: {total_chatrooms}, Messages sent: {sent_count}")
    return total_chatrooms, sent_count



async def send_message_to_everyone_all_tokens(
    tokens: List[str],
    message: str,
    status_message: types.Message = None,
    bot=None,
    chat_id: int = None,
    spam_enabled: bool = False,
    token_names: Dict[str, str] = None  # Token -> account name mapping
) -> None:
    """
    Send messages to everyone for multiple tokens concurrently,
    with improved UI updates and error handling.
    """
    token_status: Dict[str, Tuple[int, int, int, str]] = {}

    async def _worker(token: str, idx: int):
        # Fix: Use the actual token name from the mapping if available
        # Make sure we have a fallback if token not in mapping
        display_name = None
        if token_names and token in token_names:
            display_name = token_names[token]
        else:
            display_name = token[:6]
            
        rooms = sent = filtered = 0
        token_status[display_name] = (rooms, sent, filtered, "Processing")

        try:
            result = await send_message_to_everyone(
                token,
                message,
                status_message=None,
                bot=None,
                chat_id=chat_id,
                spam_enabled=spam_enabled
            )

            if isinstance(result, tuple):
                if len(result) == 3:
                    rooms, sent, filtered = result
                elif len(result) == 2:
                    rooms, sent = result
                    filtered = 0
            else:
                rooms = result
                sent = result
                filtered = 0

            logging.info(f"[Token {idx}/{len(tokens)}] Rooms: {rooms}, Sent: {sent}, Filtered: {filtered}")
            token_status[display_name] = (rooms, sent, filtered, "Done")
            return True

        except Exception as e:
            logging.error(f"[Token {idx}/{len(tokens)}] failed: {str(e)}")
            token_status[display_name] = (0, 0, 0, f"Failed: {str(e)[:20]}...")
            return False

    async def _refresh_ui():
        last_message = ""
        while any(status[3] == "Processing" for status in token_status.values()):
            lines = [
                "🔄 <b>Chatroom AIO Status</b>\n",
                "<pre style='background-color:#f4f4f4;padding:5px;border-radius:5px;'>Account  │Rooms │Sent │Filter │Status</pre>"
            ]
            for tid, (rooms, sent, filtered, status) in token_status.items():
                lines.append(
                    f"<pre>{tid:<9}│{rooms:>5} │{sent:>4} │{filtered:>6} │{status}</pre>"
                )

            current_message = "\n".join(lines)

            if current_message != last_message and bot and chat_id and status_message:
                try:
                    await bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=status_message.message_id,
                        text=current_message,
                        parse_mode="HTML"
                    )
                    last_message = current_message
                except Exception as e:
                    if "message is not modified" not in str(e):
                        logging.error(f"Error updating status: {e}")

            await asyncio.sleep(1)

    # Initialize token status for all tokens
    for idx, token in enumerate(tokens, start=1):
        # Fix: Use the actual token name from the mapping if available
        # Make sure we have a fallback if token not in mapping
        display_name = None
        if token_names and token in token_names:
            display_name = token_names[token]
        else:
            display_name = token[:6]
            
        token_status[display_name] = (0, 0, 0, "Queued")

    ui_task = asyncio.create_task(_refresh_ui()) if bot and chat_id and status_message else None
    worker_tasks = [_worker(token, idx) for idx, token in enumerate(tokens, start=1)]
    results = await asyncio.gather(*worker_tasks)

    if ui_task:
        await ui_task

    successful_tokens = sum(1 for result in results if result)
    grand_rooms = sum(rooms for rooms, _, _, _ in token_status.values())
    grand_sent = sum(sent for _, sent, _, _ in token_status.values())
    grand_filtered = sum(filtered for _, _, filtered, _ in token_status.values())

    logging.info(
        f"[AllTokens] Finished: {successful_tokens}/{len(tokens)} tokens succeeded. "
        f"Total Rooms={grand_rooms}, Total Messages={grand_sent}, Total Filtered={grand_filtered}"
    )

    if bot and chat_id and status_message:
        success_rate = (successful_tokens / len(tokens)) * 100 if len(tokens) > 0 else 0
        success_emoji = "✅" if success_rate > 90 else "⚠️" if success_rate > 70 else "❌"

        lines = [
            f"{success_emoji} <b>Chatroom AIO Completed</b> - {successful_tokens}/{len(tokens)} tokens ({success_rate:.1f}%)\n",
            "<pre>Account  │Rooms │Sent │Filter │Status</pre>"
        ]
        for tid, (rooms, sent, filtered, status) in token_status.items():
            lines.append(
                f"<pre>{tid:<9}│{rooms:>5} │{sent:>4} │{filtered:>6} │{status}</pre>"
            )

        try:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=status_message.message_id,
                text="\n".join(lines),
                parse_mode="HTML"
            )
        except Exception as e:
            if "message is not modified" not in str(e):
                logging.error(f"Error in final status update: {e}")
