import aiohttp
import asyncio
import logging
from datetime import datetime
from db import is_already_sent, add_sent_id, bulk_add_sent_ids
from typing import List, Dict
from aiogram import types
from online_status import set_online_status, refresh_user_location

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

async def process_chatroom_batch(session, token, chatrooms, message, chat_id, spam_enabled, sent_ids=None, sent_ids_lock=None):
    """Process a batch of chatrooms concurrently, with optional in-memory deduplication."""
    sent_count = 0
    filtered_count = 0
    filtered_rooms = []

    # Filter out already sent rooms
    if spam_enabled:
        room_ids = [room.get('_id') for room in chatrooms]
        # Check in-memory sent IDs first (if enabled)
        if sent_ids is not None:
            async with sent_ids_lock:
                filtered_rooms = [room for room in chatrooms if room.get('_id') not in sent_ids]
            filtered_count = len(chatrooms) - len(filtered_rooms)
            logging.info(f"In-memory deduplication: Total={len(chatrooms)}, Filtered={filtered_count}")
        else:
            # Database check
            existing_ids = await is_already_sent(chat_id, "chatroom", room_ids, bulk=True)
            filtered_rooms = [room for room in chatrooms if room.get('_id') not in existing_ids]
            filtered_count = len(chatrooms) - len(filtered_rooms)
            logging.info(f"Database deduplication: Total={len(chatrooms)}, Filtered={filtered_count}")
    else:
        filtered_rooms = chatrooms
        logging.warning("Spam check disabled; duplicate messages may be sent")

    # Process all filtered rooms concurrently
    tasks = [send_message(session, token, room.get('_id'), message) for room in filtered_rooms]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    sent_count = sum(1 for result in results if result is not None)

    # Update sent IDs (database and in-memory)
    if spam_enabled and filtered_rooms:
        sent_ids_batch = [room.get('_id') for room in filtered_rooms]
        # Update database
        await bulk_add_sent_ids(chat_id, "chatroom", sent_ids_batch)
        # Update in-memory set (if enabled)
        if sent_ids is not None:
            async with sent_ids_lock:
                sent_ids.update(sent_ids_batch)
            logging.info(f"Updated in-memory sent IDs: {len(sent_ids_batch)} added")

    logging.info(f"Batch processed: Total={len(chatrooms)}, Sent={sent_count}, Filtered={filtered_count}")
    return len(chatrooms), sent_count, filtered_count

async def send_message_to_everyone(
    token,
    message,
    status_message=None,
    bot=None,
    chat_id=None,
    spam_enabled=True,  # Default to True to avoid duplicates
    sent_ids=None,  # Optional in-memory sent IDs
    sent_ids_lock=None  # Lock for thread-safe in-memory updates
):
    """
    Send messages to all chatrooms for a single token, with proper filtered count.
    Returns a tuple: (total_chatrooms, messages_sent, filtered_count).
    """
    sent_count = 0
    total_chatrooms = 0
    filtered_count = 0
    from_date = None

    if not spam_enabled:
        logging.warning("Spam check disabled for token; may send duplicate messages")

    async with aiohttp.ClientSession() as session:
        while True:
            # Fetch next page of chatrooms
            rooms, next_from = await (fetch_chatrooms(session, token, from_date)
                                    if from_date is None else
                                    fetch_more_chatrooms(session, token, from_date))
            if not rooms:
                break

            # Process the batch of rooms
            batch_total, batch_sent, batch_filtered = await process_chatroom_batch(
                session, token, rooms, message, chat_id, spam_enabled, sent_ids, sent_ids_lock
            )
            total_chatrooms += batch_total
            sent_count += batch_sent
            filtered_count += batch_filtered

            # Update status in Telegram
            if bot and chat_id and status_message:
                try:
                    await bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=status_message.message_id,
                        text=f"Chatrooms: {total_chatrooms}, Sent: {sent_count}, Filtered: {filtered_count}",
                    )
                except Exception as e:
                    logging.error(f"Error updating status message: {str(e)}")

            logging.info(f"Progress: Total={total_chatrooms}, Sent={sent_count}, Filtered={filtered_count}")

            if not next_from:
                break
            from_date = next_from

    # Final update
    if bot and chat_id and status_message:
        try:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=status_message.message_id,
                text=f"Finished. Total Chatrooms: {total_chatrooms}, Sent: {sent_count}, Filtered: {filtered_count}"
            )
        except Exception as e:
            logging.error(f"Error updating final status: {str(e)}")

    logging.info(f"Finished token. Total={total_chatrooms}, Sent={sent_count}, Filtered={filtered_count}")
    return total_chatrooms, sent_count, filtered_count

async def send_message_to_everyone_all_tokens(
    tokens: List[str],
    message: str,
    status_message: types.Message = None,
    bot=None,
    chat_id: int = None,
    spam_enabled: bool = True,  # Default to True
    token_names: Dict[str, str] = None,  # Token -> account name mapping
    use_in_memory_deduplication: bool = False  # Enable in-memory deduplication
) -> None:
    """
    Send messages to everyone for multiple tokens concurrently,
    with proper filtered count and optional in-memory deduplication.
    """
    token_status: Dict[str, Tuple[int, int, int, str]] = {}
    sent_ids = set() if use_in_memory_deduplication else None
    sent_ids_lock = asyncio.Lock() if use_in_memory_deduplication else None

    async def _worker(token: str, idx: int):
        display_name = token_names.get(token, token[:6]) if token_names else token[:6]
        token_status[display_name] = (0, 0, 0, "Processing")

        try:
            rooms, sent, filtered = await send_message_to_everyone(
                token,
                message,
                status_message=None,
                bot=None,
                chat_id=chat_id,
                spam_enabled=spam_enabled,
                sent_ids=sent_ids,
                sent_ids_lock=sent_ids_lock
            )
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
            await asyncio.sleep(0.5)

    # Initialize token status
    for idx, token in enumerate(tokens, start=1):
        display_name = token_names.get(token, token[:6]) if token_names else token[:6]
        token_status[display_name] = (0, 0, 0, "Queued")

    if use_in_memory_deduplication:
        logging.info("In-memory deduplication enabled")
    if not spam_enabled:
        logging.warning("Spam check disabled for all tokens; duplicates possible")

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
        f"Total Rooms={grand_rooms}, Total Sent={grand_sent}, Total Filtered={grand_filtered}"
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
