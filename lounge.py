import asyncio
import aiohttp
import logging
from typing import List, Dict, Set
from aiogram import types
from db import bulk_add_sent_ids, is_already_sent
from device_info import get_or_create_device_info_for_token, get_headers_with_device_info

LOUNGE_URL = "https://api.meeff.com/lounge/dashboard/v1"
CHATROOM_URL = "https://api.meeff.com/chatroom/open/v2"
SEND_MESSAGE_URL = "https://api.meeff.com/chat/send/v2"
BASE_HEADERS = {
    'User-Agent': "okhttp/4.12.0",
    'Accept-Encoding': "gzip",
    'content-type': "application/json; charset=utf-8",
}

# Configure logging
logger = logging.getLogger(__name__)

async def fetch_lounge_users(session: aiohttp.ClientSession, token: str, user_id: int) -> List[Dict]:
    """Fetch users from lounge with a persistent session and consistent device info."""
    device_info = await get_or_create_device_info_for_token(user_id, token)
    headers = get_headers_with_device_info(BASE_HEADERS, device_info)
    headers['meeff-access-token'] = token
    
    try:
        async with session.get(LOUNGE_URL, params={'locale': "en"}, headers=headers, timeout=10) as response:
            if response.status != 200:
                logger.warning(f"Failed to fetch lounge users (Status: {response.status})")
                return []
            data = await response.json()
            return data.get("both", [])
    except Exception as e:
        logger.error(f"Error fetching lounge users: {str(e)}")
        return []

async def open_chatroom_and_send(
    session: aiohttp.ClientSession, token: str, target_meeff_id: str, message: str, telegram_user_id: int
) -> bool:
    """
    Atomically opens a chatroom and sends one or more comma-separated messages
    using consistent device info.
    """
    device_info = await get_or_create_device_info_for_token(telegram_user_id, token)
    headers = get_headers_with_device_info(BASE_HEADERS, device_info)
    headers['meeff-access-token'] = token
    
    # 1. Open Chatroom
    chatroom_id = None
    try:
        payload = {"waitingRoomId": target_meeff_id, "locale": "en"}
        async with session.post(CHATROOM_URL, json=payload, headers=headers, timeout=10) as response:
            if response.status == 412:
                logger.info(f"User {target_meeff_id} has disabled chat.")
                return False
            if response.status != 200:
                logger.warning(f"Failed to open chatroom with {target_meeff_id} (Status: {response.status})")
                return False
            data = await response.json()
            chatroom_id = data.get("chatRoom", {}).get("_id")
    except Exception as e:
        logger.error(f"Error opening chatroom with {target_meeff_id}: {e}")
        return False

    if not chatroom_id:
        return False
        
    # 2. Split message and send each part
    messages_to_send = [msg.strip() for msg in message.split(',') if msg.strip()]
    
    if not messages_to_send:
        logger.warning(f"Message for {target_meeff_id} was empty after splitting.")
        return False

    any_message_sent = False
    for i, msg_part in enumerate(messages_to_send):
        try:
            payload = {"chatRoomId": chatroom_id, "message": msg_part, "locale": "en"}
            async with session.post(SEND_MESSAGE_URL, json=payload, headers=headers, timeout=10) as response:
                if response.status == 200:
                    logger.info(f"Sent message part {i+1}/{len(messages_to_send)} to {target_meeff_id}")
                    any_message_sent = True 
                else:
                    logger.warning(f"Failed to send message part {i+1} to {target_meeff_id} (Status: {response.status})")
            
            if i < len(messages_to_send) - 1:
                await asyncio.sleep(0.5)

        except Exception as e:
            logger.error(f"Error sending message part {i+1} to {target_meeff_id}: {e}")
    
    return any_message_sent

async def process_lounge_batch(
    session: aiohttp.ClientSession, token: str, users: List[Dict], message: str,
    sent_ids: Set[str], processing_ids: Set[str], lock: asyncio.Lock, user_id: int
) -> tuple[int, int, List[str]]:
    """Processes a batch of users, passing the user_id for consistent device info."""
    tasks = []
    users_to_process = []
    filtered_count = 0
    
    async with lock:
        for user in users:
            user_meeff_id = user.get("user", {}).get("_id")
            if not user_meeff_id:
                continue
            if user_meeff_id not in sent_ids and user_meeff_id not in processing_ids:
                users_to_process.append(user)
                processing_ids.add(user_meeff_id)
            else:
                filtered_count += 1
    
    for user in users_to_process:
        user_meeff_id = user["user"]["_id"]
        tasks.append(open_chatroom_and_send(session, token, user_meeff_id, message, user_id))
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    successful_ids = []
    async with lock:
        for i, result in enumerate(results):
            user_meeff_id = users_to_process[i]["user"]["_id"]
            if result is True:
                successful_ids.append(user_meeff_id)
            processing_ids.discard(user_meeff_id)
            
    return len(successful_ids), filtered_count, successful_ids


async def send_lounge(
    token: str, message: str, status_message: types.Message,
    bot, chat_id: int, spam_enabled: bool, user_id: int
) -> None:
    """Sends a message to all users in the lounge for a single account, re-fetching until empty."""
    total_sent = 0
    total_filtered = 0
    sent_ids = await is_already_sent(chat_id, "lounge", None, bulk=True) if spam_enabled else set()
    processing_ids = set()
    lock = asyncio.Lock()

    async def update_status(msg: str):
        try:
            await bot.edit_message_text(chat_id=chat_id, message_id=status_message.message_id, text=msg, parse_mode="HTML")
        except Exception:
            pass

    await update_status("⏳ <b>Lounge Messaging:</b> Starting...")
    async with aiohttp.ClientSession() as session:
        # NEW: Start a loop that will continue until no users are found.
        while True:
            await update_status(
                f"⏳ <b>Lounge Messaging:</b> Fetching new users...\n"
                f"Sent: {total_sent} | Filtered: {total_filtered}"
            )
            
            users = await fetch_lounge_users(session, token, user_id)
            
            # NEW: If no users are returned, break the loop. This is the exit condition.
            if not users:
                await update_status(
                    f"✅ <b>Lounge Completed:</b> No more users found.\n"
                    f"Total Sent: {total_sent} | Total Filtered: {total_filtered}"
                )
                break

            batch_sent, batch_filtered, successful_ids = await process_lounge_batch(
                session, token, users, message, sent_ids, processing_ids, lock, user_id
            )
            
            # NEW: Accumulate totals from each batch.
            total_sent += batch_sent
            total_filtered += batch_filtered
            
            if spam_enabled and successful_ids:
                await bulk_add_sent_ids(chat_id, "lounge", successful_ids)
                # NEW: Add successfully sent IDs to the main set to avoid re-sending in the same run.
                sent_ids.update(successful_ids)
            
            # NEW: Short delay before fetching the next batch to avoid rate-limiting.
            await asyncio.sleep(2)


async def send_lounge_all_tokens(
    tokens_data: List[Dict], message: str, status_message: types.Message,
    bot, chat_id: int, spam_enabled: bool, user_id: int
) -> None:
    """Processes lounge messaging concurrently for all tokens with proper deduplication and re-fetching."""
    token_status = {
        td["token"]: {
            "name": td.get("name", f"Acc {i+1}"),
            "sent": 0,
            "filtered": 0,
            "status": "Queued"
        } for i, td in enumerate(tokens_data)
    }
    
    sent_ids = await is_already_sent(chat_id, "lounge", None, bulk=True) if spam_enabled else set()
    processing_ids = set()
    lock = asyncio.Lock()
    running = True

   async def _worker(token_data: Dict):
        token = token_data.get("token")
        
        async with aiohttp.ClientSession() as session:
            # Loop for this specific worker/token.
            while True:
                token_status[token]["status"] = "Fetching"
                users = await fetch_lounge_users(session, token, user_id)
                
                # If no users are found from the API, this worker's job is done.
                if not users:
                    # If it never sent anything, mark as 'No users', otherwise 'Done'.
                    if token_status[token]["sent"] == 0:
                        token_status[token]["status"] = "No users"
                    else:
                        token_status[token]["status"] = "Done"
                    break # Exit the loop for this worker.

                # Update status to show it's working on a batch.
                token_status[token]["status"] = "Processing"
                
                batch_sent, batch_filtered, successful_ids = await process_lounge_batch(
                    session, token, users, message, sent_ids, processing_ids, lock, user_id
                )

                # Update the token's specific stats in the shared dictionary.
                token_status[token]["sent"] += batch_sent
                token_status[token]["filtered"] += batch_filtered

                # Update the global 'sent_ids' set under a lock.
                async with lock:
                    sent_ids.update(successful_ids)
                    if spam_enabled and successful_ids:
                        await bulk_add_sent_ids(chat_id, "lounge", successful_ids)
                

                if users and batch_sent == 0:
                    # If it never sent anything, mark as 'No users', otherwise 'Done'.
                    if token_status[token]["sent"] == 0:
                        token_status[token]["status"] = "No users"
                    else:
                        token_status[token]["status"] = "Done"
                    break # Exit the while True loop for this account.
                
                # Short delay before the next fetch for this token.
                await asyncio.sleep(2)

    async def _refresh_ui():
        last_message = ""
        while running:
            lines = ["🧾 <b>AIO Lounge Status</b>", "<pre>Account   | Sent | Filtered | State</pre>"]
            for status in token_status.values():
                name = status['name']
                display_name = name[:10].ljust(10) if len(name) <= 10 else name[:9] + '…'
                lines.append(f"<pre>{display_name}| {status['sent']:<4} | {status['filtered']:<8} | {status['status']}</pre>")
            
            current_message = "\n".join(lines)
            if current_message != last_message:
                try:
                    await bot.edit_message_text(
                        chat_id=chat_id, message_id=status_message.message_id,
                        text=current_message, parse_mode="HTML"
                    )
                    last_message = current_message
                except Exception as e:
                    if "message is not modified" not in str(e):
                        logger.error(f"UI refresh error: {e}")
            await asyncio.sleep(1)

    ui_task = asyncio.create_task(_refresh_ui())
    worker_tasks = [asyncio.create_task(_worker(td)) for td in tokens_data]
    await asyncio.gather(*worker_tasks, return_exceptions=True)

    running = False
    await asyncio.sleep(1.1)
    ui_task.cancel()

    total_sent = sum(s["sent"] for s in token_status.values())
    
    final_lines = [f"✅ <b>AIO Lounge Completed</b> (Total Sent: {total_sent})", "<pre>Account   | Sent | Filtered | State</pre>"]
    for status in token_status.values():
        name = status['name']
        display_name = name[:10].ljust(10) if len(name) <= 10 else name[:9] + '…'
        final_lines.append(f"<pre>{display_name}| {status['sent']:<4} | {status['filtered']:<8} | {status['status']}</pre>")
    
    await bot.edit_message_text(
        chat_id=chat_id, message_id=status_message.message_id,
        text="\n".join(final_lines), parse_mode="HTML"
    )
