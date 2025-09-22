import asyncio
import aiohttp
import logging
import html
from aiogram import Bot
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from db import is_already_sent, bulk_add_sent_ids
from device_info import get_or_create_device_info_for_token, get_headers_with_device_info
from collections import defaultdict

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# --- Helper function to fetch chat rooms ---
async def get_chat_rooms(session, token, device_info):
    """Fetches a list of all chat rooms for a given token."""
    url = "https://api.meeff.com/api/v2/chat/rooms?type=all"
    headers = get_headers_with_device_info({
        'User-Agent': "okhttp/4.12.0",
        'meeff-access-token': token
    }, device_info)
    try:
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                data = await response.json(content_type=None)
                return data.get("chatRooms", [])
            else:
                logging.error(f"Failed to get chat rooms for token {token[:10]}... Status: {response.status}")
                return []
    except Exception as e:
        logging.error(f"Exception while fetching chat rooms: {e}")
        return []

# --- Single Token Chatroom Messaging ---
async def send_message_to_everyone(token, custom_message, user_id, spam_enabled, tg_user_id, sent_ids, sent_ids_lock):
    """
    Sends a message to everyone in the chat list for a single token.
    Splits the message by commas and sends parts sequentially.
    """
    # 1. Split the message into parts. If a part is empty after stripping, it's ignored.
    message_parts = [msg.strip() for msg in custom_message.split(',') if msg.strip()]

    if not message_parts:
        logging.warning("Chatroom message was empty after splitting by comma.")
        return 0, 0, 0  # Total, Sent, Filtered

    device_info = await get_or_create_device_info_for_token(user_id, token)
    total_chats, sent_count, filtered_count = 0, 0, 0
    ids_to_persist = []

    async with aiohttp.ClientSession() as session:
        chat_rooms = await get_chat_rooms(session, token, device_info)
        total_chats = len(chat_rooms)

        for room in chat_rooms:
            room_id = room.get("_id")
            # Use the partner's user ID for spam filtering to avoid duplicates across accounts
            partner_user_id = room.get("partnerUserId")

            if not room_id or not partner_user_id:
                continue

            # 2. Spam filter check is done ONCE per recipient.
            if spam_enabled:
                async with sent_ids_lock:
                    if partner_user_id in sent_ids:
                        filtered_count += 1
                        continue
                    # Add to the in-memory set immediately to prevent race conditions
                    sent_ids.add(partner_user_id)
            
            try:
                # 3. Loop through and send each message part with a delay.
                for part in message_parts:
                    send_url = f"https://api.meeff.com/api/v2/chat/rooms/{room_id}/messages"
                    payload = {"message": part}
                    headers = get_headers_with_device_info({'meeff-access-token': token}, device_info)
                    
                    async with session.post(send_url, json=payload, headers=headers) as response:
                        if response.status != 200:
                            logging.error(f"Failed to send message part to room {room_id}. Status: {response.status}")
                            # If one part fails, stop sending to this user.
                            break
                    
                    await asyncio.sleep(0.5) # Delay between parts for a natural feel
                else:
                    # This block runs only if the for loop completes without a 'break'.
                    sent_count += 1
                    if spam_enabled:
                        ids_to_persist.append(partner_user_id)
            except Exception as e:
                logging.error(f"An error occurred while sending message parts to room {room_id}: {e}")

    # Persist all newly sent IDs to the database at once.
    if spam_enabled and ids_to_persist:
        await bulk_add_sent_ids(user_id, "chatroom", ids_to_persist)

    return total_chats, sent_count, filtered_count


# --- All Tokens Chatroom Messaging ---
async def send_message_to_everyone_all_tokens(tokens, custom_message, status_message, bot, tg_user_id, spam_enabled, token_names, use_in_memory_deduplication, user_id):
    """
    Sends a message to everyone for all active tokens.
    Splits the message by commas and sends parts sequentially.
    """
    # 1. Split the message into parts at the very beginning.
    message_parts = [msg.strip() for msg in custom_message.split(',') if msg.strip()]

    if not message_parts:
        await status_message.edit_text("<b>Error:</b> Message is empty.", parse_mode="HTML")
        return

    session_sent_ids = await is_already_sent(user_id, "chatroom", None, bulk=True) if spam_enabled else set()
    lock = asyncio.Lock()
    token_stats = defaultdict(lambda: {"sent": 0, "filtered": 0, "total": 0, "status": "Queued"})

    async def _worker(token):
        name = token_names.get(token, "Unknown")
        token_stats[token]["status"] = "Running"
        device_info = await get_or_create_device_info_for_token(user_id, token)
        ids_to_persist_for_token = []

        async with aiohttp.ClientSession() as session:
            chat_rooms = await get_chat_rooms(session, token, device_info)
            token_stats[token]["total"] = len(chat_rooms)

            for room in chat_rooms:
                room_id = room.get("_id")
                partner_user_id = room.get("partnerUserId")
                if not room_id or not partner_user_id:
                    continue
                
                # 2. Spam filter check is done ONCE per recipient.
                if spam_enabled:
                    async with lock:
                        if partner_user_id in session_sent_ids:
                            token_stats[token]["filtered"] += 1
                            continue
                        session_sent_ids.add(partner_user_id)
                
                try:
                    # 3. Loop through and send each message part with a delay.
                    for part in message_parts:
                        send_url = f"https://api.meeff.com/api/v2/chat/rooms/{room_id}/messages"
                        payload = {"message": part}
                        headers = get_headers_with_device_info({'meeff-access-token': token}, device_info)
                        
                        async with session.post(send_url, json=payload, headers=headers) as response:
                            if response.status != 200:
                                break
                        await asyncio.sleep(0.5)
                    else:
                        token_stats[token]["sent"] += 1
                        if spam_enabled:
                            ids_to_persist_for_token.append(partner_user_id)
                except Exception:
                    pass
        
        if spam_enabled and ids_to_persist_for_token:
            await bulk_add_sent_ids(user_id, "chatroom", ids_to_persist_for_token)
        token_stats[token]["status"] = "Done"

    async def _update_ui():
        while any(stats["status"] != "Done" for stats in token_stats.values()):
            total_sent = sum(s["sent"] for s in token_stats.values())
            total_filtered = sum(s["filtered"] for s in token_stats.values())
            
            header = f"<b>Multi-Chatroom Messaging</b>\nSent: {total_sent} | Filtered: {total_filtered}\n"
            lines = [header, "<code>{:<15} | {:>5} | {:>7} | {}</code>".format("Account", "Sent", "Filtered", "Status")]
            for token in tokens:
                name = token_names.get(token, "Unknown")[:15]
                stats = token_stats[token]
                lines.append("<code>{:<15} | {:>5} | {:>7} | {}</code>".format(name, stats['sent'], stats['filtered'], stats['status']))
            
            try:
                await status_message.edit_text("\n".join(lines), parse_mode="HTML")
            except Exception:
                pass
            await asyncio.sleep(2)

    ui_task = asyncio.create_task(_update_ui())
    await asyncio.gather(*[_worker(token) for token in tokens])
    ui_task.cancel()

    # Final UI update
    total_sent = sum(s["sent"] for s in token_stats.values())
    total_filtered = sum(s["filtered"] for s in token_stats.values())
    header = f"<b>✅ Multi-Chat Complete</b>\nSent: {total_sent} | Filtered: {total_filtered}\n"
    lines = [header, "<code>{:<15} | {:>5} | {:>7} | {}</code>".format("Account", "Sent", "Filtered", "Status")]
    for token in tokens:
        name = token_names.get(token, "Unknown")[:15]
        stats = token_stats[token]
        lines.append("<code>{:<15} | {:>5} | {:>7} | {}</code>".format(name, stats['sent'], stats['filtered'], stats['status']))
    await status_message.edit_text("\n".join(lines), parse_mode="HTML")
