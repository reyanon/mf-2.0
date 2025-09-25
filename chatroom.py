import aiohttp
import asyncio
import logging
import html
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from typing import List

CHATROOM_URL = "https://api.meeff.com/chatroom/dashboard/v1"
MORE_CHATROOMS_URL = "https://api.meeff.com/chatroom/more/v1"
SEND_MESSAGE_URL = "https://api.meeff.com/chat/send/v2"
HEADERS = {
    'User-Agent': "okhttp/4.12.0",
    'Accept-Encoding': "gzip",
    'content-type': "application/json; charset=utf-8"
}

# --- Helpers for API Calls (Unchanged) ---

async def fetch_chatrooms(session, token, from_date=None):
    headers = {**HEADERS, 'meeff-access-token': token}
    params = {'locale': "en"}
    if from_date:
        params['fromDate'] = from_date
    async with session.get(CHATROOM_URL, params=params, headers=headers) as resp:
        if resp.status != 200:
            logging.error(f"Failed to fetch chatrooms: {resp.status}")
            return [], None
        data = await resp.json()
        return data.get("rooms", []), data.get("next")

async def fetch_more_chatrooms(session, token, from_date):
    headers = {**HEADERS, 'meeff-access-token': token}
    payload = {"fromDate": from_date, "locale": "en"}
    async with session.post(MORE_CHATROOMS_URL, json=payload, headers=headers) as resp:
        if resp.status != 200:
            logging.error(f"Failed to fetch more chatrooms: {resp.status}")
            return [], None
        data = await resp.json()
        return data.get("rooms", []), data.get("next")

async def send_message(session, token, chatroom_id, message):
    """Sends a SINGLE message to a chatroom."""
    headers = {**HEADERS, 'meeff-access-token': token}
    payload = {"chatRoomId": chatroom_id, "message": message, "locale": "en"}
    async with session.post(SEND_MESSAGE_URL, json=payload, headers=headers) as resp:
        if resp.status != 200:
            logging.warning(f"Failed to send message to {chatroom_id}: Status {resp.status}")
            return None
        return await resp.json()

# --- NEW: Helper function to send multiple messages with a delay ---
async def send_multiple_messages_to_chatroom(session, token: str, chatroom_id: str, messages: List[str]) -> int:
    """
    Sends a list of messages sequentially to a single chatroom,
    with a delay between each message.
    Returns the number of messages successfully sent.
    """
    if not messages:
        return 0

    success_count = 0
    for i, msg in enumerate(messages):
        try:
            result = await send_message(session, token, chatroom_id, msg)
            if result:
                success_count += 1
            
            # Add a delay if it's not the last message in the list
            if i < len(messages) - 1:
                await asyncio.sleep(0.5)  # 0.5-second delay
        except Exception as e:
            logging.error(f"Error sending part {i+1} to {chatroom_id}: {e}")
            
    return success_count

# --- MODIFIED: Main Feature function ---
async def send_message_to_everyone(token, messages, status_message=None, bot=None, chat_id=None):
    # This part correctly prepares the list of messages from a string
    if isinstance(messages, str):
        messages = [msg.strip() for msg in messages.split(",") if msg.strip()]

    sent_count, total_chatrooms, from_date = 0, 0, None
    connector = aiohttp.TCPConnector(limit=30)
    
    async with aiohttp.ClientSession(connector=connector) as session:
        while True:
            chatrooms, next_from_date = (
                await fetch_chatrooms(session, token)
                if from_date is None else
                await fetch_more_chatrooms(session, token, from_date)
            )
            
            if not chatrooms:
                logging.info("No more chatrooms found.")
                break

            total_chatrooms += len(chatrooms)

            # MODIFIED: Create tasks for the new helper function.
            # This now processes each chatroom concurrently, while messages
            # within each chatroom are sent sequentially.
            send_tasks = [
                send_multiple_messages_to_chatroom(session, token, chatroom["_id"], messages)
                for chatroom in chatrooms
            ]
            
            results = await asyncio.gather(*send_tasks)
            
            # MODIFIED: Accurately sum the number of successfully sent messages
            sent_count += sum(results)

            if bot and chat_id and status_message:
                await bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=status_message.message_id,
                    text=f"Chatrooms: {total_chatrooms} | Messages sent: {sent_count}",
                )
            
            logging.info(f"Processed {len(chatrooms)} chatrooms.")
            
            if not next_from_date:
                break
            from_date = next_from_date

    logging.info(f"Finished. Total Chatrooms: {total_chatrooms}, Messages sent: {sent_count}")
    if bot and chat_id and status_message:
        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=status_message.message_id,
            text=f"✅ Finished! \nTotal Chatrooms: {total_chatrooms} | Messages sent: {sent_count}",
        )
    return sent_count

# --- Command Handler for /chatroom (Unchanged) ---

async def chatroom_command_handler(message, has_valid_access, get_current_account, get_tokens, user_states):
    user_id = message.chat.id
    if not has_valid_access(user_id):
        await message.reply("You are not authorized to use this bot.")
        return
    tokens = get_tokens(user_id)
    if not tokens:
        await message.reply("No accounts found. Please add an account first.")
        return
    token = get_current_account(user_id)
    if not token:
        await message.reply("No active account found. Please set an account before sending messages.")
        return
    command_text = message.text.strip()
    if len(command_text.split()) < 2:
        await message.reply("Please provide a message to send. Usage: /chatroom <message>")
        return
    messages = [msg.strip() for msg in " ".join(command_text.split()[1:]).split(",") if msg.strip()]
    state = user_states[user_id]
    state['pending_chatroom_message'] = messages
    msg_lines = "\n".join(f"<b>Message:</b> {html.escape(m)}" for m in messages)
    markup = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Current", callback_data="chatroom_current"),
            InlineKeyboardButton(text="All", callback_data="chatroom_all")
        ],
        [InlineKeyboardButton(text="Cancel", callback_data="chatroom_cancel")]
    ])
    await message.reply(
        f"How would you like to send your chatroom message?\n\n{msg_lines}",
        reply_markup=markup,
        parse_mode="HTML"
    )

# --- Callback Handler for chatroom actions (Unchanged) ---

async def handle_chatroom_callback(
    callback_query, state, bot, user_id,
    get_current_account, get_tokens, send_message_to_everyone_fn
):
    data = callback_query.data
    if data == "chatroom_current":
        await callback_query.answer("Processing...")
        token = get_current_account(user_id)
        tokens = get_tokens(user_id)
        account_name = next((t["name"] for t in tokens if t["token"] == token), "Unknown")
        status_message = await callback_query.message.edit_text(
            f"<b>Account:</b> {html.escape(account_name)}\nMessages sending...", parse_mode="HTML"
        )
        messages = state.get('pending_chatroom_message', [])
        sent_count = await send_message_to_everyone_fn(token, messages, status_message=status_message, bot=bot, chat_id=user_id)
        # The final message is now handled inside send_message_to_everyone_fn
        state.pop('pending_chatroom_message', None)
        return True

    elif data == "chatroom_all":
        await callback_query.answer()
        tokens = get_tokens(user_id)
        acc_line = "<b>Accounts:</b> " + ", ".join(html.escape(t["name"]) for t in tokens)
        confirm_markup = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Confirm", callback_data="chatroom_confirm")],
            [InlineKeyboardButton(text="Cancel", callback_data="chatroom_cancel")]
        ])
        await callback_query.message.edit_text(
            f"You chose to send the chatroom message to:\n{acc_line}\n\nPress Confirm to proceed.",
            reply_markup=confirm_markup,
            parse_mode="HTML"
        )
        state['chatroom_send_type'] = "chatroom_all"
        return True

    elif data == "chatroom_confirm":
        await callback_query.answer("Processing...")
        messages = state.get('pending_chatroom_message', [])
        status_message = await callback_query.message.edit_text("Sending messages to all accounts, please wait...")
        tokens = get_tokens(user_id)
        if not tokens:
            await status_message.edit_text("No accounts found.")
            return True
        per_account_counts, total_sent = [], 0
        for idx, token_info in enumerate(tokens):
            account_name = token_info.get('name', f"Account {idx+1}")
            await status_message.edit_text(
                f"<b>Processing Account:</b> {html.escape(account_name)} ({idx+1}/{len(tokens)})...", parse_mode="HTML"
            )
            sent_count = await send_message_to_everyone_fn(
                token_info["token"], messages, status_message=status_message, bot=bot, chat_id=user_id
            )
            per_account_counts.append(sent_count)
            total_sent += sent_count
        
        summary_lines = [f"✅ <b>All Accounts Processed</b>", f"Total Messages Sent: {total_sent}\n"]
        for i, token_info in enumerate(tokens):
            acc_name = html.escape(token_info.get('name', f"Acc {i+1}"))
            summary_lines.append(f"• {acc_name}: {per_account_counts[i]} sent")

        await status_message.edit_text("\n".join(summary_lines), parse_mode="HTML")
        state.pop('chatroom_send_type', None)
        state.pop('pending_chatroom_message', None)
        return True

    elif data == "chatroom_cancel":
        await callback_query.answer("Cancelled.")
        state.pop('chatroom_send_type', None)
        state.pop('pending_chatroom_message', None)
        await callback_query.message.edit_text("Chatroom message sending cancelled.")
        return True

    return False
