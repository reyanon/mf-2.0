
import asyncio
import aiohttp
import logging
import html
import json
from collections import defaultdict
from aiogram import Bot, Dispatcher, Router, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, BotCommand
from aiogram.filters import Command
from aiogram.types.callback_query import CallbackQuery
from datetime import datetime, timedelta
from aiogram.exceptions import TelegramBadRequest
from db import set_token, get_tokens, set_current_account, get_current_account, delete_token, set_user_filters, get_user_filters
from lounge import send_lounge
from chatroom import send_message_to_everyone
from unsubscribe import unsubscribe_everyone
from filters import filter_command, set_filter, get_filter_keyboard
from allcountry import run_all_countries
from chatroom import send_message_to_everyone_all_tokens
from lounge import send_lounge_all_tokens
from db import (
    set_spam_filter,
    get_spam_filter,
    is_already_sent,
    add_sent_id,
    toggle_token_status,
    get_active_tokens,
    get_token_status
)
from pymongo import MongoClient
import re
from db import db
import random
from friend_requests import (
    run_requests, 
    process_all_tokens, 
    user_states,
    stop_markup
)

# Tokens
API_TOKEN = "7916536914:AAHwtvO8hfGl2U4xcfM1fAjMLNypPFEW5JQ"

# Admin user IDs
ADMIN_USER_IDS = [7405203657, 7996471035, 8060390897]  # Replace with actual admin user IDs

# Password access dictionary
password_access = {}

# Password for temporary access
TEMP_PASSWORD = "11223344"  # Replace with your chosen password

TARGET_CHANNEL_ID = -1002610862940  # Your target group's chat ID

# Add these global variables
CURRENT_CONNECTED_COLLECTION = {}  # Store current connected DB for each admin: {admin_id: db_name}

# Initialize logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Initialize bot, router and dispatcher
bot = Bot(token=API_TOKEN)
router = Router()
dp = Dispatcher()

# Inline keyboards with simplified organization
start_markup = InlineKeyboardMarkup(inline_keyboard=[
    [
        InlineKeyboardButton(text="Send Request", callback_data="send_request_menu"),
        InlineKeyboardButton(text="Send Request All", callback_data="send_request_all_menu")
    ]
])

# Send Request submenu
send_request_markup = InlineKeyboardMarkup(inline_keyboard=[
    [
        InlineKeyboardButton(text="Start Request", callback_data="start"),
        InlineKeyboardButton(text="All Countries", callback_data="all_countries")
    ],
    [InlineKeyboardButton(text="Back", callback_data="back_to_menu")]
])

# Send Request All submenu with confirmation
send_request_all_markup = InlineKeyboardMarkup(inline_keyboard=[
    [
        InlineKeyboardButton(text="Confirm", callback_data="start_all"),
        InlineKeyboardButton(text="Cancel", callback_data="back_to_menu")
    ]
])

# Back button markup
back_markup = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="Back", callback_data="back_to_menu")]
])

# Stop markup
stop_markup = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="Stop", callback_data="stop")]
])

def is_admin(user_id):
    return user_id in ADMIN_USER_IDS

def has_valid_access(user_id):
    if is_admin(user_id):
        return True
    if user_id in password_access and password_access[user_id] > datetime.now():
        return True
    return False

def get_db_for_admin(user_id):
    """Get the currently connected database for an admin or the default one"""
    if user_id in ADMIN_USER_IDS and user_id in CURRENT_CONNECTED_COLLECTION:
        db_name = CURRENT_CONNECTED_COLLECTION[user_id]
        return client[db_name]
    return db  # Return the default db

def get_settings_menu(user_id):
    """Generate the settings menu markup"""
    if user_id not in user_states:
        user_states[user_id] = {}
    
    spam_on = get_spam_filter(user_id)
    buttons = [
        [
            InlineKeyboardButton(text="Filter", callback_data="show_filters"),
            InlineKeyboardButton(
                text=f"Spam Filter: {'ON ✅' if spam_on else 'OFF ❌'}",
                callback_data="toggle_spam_filter"
            )
        ],
        [
            InlineKeyboardButton(text="Manage Accounts", callback_data="manage_accounts"),
            InlineKeyboardButton(text="Back", callback_data="back_to_menu")
        ]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

@router.message(Command("password"))
async def password_command(message: types.Message):
    user_id = message.chat.id
    command_text = message.text.strip()
    if len(command_text.split()) < 2:
        await message.reply("Please provide the password. Usage: /password <password>")
        return

    provided_password = command_text.split()[1]
    if provided_password == TEMP_PASSWORD:
        password_access[user_id] = datetime.now() + timedelta(hours=1)
        await message.reply("Access granted for one hour.")
        await bot.delete_message(chat_id=message.chat.id, message_id=message.message_id)
    else:
        await message.reply("Incorrect password.")

@router.message(Command("start"))
async def start_command(message: types.Message):
    user_id = message.chat.id
    if not has_valid_access(user_id):
        await message.reply("You are not authorized to use this bot.")
        return
    state = user_states[user_id]
    status = await message.answer("Welcome! Use the button below to start requests.", reply_markup=start_markup)
    state["status_message_id"] = status.message_id
    state["pinned_message_id"] = None

@router.message(Command("send_lounge_all"))
async def send_lounge_all(message: types.Message):
    user_id = message.chat.id

    if not has_valid_access(user_id):
        return await message.reply("🚫 You are not authorized to use this bot.")

    parts = message.text.split(maxsplit=1)
    if len(parts) != 2:
        return await message.reply("ℹ️ Usage: /send_lounge_all <message>")

    custom_message = parts[1]
    active_tokens_data = get_active_tokens(user_id)

    if not active_tokens_data:
        return await message.reply("🔍 No active tokens found.")
        
    spam_enabled = get_spam_filter(user_id)
    status = await message.reply(
        f"⏳ Starting lounge messages for {len(active_tokens_data)} active tokens...\n"
        f"📝 Message: {custom_message[:50]}...\n"
        f"🛡️ Spam filter: {'ON' if spam_enabled else 'OFF'}"
    )

    try:
        await send_lounge_all_tokens(
            active_tokens_data, 
            custom_message, 
            status, 
            bot, 
            message.chat.id, 
            spam_enabled
        )
    except Exception as e:
        await status.edit_text(f"❌ Error sending lounge messages: {str(e)}")
        logging.error(f"Error in /send_lounge_all command: {str(e)}")

@router.message(Command("lounge"))
async def lounge_command(message: types.Message):
    user_id = message.chat.id

    if not has_valid_access(user_id):
        await message.reply("🚫 You are not authorized to use this bot.")
        return

    token = get_current_account(user_id)
    if not token:
        await message.reply("🔍 No active account found. Please set an account before sending messages.")
        return

    command_text = message.text.strip()
    if len(command_text.split()) < 2:
        await message.reply("ℹ️ Usage: /lounge <message>")
        return

    custom_message = " ".join(command_text.split()[1:])
    spam_enabled = get_spam_filter(user_id)
    
    status_message = await message.reply(
        f"⏳ Starting lounge messaging...\n"
        f"📝 Message: {custom_message[:50]}...\n"
        f"🛡️ Spam filter: {'ON' if spam_enabled else 'OFF'}"
    )

    try:
        await send_lounge(
            token, 
            custom_message, 
            status_message, 
            bot, 
            user_id, 
            spam_enabled
        )
    except Exception as e:
        await status_message.edit_text(f"❌ Error sending lounge messages: {str(e)}")
        logging.error(f"Error in /lounge command: {str(e)}")

@router.message(Command("chatroom"))
async def send_to_all_command(message: types.Message):
    """
    Command: /chatroom <message>
    Sends message to all chatrooms for the current active token
    """
    user_id = message.chat.id

    if not has_valid_access(user_id):
        await message.reply("🚫 You are not authorized to use this bot.")
        return

    token = get_current_account(user_id)
    if not token:
        await message.reply("🔍 No active account found. Please set an account before sending messages.")
        return

    command_text = message.text.strip()
    if len(command_text.split()) < 2:
        await message.reply("ℹ️ Usage: /chatroom <message>")
        return

    custom_message = " ".join(command_text.split()[1:])
    spam_enabled = get_spam_filter(user_id)
    
    status_message = await message.reply(
        f"⏳ Starting to send message to all chatrooms...\n"
        f"📝 Message: {custom_message[:50]}...\n"
        f"🛡️ Spam filter: {'ON' if spam_enabled else 'OFF'}"
    )

    try:
        total_chatrooms, sent_count = await send_message_to_everyone(
            token, 
            custom_message, 
            status_message=status_message, 
            bot=bot, 
            chat_id=user_id, 
            spam_enabled=spam_enabled
        )

        await status_message.edit_text(
            f"✅ Finished sending messages\n"
            f"📊 Total chatrooms: {total_chatrooms}\n"
            f"✉️ Messages sent: {sent_count}\n"
            f"🛡️ Spam filter prevented: {total_chatrooms - sent_count}"
        )
    except Exception as e:
        await status_message.edit_text(f"❌ Error sending messages: {str(e)}")
        logging.error(f"Error in /chatroom command: {str(e)}")

@router.message(Command("send_chat_all"))
async def send_chat_all(message: types.Message):
    """
    Command: /send_chat_all <message>
    Sends message to all chatrooms for ALL active tokens
    """
    user_id = message.chat.id

    if not has_valid_access(user_id):
        await message.reply("🚫 You are not authorized to use this bot.")
        return

    parts = message.text.split(maxsplit=1)
    if len(parts) != 2:
        await message.reply("ℹ️ Usage: /send_chat_all <message>")
        return

    custom_message = parts[1]
    active_tokens = get_active_tokens(user_id)
    tokens = [t["token"] for t in active_tokens]
    
    if not tokens:
        await message.reply("🔍 No active tokens found.")
        return
        
    spam_enabled = get_spam_filter(user_id)

    status = await message.reply(
        f"⏳ Starting chatroom messages to {len(tokens)} active tokens...\n"
        f"📝 Message: {custom_message[:50]}...\n"
        f"🛡️ Spam filter: {'ON' if spam_enabled else 'OFF'}"
    )

    try:
        await send_message_to_everyone_all_tokens(
            tokens, 
            custom_message, 
            status, 
            bot, 
            message.chat.id, 
            spam_enabled=spam_enabled
        )
    except Exception as e:
        await status.edit_text(f"❌ Error sending messages: {str(e)}")
        logging.error(f"Error in /send_chat_all command: {str(e)}")

@router.message(Command("skip"))
async def unsubscribe_all_command(message: types.Message):
    user_id = message.chat.id
    if not has_valid_access(user_id):
        await message.reply("You are not authorized to use this bot.")
        return
    token = get_current_account(user_id)
    if not token:
        await message.reply("No active account found. Please set an account before unsubscribing.")
        return

    status_message = await message.reply("Fetching chatrooms and unsubscribing...")
    await unsubscribe_everyone(token, status_message=status_message, bot=bot, chat_id=user_id)
    await status_message.edit_text("Unsubscribed from all chatrooms.")

@router.message(Command("invoke"))
async def invoke_command(message: types.Message):
    user_id = message.chat.id
    if not has_valid_access(user_id):
        await message.reply("You are not authorized to use this bot.")
        return

    tokens = get_tokens(user_id)
    if not tokens:
        await message.reply("No tokens found.")
        return

    disabled_accounts = []
    working_accounts = []
    url = "https://api.meeff.com/facetalk/vibemeet/history/count/v1"
    params = {'locale': "en"}

    async with aiohttp.ClientSession() as session:
        for token_obj in tokens:
            token = token_obj["token"]
            headers = {
                'User-Agent': "okhttp/5.0.0-alpha.14",
                'Accept-Encoding': "gzip",
                'meeff-access-token': token
            }
            try:
                async with session.get(url, params=params, headers=headers) as resp:
                    result = await resp.json(content_type=None)
                    if "errorCode" in result and result["errorCode"] == "AuthRequired":
                        disabled_accounts.append(token_obj)
                    else:
                        working_accounts.append(token_obj)
            except Exception as e:
                logging.error(f"Error checking token {token_obj.get('name')}: {e}")
                disabled_accounts.append(token_obj)

    if disabled_accounts:
        for token_obj in disabled_accounts:
            delete_token(user_id, token_obj["token"])
            await message.reply(f"Deleted disabled token for account: {token_obj['name']}")
    else:
        await message.reply("All accounts are working.")

@router.message(Command("aio"))
async def aio_command(message: types.Message):
    if not has_valid_access(message.chat.id):
        await message.reply("You are not authorized to use this bot.")
        return
    await message.answer("Choose an action:", reply_markup=aio_markup)

@router.message(Command("settings"))
async def settings_command(message: types.Message):
    user_id = message.chat.id
    if not has_valid_access(user_id):
        await message.reply("You are not authorized to use this bot.")
        return
    
    await message.reply("Settings menu:", reply_markup=get_settings_menu(user_id))

@router.message(Command("db_connect"))
async def db_connect_command(message: types.Message):
    """Connect to a specific user's collection (Admin only)"""
    user_id = message.from_user.id
    
    if user_id not in ADMIN_USER_IDS:
        return
    
    command_parts = message.text.strip().split()
    if len(command_parts) != 2:
        await message.reply("Usage: /db_connect <user_id>")
        return
    
    try:
        target_user_id = int(command_parts[1])
        collection_name = f"user_{target_user_id}"
        
        if collection_name not in db.list_collection_names():
            await message.reply(f"Collection '{collection_name}' doesn't exist in the database.")
            return
            
        CURRENT_CONNECTED_COLLECTION[user_id] = collection_name
        
        await message.reply(f"Connected to collection: `{collection_name}`")
    except ValueError:
        await message.reply("Invalid user ID. Please provide a numeric user ID.")

@router.message(Command("db_disconnect"))
async def db_disconnect_command(message: types.Message):
    """Disconnect from custom collection and return to default (Admin only)"""
    user_id = message.from_user.id
    
    if user_id not in ADMIN_USER_IDS:
        return
    
    if user_id in CURRENT_CONNECTED_COLLECTION:
        prev_collection = CURRENT_CONNECTED_COLLECTION[user_id]
        del CURRENT_CONNECTED_COLLECTION[user_id]
        await message.reply(f"Disconnected from `{prev_collection}`. Now using your own collection.")
    else:
        await message.reply("You are already using your own collection.")

@router.message(Command("db_list"))
async def db_list_command(message: types.Message):
    """List all available user collections (Admin only)"""
    user_id = message.from_user.id
    
    if user_id not in ADMIN_USER_IDS:
        return
    
    collection_names = db.list_collection_names()
    user_collections = [name for name in collection_names if name.startswith("user_")]
    current_collection = CURRENT_CONNECTED_COLLECTION.get(user_id, f"user_{user_id}")
    response = "Available user collections:\n\n"
    
    if user_collections:
        for coll_name in user_collections:
            marker = "→ " if coll_name == current_collection else "  "
            response += f"{marker}`{coll_name}`\n"
    else:
        response += "No user collections found."
    
    response += f"\nCurrently connected to: `{current_collection}`"
    await message.reply(response)

@router.message()
async def handle_new_token(message: types.Message):
    if message.text and message.text.startswith("/"):
        return
    user_id = message.from_user.id

    if message.from_user.is_bot:
        return

    if not has_valid_access(user_id):
        await message.reply("You are not authorized to use this bot.")
        return

    if message.text:
        token_data = message.text.strip().split(" ")
        token = token_data[0]
        if len(token) < 10:
            await message.reply("Invalid token. Please try again.")
            return

        url = "https://api.meeff.com/facetalk/vibemeet/history/count/v1"
        params = {'locale': "en"}
        headers = {
            'User-Agent': "okhttp/5.0.0-alpha.14",
            'Accept-Encoding': "gzip",
            'meeff-access-token': token
        }
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url, params=params, headers=headers) as resp:
                    result = await resp.json(content_type=None)
                    if "errorCode" in result and result["errorCode"] == "AuthRequired":
                        await message.reply("The token you provided is invalid or disabled. Please try a different token.")
                        return
            except Exception as e:
                logging.error(f"Error verifying token: {e}")
                await message.reply("Error verifying the token. Please try again.")
                return

        tokens = get_tokens(user_id)
        account_name = " ".join(token_data[1:]) if len(token_data) > 1 else f"Account {len(tokens) + 1}"
        set_token(user_id, token, account_name)
        await message.reply(f"Your access token has been verified and saved as {account_name}. Use the menu to manage accounts.")
    else:
        await message.reply("Message text is empty. Please provide a valid token.")

@router.callback_query()
async def callback_handler(callback_query: CallbackQuery):
    user_id = callback_query.from_user.id
    data = callback_query.data

    if not has_valid_access(user_id):
        await callback_query.answer("You are not authorized to use this bot.")
        return

    if user_id not in user_states:
        user_states[user_id] = {}
    state = user_states[user_id]

    if data == "send_request_menu":
        await callback_query.message.edit_text(
            "Send Request options:",
            reply_markup=send_request_markup
        )
        return
    
    elif data == "send_request_all_menu":
        await callback_query.message.edit_text(
            "Are you sure you want to send request to all active accounts?",
            reply_markup=send_request_all_markup
        )
        return

    elif data == "show_filters":
        await callback_query.message.edit_text(
            "Set your filter preferences:",
            reply_markup=get_filter_keyboard()
        )
        return

    elif data in ["filter_gender", "filter_age", "filter_nationality", "filter_back"] or \
         data.startswith("filter_gender_") or data.startswith("filter_age_") or \
         data.startswith("filter_nationality_"):
        await set_filter(callback_query)
        return

    elif data == "manage_accounts":
        tokens = get_tokens(user_id)
        current_token = get_current_account(user_id)

        if not tokens:
            await callback_query.message.edit_text(
                "No accounts saved. Send a new token to add an account.",
                reply_markup=back_markup
            )
            return

        buttons = []
        for i, tok in enumerate(tokens):
            is_active = tok.get("active", True)
            status_emoji = "✅ Active" if is_active else "❌ Inactive"
            is_current = tok['token'] == current_token
            
            buttons.append([
                InlineKeyboardButton(
                    text=f"{tok['name']} {'(Current)' if is_current else ''}",
                    callback_data=f"set_account_{i}"
                ),
                InlineKeyboardButton(
                    text=status_emoji,
                    callback_data=f"toggle_status_{i}"
                ),
                InlineKeyboardButton(
                    text="Delete",
                    callback_data=f"confirm_delete_{i}"
                )
            ])

        buttons.append([
            InlineKeyboardButton(text="Back", callback_data="settings_menu")
        ])

        await callback_query.message.edit_text(
            "Manage your accounts:\n(Active accounts are available for multi-token functions)",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
        )
        return
    
    elif data.startswith("confirm_delete_"):
        idx = int(data.split("_")[-1])
        tokens = get_tokens(user_id)
        if 0 <= idx < len(tokens):
            account_name = tokens[idx]["name"]
            buttons = [
                [
                    InlineKeyboardButton(text="Yes, Delete", callback_data=f"delete_account_{i}"),
                    InlineKeyboardButton(text="Cancel", callback_data="manage_accounts")
                ]
            ]
            await callback_query.message.edit_text(
                f"Are you sure you want to delete account '{account_name}'?",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
            )
        else:
            await callback_query.answer("Invalid account selected.")
        return
        
    elif data.startswith("toggle_status_"):
        idx = int(data.split("_")[-1])
        tokens = get_tokens(user_id)
        if 0 <= idx < len(tokens):
            token = tokens[idx]["token"]
            toggle_token_status(user_id, token)
            await callback_query.answer(f"Status toggled for {tokens[idx]['name']}")
            
            tokens = get_tokens(user_id)
            current_token = get_current_account(user_id)
            buttons = []
            for i, tok in enumerate(tokens):
                is_active = tok.get("active", True)
                status_emoji = "✅ Active" if is_active else "❌ Inactive"
                is_current = tok['token'] == current_token
                buttons.append([
                    InlineKeyboardButton(
                        text=f"{tok['name']} {'(Current)' if is_current else ''}",
                        callback_data=f"set_account_{i}"
                    ),
                    InlineKeyboardButton(
                        text=status_emoji,
                        callback_data=f"toggle_status_{i}"
                    ),
                    InlineKeyboardButton(
                        text="Delete",
                        callback_data=f"confirm_delete_{i}"
                    )
                ])
            buttons.append([InlineKeyboardButton(text="Back", callback_data="settings_menu")])
            await callback_query.message.edit_text(
                "Manage your accounts:\n(Active accounts are available for multi-token functions)",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
            )
        else:
            await callback_query.answer("Invalid account selected.")
        return

    elif data == "toggle_spam_filter":
        new_state = not get_spam_filter(user_id)
        set_spam_filter(user_id, new_state)
        await callback_query.answer(
            f"Spam Filter {'Enabled ✅' if new_state else 'Disabled ❌'}"
        )
        await callback_query.message.edit_text(
            "Spam filter updated. Returning to settings menu...",
            reply_markup=get_settings_menu(user_id)
        )
        return

    elif data.startswith("set_account_"):
        idx = int(data.split("_")[-1])
        tokens = get_tokens(user_id)
        if 0 <= idx < len(tokens):
            set_current_account(user_id, tokens[idx]["token"])
            await callback_query.message.edit_text("Account set as active. You can now start requests.", reply_markup=back_markup)
        else:
            await callback_query.answer("Invalid account selected.")
        return

    elif data.startswith("delete_account_"):
        idx = int(data.split("_")[-1])
        tokens = get_tokens(user_id)
        if 0 <= idx < len(tokens):
            delete_token(user_id, tokens[idx]["token"])
            await callback_query.message.edit_text("Account has been deleted.", reply_markup=back_markup)
        else:
            await callback_query.answer("Invalid account selected.")
        return

    elif data == "back_to_menu":
        await callback_query.message.edit_text("Welcome! Choose an option below:", reply_markup=start_markup)
        return
        
    elif data == "settings_menu":
        await callback_query.message.edit_text(
            "Settings menu:",
            reply_markup=get_settings_menu(user_id)
        )
        return

    elif data == "start":
        if state.get("running", False):
            await callback_query.answer("Requests are already running!")
        else:
            state["running"] = True
            state["total_added_friends"] = 0
            try:
                status_message = await callback_query.message.edit_text("Initializing requests...", reply_markup=stop_markup)
                state["status_message_id"] = status_message.message_id
                state["pinned_message_id"] = status_message.message_id
                
                await bot.pin_chat_message(chat_id=user_id, message_id=state["status_message_id"])
                
                asyncio.create_task(run_requests(user_id, bot, TARGET_CHANNEL_ID))
                await callback_query.answer("Requests started!")
            except Exception as e:
                logging.error(f"Error while starting requests: {e}")
                await callback_query.message.edit_text("Failed to start requests. Please try again later.", reply_markup=start_markup)
                state["running"] = False

    elif data == "start_all":
        if state.get("running", False):
            await callback_query.answer("Another request is already running!")
        else:
            tokens = get_active_tokens(user_id)
            if not tokens:
                await callback_query.answer("No active tokens found.")
                return
        
            state["running"] = True
            state["total_added_friends"] = 0
        
            try:
                msg = await callback_query.message.edit_text(
                    "🔄 Starting requests for all active tokens...", 
                    reply_markup=stop_markup
                )
                state["status_message_id"] = msg.message_id
                state["pinned_message_id"] = msg.message_id
                
                await bot.pin_chat_message(chat_id=user_id, message_id=msg.message_id)
                
                asyncio.create_task(process_all_tokens(user_id, tokens, bot, TARGET_CHANNEL_ID))
                await callback_query.answer("Processing all tokens started!")
            except Exception as e:
                logging.error(f"Error starting all tokens: {e}")
                await callback_query.message.edit_text(
                    "Failed to start processing all tokens. Please try again later.", 
                    reply_markup=start_markup
                )
                state["running"] = False

    elif data == "stop":
        if not state.get("running", False):
            await callback_query.answer("Requests are not running!")
        else:
            state["running"] = False
            message_text = (
                f"Requests stopped. Use the button below to start again.\n"
                f"Total Added Friends: {state.get('total_added_friends', 0)}"
            )
            await callback_query.message.edit_text(message_text, reply_markup=start_markup)
            await callback_query.answer("Requests stopped.")
            if state.get("pinned_message_id"):
                await bot.unpin_chat_message(chat_id=user_id, message_id=state["pinned_message_id"])
                state["pinned_message_id"] = None

    elif data == "all_countries":
        if state.get("running", False):
            await callback_query.answer("Another process is already running!")
        else:
            state["running"] = True
            try:
                status_message = await callback_query.message.edit_text(
                    "Starting All Countries feature...",
                    reply_markup=stop_markup
                )
                state["status_message_id"] = status_message.message_id
                state["pinned_message_id"] = status_message.message_id
                state["stop_markup"] = stop_markup
                await bot.pin_chat_message(chat_id=user_id, message_id=status_message.message_id)
                asyncio.create_task(run_all_countries(user_id, state, bot, get_current_account))
                await callback_query.answer("All Countries feature started!")
            except Exception as e:
                logging.error(f"Error while starting All Countries feature: {e}")
                await callback_query.message.edit_text("Failed to start All Countries feature.", reply_markup=start_markup)
                state["running"] = False

async def set_bot_commands():
    commands = [
        BotCommand(command="start", description="Start the bot"),
        BotCommand(command="lounge", description="Send message in the lounge"),
        BotCommand(command="chatroom", description="Send message in Chatroom"),
        BotCommand(command="send_lounge_all", description="Send lounge message to ALL ID"),
        BotCommand(command="send_chat_all", description="Send chatroom message to ALL ID"),
        BotCommand(command="invoke", description="Verify and remove disabled accounts"),
        BotCommand(command="settings", description="Access bot settings and account management"),
     #  BotCommand(command="skip", description="Skip everyone in the chatroom"),
     #   BotCommand(command="password", description="Enter password for temporary access")
    ]
    await bot.set_my_commands(commands)

async def main():
    await set_bot_commands()
    dp.include_router(router)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
