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
from db import (
    set_token, get_tokens, set_current_account, get_current_account, delete_token, 
    set_user_filters, get_user_filters, set_spam_filter, get_spam_filter, 
    is_already_sent, add_sent_id, toggle_token_status, get_active_tokens, 
    get_token_status, set_account_active, get_info_card,
    # New DB management functions
    list_all_collections, get_collection_summary, connect_to_collection,
    rename_user_collection, transfer_to_user, get_current_collection_info
)
from lounge import send_lounge
from chatroom import send_message_to_everyone
from unsubscribe import unsubscribe_everyone
from filters import filter_command, set_filter, get_filter_keyboard
from allcountry import run_all_countries
from chatroom import send_message_to_everyone_all_tokens
from lounge import send_lounge_all_tokens
from signup import signup_command, signup_callback_handler, signup_message_handler
from friend_requests import (
    run_requests, 
    process_all_tokens, 
    user_states,
    stop_markup
)

# Tokens
API_TOKEN = "7916536914:AAHwtvO8hfGl2U4xcfM1fAjMLNypPFEW5JQ"

# Admin user IDs
ADMIN_USER_IDS = [7405203657, 8060390897, 8112528756, 7691399254]  # Replace with actual admin user IDs

# Password access dictionary
password_access = {}

# Password for temporary access
TEMP_PASSWORD = "11223344"

TARGET_CHANNEL_ID = -1002610862940

# DB operation states
db_operation_states = {}

# Initialize logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Initialize bot, router and dispatcher
bot = Bot(token=API_TOKEN)
router = Router()
dp = Dispatcher()

def is_admin(user_id):
    return user_id in ADMIN_USER_IDS

def has_valid_access(user_id):
    if is_admin(user_id):
        return True
    if user_id in password_access and password_access[user_id] > datetime.now():
        return True
    return False

def get_settings_menu(user_id):
    """Generate the enhanced settings menu markup"""
    if user_id not in user_states:
        user_states[user_id] = {}
    
    spam_on = get_spam_filter(user_id)
    
    buttons = [
        [
            InlineKeyboardButton(text="👤 Manage Accounts", callback_data="manage_accounts"),
            InlineKeyboardButton(text="🎯 Filters", callback_data="show_filters")
        ],
        [
            InlineKeyboardButton(
                text=f"🛡️ Spam Filter: {'ON ✅' if spam_on else 'OFF ❌'}",
                callback_data="toggle_spam_filter"
            )
        ],
        [
            InlineKeyboardButton(text="🗄️ DB Settings", callback_data="db_settings"),
         #   InlineKeyboardButton(text="🆕 Sign Up", callback_data="signup_go")
        ],
        [
         #   InlineKeyboardButton(text="🔐 Sign In", callback_data="signin_go"),
            InlineKeyboardButton(text="🔙 Back", callback_data="back_to_menu")
        ]
    ]
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_db_settings_menu():
    """Get DB settings menu"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🔗 Connect DB", callback_data="db_connect"),
            InlineKeyboardButton(text="📝 Rename DB", callback_data="db_rename")
        ],
        [
            InlineKeyboardButton(text="👁️ View DB", callback_data="db_view"),
            InlineKeyboardButton(text="📤 Transfer DB", callback_data="db_transfer")
        ],
        [InlineKeyboardButton(text="🔙 Back", callback_data="settings_menu")]
    ])

def get_unsubscribe_menu():
    """Get unsubscribe options menu"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Unsubscribe Current", callback_data="unsub_current"),
            InlineKeyboardButton(text="Unsubscribe All", callback_data="unsub_all")
        ],
        [InlineKeyboardButton(text="🔙 Back", callback_data="back_to_menu")]
    ])

def get_confirmation_menu(action_type):
    """Get confirmation menu for actions"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Yes", callback_data=f"confirm_{action_type}"),
            InlineKeyboardButton(text="❌ Cancel", callback_data="back_to_menu")
        ]
    ])

# Enhanced mobile-friendly keyboards
start_markup = InlineKeyboardMarkup(inline_keyboard=[
    [
        InlineKeyboardButton(text="🚀 Send Request", callback_data="send_request_menu"),
        InlineKeyboardButton(text="🌍 All Countries", callback_data="all_countries")
    ]
])

send_request_markup = InlineKeyboardMarkup(inline_keyboard=[
    [
        InlineKeyboardButton(text="▶ Start Request", callback_data="start"),
        InlineKeyboardButton(text="▶ Request All", callback_data="start_all")
    ],
    [InlineKeyboardButton(text="🔙 Back", callback_data="back_to_menu")]
])

back_markup = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="🔙 Back", callback_data="back_to_menu")]
])

stop_markup = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="⏹️ Stop", callback_data="stop")]
])

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
        await message.reply("🔐 Access granted for one hour.")
        await bot.delete_message(chat_id=message.chat.id, message_id=message.message_id)
    else:
        await message.reply("❌ Incorrect password.")

@router.message(Command("start"))
async def start_command(message: types.Message):
    user_id = message.chat.id
    
    if not has_valid_access(user_id):
        await message.reply("🚫 You are not authorized to use this bot. Use /password to get access.")
        return
    
    state = user_states[user_id]
    welcome_text = "🎯 <b>Meeff Bot Dashboard</b>\n\nChoose an option below to get started:"
    
    status = await message.answer(
        welcome_text,
        reply_markup=start_markup,
        parse_mode="HTML"
    )
    state["status_message_id"] = status.message_id
    state["pinned_message_id"] = None

@router.message(Command("signup"))
async def signup_cmd(message: types.Message):
    if not has_valid_access(message.chat.id):
        await message.reply("🚫 You are not authorized to use this bot.")
        return
    await signup_command(message)

@router.message(Command("signin"))
async def signin_cmd(message: types.Message):
    if not has_valid_access(message.chat.id):
        await message.reply("🚫 You are not authorized to use this bot.")
        return
    # Trigger signin flow
    user_id = message.from_user.id
    from signup import user_signup_states, BACK_TO_SIGNUP
    user_signup_states[user_id] = {"stage": "signin_email"}
    await message.answer(
        "🔐 <b>Sign In</b>\n\n"
        "Please enter your email address:",
        reply_markup=BACK_TO_SIGNUP,
        parse_mode="HTML"
    )

@router.message(Command("skip"))
async def skip_command(message: types.Message):
    user_id = message.chat.id
    if not has_valid_access(user_id):
        await message.reply("🚫 You are not authorized to use this bot.")
        return
    
    await message.answer(
        "⏭️ <b>Unsubscribe Options</b>\n\n"
        "Choose which accounts to unsubscribe from chatrooms:",
        reply_markup=get_unsubscribe_menu(),
        parse_mode="HTML"
    )

@router.message(Command("send_lounge_all"))
async def send_lounge_all(message: types.Message):
    user_id = message.chat.id

    if not has_valid_access(user_id):
        return await message.reply("🚫 You are not authorized to use this bot.")

    parts = message.text.split(maxsplit=1)
    if len(parts) != 2:
        return await message.reply(
            "ℹ️ <b>Usage</b>\n\n"
            "<code>/send_lounge_all <message></code>",
            parse_mode="HTML"
        )

    custom_message = parts[1]
    active_tokens_data = get_active_tokens(user_id)

    if not active_tokens_data:
        return await message.reply("🔍 No active tokens found.")
        
    spam_enabled = get_spam_filter(user_id)
    status = await message.reply(
        f"⏳ <b>Starting Lounge Messages</b>\n\n"
        f"📊 Active tokens: {len(active_tokens_data)}\n"
        f"📝 Message: <code>{custom_message[:50]}...</code>\n"
        f"🛡️ Spam filter: {'ON' if spam_enabled else 'OFF'}",
        parse_mode="HTML"
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
        await message.reply(
            "ℹ️ <b>Usage</b>\n\n"
            "<code>/lounge <message></code>",
            parse_mode="HTML"
        )
        return

    custom_message = " ".join(command_text.split()[1:])
    spam_enabled = get_spam_filter(user_id)
    
    status_message = await message.reply(
        f"⏳ <b>Starting Lounge Messaging</b>\n\n"
        f"📝 Message: <code>{custom_message[:50]}...</code>\n"
        f"🛡️ Spam filter: {'ON' if spam_enabled else 'OFF'}",
        parse_mode="HTML"
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
    """Enhanced chatroom command with better mobile UI"""
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
        await message.reply(
            "ℹ️ <b>Usage</b>\n\n"
            "<code>/chatroom <message></code>",
            parse_mode="HTML"
        )
        return

    custom_message = " ".join(command_text.split()[1:])
    spam_enabled = get_spam_filter(user_id)
    
    status_message = await message.reply(
        f"⏳ <b>Starting Chatroom Messages</b>\n\n"
        f"📝 Message: <code>{custom_message[:50]}...</code>\n"
        f"🛡️ Spam filter: {'ON' if spam_enabled else 'OFF'}\n\n"
        f"🔄 Initializing...",
        parse_mode="HTML"
    )

    try:
        total_chatrooms, sent_count, filtered_count = await send_message_to_everyone(
            token, 
            custom_message, 
            status_message=status_message, 
            bot=bot, 
            chat_id=user_id, 
            spam_enabled=spam_enabled
        )

        await status_message.edit_text(
            f"✅ <b>Chatroom Messages Complete</b>\n\n"
            f"📊 <b>Results:</b>\n"
            f"• Total chatrooms: <code>{total_chatrooms}</code>\n"
            f"• Messages sent: <code>{sent_count}</code>\n"
            f"• Filtered (duplicates): <code>{filtered_count}</code>\n\n"
            f"🛡️ Spam filter: {'ON' if spam_enabled else 'OFF'}",
            parse_mode="HTML"
        )
    except Exception as e:
        await status_message.edit_text(
            f"❌ <b>Error</b>\n\n"
            f"Failed to send messages: {str(e)[:200]}",
            parse_mode="HTML"
        )
        logging.error(f"Error in /chatroom command: {str(e)}")

@router.message(Command("send_chat_all"))
async def send_chat_all(message: types.Message):
    """Enhanced send_chat_all command with better mobile UI"""
    user_id = message.chat.id

    if not has_valid_access(user_id):
        await message.reply("🚫 You are not authorized to use this bot.")
        return

    parts = message.text.split(maxsplit=1)
    if len(parts) != 2:
        await message.reply(
            "ℹ️ <b>Usage</b>\n\n"
            "<code>/send_chat_all <message></code>",
            parse_mode="HTML"
        )
        return

    custom_message = parts[1]
    active_tokens = get_active_tokens(user_id)
    tokens = [t["token"] for t in active_tokens]
    
    if not tokens:
        await message.reply("🔍 No active tokens found.")
        return
        
    spam_enabled = get_spam_filter(user_id)

    status = await message.reply(
        f"⏳ <b>Starting Multi-Account Chatroom</b>\n\n"
        f"📊 Active tokens: <code>{len(tokens)}</code>\n"
        f"📝 Message: <code>{custom_message[:50]}...</code>\n"
        f"🛡️ Spam filter: {'ON' if spam_enabled else 'OFF'}\n\n"
        f"🔄 Initializing...",
        parse_mode="HTML"
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
        await status.edit_text(
            f"❌ <b>Error</b>\n\n"
            f"Failed to send messages: {str(e)[:200]}",
            parse_mode="HTML"
        )
        logging.error(f"Error in /send_chat_all command: {str(e)}")

@router.message(Command("invoke"))
async def invoke_command(message: types.Message):
    user_id = message.chat.id
    if not has_valid_access(user_id):
        await message.reply("🚫 You are not authorized to use this bot.")
        return

    tokens = get_tokens(user_id)
    if not tokens:
        await message.reply("🔍 No tokens found.")
        return

    status_msg = await message.reply(
        "🔄 <b>Checking Account Status</b>\n\n"
        "Verifying all accounts...",
        parse_mode="HTML"
    )

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
        
        await status_msg.edit_text(
            f"🔧 <b>Account Cleanup Complete</b>\n\n"
            f"✅ Working accounts: <code>{len(working_accounts)}</code>\n"
            f"❌ Disabled accounts removed: <code>{len(disabled_accounts)}</code>\n\n"
            f"<b>Removed accounts:</b>\n" + 
            "\n".join([f"• {acc['name']}" for acc in disabled_accounts]),
            parse_mode="HTML"
        )
    else:
        await status_msg.edit_text(
            f"✅ <b>All Accounts Working</b>\n\n"
            f"All {len(working_accounts)} accounts are functioning properly.",
            parse_mode="HTML"
        )

@router.message(Command("settings"))
async def settings_command(message: types.Message):
    user_id = message.chat.id
    if not has_valid_access(user_id):
        await message.reply("🚫 You are not authorized to use this bot.")
        return
    
    settings_text = "⚙️ <b>Settings Menu</b>\n\nChoose an option below:"
    
    await message.reply(
        settings_text,
        reply_markup=get_settings_menu(user_id),
        parse_mode="HTML"
    )

@router.message(Command("add"))
async def add_person_command(message: types.Message):
    user_id = message.chat.id
    if not has_valid_access(user_id):
        await message.reply("🚫 You are not authorized to use this bot.")
        return
    args = message.text.strip().split()
    if len(args) < 2:
        await message.reply("Please provide the person ID. Usage: /add <person_id>")
        return
    person_id = args[1]
    token = get_current_account(user_id)
    if not token:
        await message.reply("No active account found. Please set an account first.")
        return
    url = f"https://api.meeff.com/user/undoableAnswer/v5/?userId={person_id}&isOkay=1"
    headers = {"meeff-access-token": token, "Connection": "keep-alive"}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                data = await response.json()
                if data.get("errorCode") == "LikeExceeded":
                    await message.reply("You've reached the daily like limit.")
                elif data.get("errorCode"):
                    await message.reply(f"Failed: {data.get('errorMessage', 'Unknown error')}")
                else:
                    await message.reply(f"Successfully added person with ID: {person_id}")
    except Exception as e:
        logging.error(f"Error adding person by ID: {e}")
        await message.reply("An error occurred while trying to add this person.")

@router.message()
async def handle_new_token(message: types.Message):
    if message.text and message.text.startswith("/"):
        return
    user_id = message.from_user.id

    if message.from_user.is_bot:
        return

    # Handle signup/signin messages first
    if await signup_message_handler(message):
        return

    # Handle DB operation states
    if user_id in db_operation_states:
        state = db_operation_states[user_id]
        
        if state.get("operation") == "connect_db":
            collection_name = message.text.strip()
            if not collection_name.startswith("user_"):
                collection_name = f"user_{collection_name}"
            
            processing_msg = await message.reply(
                "🔄 <b>Connecting to DB</b>\n\nPlease wait...",
                parse_mode="HTML"
            )
            
            success, msg = connect_to_collection(collection_name, user_id)
            if success:
                await processing_msg.edit_text(
                    f"✅ <b>DB Connected Successfully</b>\n\n{msg}",
                    parse_mode="HTML"
                )
            else:
                await processing_msg.edit_text(
                    f"❌ <b>Connection Failed</b>\n\n{msg}",
                    parse_mode="HTML"
                )
            del db_operation_states[user_id]
            return
            
        elif state.get("operation") == "rename_db":
            new_name = message.text.strip()
            
            processing_msg = await message.reply(
                "🔄 <b>Renaming DB</b>\n\nPlease wait...",
                parse_mode="HTML"
            )
            
            success, msg = rename_user_collection(user_id, new_name)
            if success:
                await processing_msg.edit_text(
                    f"✅ <b>DB Renamed Successfully</b>\n\n{msg}",
                    parse_mode="HTML"
                )
            else:
                await processing_msg.edit_text(
                    f"❌ <b>Rename Failed</b>\n\n{msg}",
                    parse_mode="HTML"
                )
            del db_operation_states[user_id]
            return
            
        elif state.get("operation") == "transfer_db":
            try:
                target_user_id = int(message.text.strip())
            except ValueError:
                await message.reply("❌ Invalid user ID. Please enter a valid number.")
                return
            
            processing_msg = await message.reply(
                "🔄 <b>Transferring DB</b>\n\nPlease wait...",
                parse_mode="HTML"
            )
            
            success, msg = transfer_to_user(user_id, target_user_id)
            if success:
                await processing_msg.edit_text(
                    f"✅ <b>DB Transferred Successfully</b>\n\n{msg}",
                    parse_mode="HTML"
                )
            else:
                await processing_msg.edit_text(
                    f"❌ <b>Transfer Failed</b>\n\n{msg}",
                    parse_mode="HTML"
                )
            del db_operation_states[user_id]
            return

    if not has_valid_access(user_id):
        await message.reply("🚫 You are not authorized to use this bot.")
        return

    if message.text:
        token_data = message.text.strip().split(" ")
        token = token_data[0]
        if len(token) < 10:
            await message.reply("❌ Invalid token. Please try again.")
            return

        # Verify token
        url = "https://api.meeff.com/facetalk/vibemeet/history/count/v1"
        params = {'locale': "en"}
        headers = {
            'User-Agent': "okhttp/5.0.0-alpha.14",
            'Accept-Encoding': "gzip",
            'meeff-access-token': token
        }
        
        verification_msg = await message.reply(
            "🔄 <b>Verifying Token</b>\n\n"
            "Please wait...",
            parse_mode="HTML"
        )
        
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url, params=params, headers=headers) as resp:
                    result = await resp.json(content_type=None)
                    if "errorCode" in result and result["errorCode"] == "AuthRequired":
                        await verification_msg.edit_text(
                            "❌ <b>Invalid Token</b>\n\n"
                            "The token you provided is invalid or disabled. Please try a different token.",
                            parse_mode="HTML"
                        )
                        return
            except Exception as e:
                logging.error(f"Error verifying token: {e}")
                await verification_msg.edit_text(
                    "❌ <b>Verification Error</b>\n\n"
                    "Error verifying the token. Please try again.",
                    parse_mode="HTML"
                )
                return

        tokens = get_tokens(user_id)
        account_name = " ".join(token_data[1:]) if len(token_data) > 1 else f"Account {len(tokens) + 1}"
        set_token(user_id, token, account_name)
        
        await verification_msg.edit_text(
            f"✅ <b>Token Verified</b>\n\n"
            f"Your access token has been verified and saved as '<code>{account_name}</code>'.\n\n"
            f"Use the settings menu to manage accounts.",
            parse_mode="HTML"
        )
    else:
        await message.reply("❌ Message text is empty. Please provide a valid token.")

@router.callback_query()
async def callback_handler(callback_query: CallbackQuery):
    user_id = callback_query.from_user.id
    data = callback_query.data

    # Handle signup/signin callbacks first
    if await signup_callback_handler(callback_query):
        return

    if not has_valid_access(user_id):
        await callback_query.answer("🚫 You are not authorized to use this bot.")
        return

    if user_id not in user_states:
        user_states[user_id] = {}
    state = user_states[user_id]

    # DB Settings callbacks
    if data == "db_settings":
        current_info = get_current_collection_info(user_id)
        info_text = "🗄️ <b>Database Settings</b>\n\n"
        
        if current_info["exists"]:
            summary = current_info["summary"]
            info_text += (
                f"📊 <b>Current DB:</b> <code>{current_info['collection_name']}</code>\n"
                f"👤 Accounts: <code>{summary.get('tokens_count', 0)}</code>\n"
                f"📝 Sent Records: <code>{summary.get('sent_records', {}).get('total', 0)}</code>\n"
                f"🛡️ Spam Filter: {'ON' if summary.get('spam_filter_enabled') else 'OFF'}\n\n"
            )
        else:
            info_text += "❌ No database found for your account.\n\n"
        
        info_text += "Choose an option below:"
        
        await callback_query.message.edit_text(
            info_text,
            reply_markup=get_db_settings_menu(),
            parse_mode="HTML"
        )
        return

    elif data == "db_connect":
        db_operation_states[user_id] = {"operation": "connect_db"}
        await callback_query.message.edit_text(
            "🔗 <b>Connect to Database</b>\n\n"
            "Enter the collection name you want to connect to:\n"
            "(e.g., user_123456 or just 123456)",
            parse_mode="HTML"
        )
        return

    elif data == "db_rename":
        db_operation_states[user_id] = {"operation": "rename_db"}
        await callback_query.message.edit_text(
            "📝 <b>Rename Database</b>\n\n"
            "Enter the new name for your database collection:",
            parse_mode="HTML"
        )
        return

    elif data == "db_view":
        collections = list_all_collections()
        if not collections:
            await callback_query.message.edit_text(
                "❌ <b>No Collections Found</b>\n\n"
                "No user collections exist in the database.",
                reply_markup=get_db_settings_menu(),
                parse_mode="HTML"
            )
            return

        view_text = "👁️ <b>All Database Collections</b>\n\n"
        for i, col in enumerate(collections[:10], 1):  # Show first 10
            summary = col["summary"]
            accounts = summary.get("tokens_count", 0)
            created = summary.get("created_at")
            created_str = created.strftime("%Y-%m-%d") if created else "Unknown"
            
            view_text += (
                f"<b>{i}.</b> <code>{col['collection_name']}</code>\n"
                f"   👤 Accounts: {accounts} | 📅 Created: {created_str}\n\n"
            )

        if len(collections) > 10:
            view_text += f"... and {len(collections) - 10} more collections"

        await callback_query.message.edit_text(
            view_text,
            reply_markup=get_db_settings_menu(),
            parse_mode="HTML"
        )
        return

    elif data == "db_transfer":
        db_operation_states[user_id] = {"operation": "transfer_db"}
        await callback_query.message.edit_text(
            "📤 <b>Transfer Database</b>\n\n"
            "Enter the Telegram user ID to transfer your database to:",
            parse_mode="HTML"
        )
        return

    # Unsubscribe callbacks
    elif data == "unsub_current":
        await callback_query.message.edit_text(
            "⚠️ <b>Confirm Unsubscribe Current</b>\n\n"
            "Are you sure you want to unsubscribe the current account from all chatrooms?",
            reply_markup=get_confirmation_menu("unsub_current"),
            parse_mode="HTML"
        )
        return

    elif data == "unsub_all":
        active_tokens = get_active_tokens(user_id)
        await callback_query.message.edit_text(
            f"⚠️ <b>Confirm Unsubscribe All</b>\n\n"
            f"Are you sure you want to unsubscribe ALL {len(active_tokens)} active accounts from chatrooms?",
            reply_markup=get_confirmation_menu("unsub_all"),
            parse_mode="HTML"
        )
        return

    elif data == "confirm_unsub_current":
        token = get_current_account(user_id)
        if not token:
            await callback_query.message.edit_text(
                "❌ No active account found.",
                reply_markup=back_markup,
                parse_mode="HTML"
            )
            return

        status_message = await callback_query.message.edit_text(
            "⏳ <b>Unsubscribing Current Account</b>\n\n"
            "🔄 Processing...",
            parse_mode="HTML"
        )
        await unsubscribe_everyone(token, status_message=status_message, bot=bot, chat_id=user_id)
        return

    elif data == "confirm_unsub_all":
        active_tokens = get_active_tokens(user_id)
        if not active_tokens:
            await callback_query.message.edit_text(
                "❌ No active accounts found.",
                reply_markup=back_markup,
                parse_mode="HTML"
            )
            return

        status_message = await callback_query.message.edit_text(
            f"⏳ <b>Unsubscribing All Accounts</b>\n\n"
            f"📊 Processing {len(active_tokens)} accounts...",
            parse_mode="HTML"
        )

        total_unsubscribed = 0
        for i, token_obj in enumerate(active_tokens, 1):
            await status_message.edit_text(
                f"⏳ <b>Unsubscribing All Accounts</b>\n\n"
                f"📊 Processing account {i}/{len(active_tokens)}: {token_obj['name']}",
                parse_mode="HTML"
            )
            await unsubscribe_everyone(token_obj["token"])
            total_unsubscribed += 1

        await status_message.edit_text(
            f"✅ <b>Unsubscribe Complete</b>\n\n"
            f"Successfully unsubscribed {total_unsubscribed} accounts from all chatrooms.",
            parse_mode="HTML"
        )
        return

    if data == "send_request_menu":
        await callback_query.message.edit_text(
            "🚀 <b>Send Request Options</b>\n\n"
            "Choose your request type:",
            reply_markup=send_request_markup,
            parse_mode="HTML"
        )
        return
    
    elif data == "settings_menu":
        settings_text = "⚙️ <b>Settings Menu</b>\n\nChoose an option below:"
        
        await callback_query.message.edit_text(
            settings_text,
            reply_markup=get_settings_menu(user_id),
            parse_mode="HTML"
        )
        return

    elif data == "show_filters":
        await callback_query.message.edit_text(
            "🎯 <b>Filter Settings</b>\n\n"
            "Configure your search preferences:",
            reply_markup=get_filter_keyboard(),
            parse_mode="HTML"
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
                "👤 <b>No Accounts Found</b>\n\n"
                "No accounts saved. Send a new token to add an account.",
                reply_markup=back_markup,
                parse_mode="HTML"
            )
            return

        buttons = []
        for i, tok in enumerate(tokens):
            is_active = tok.get("active", True)
            status_emoji = "✅" if is_active else "❌"
            is_current = tok['token'] == current_token
            
            # Account name display: Truncate if too long, add current indicator
            account_name_display = f"{'🔹' if is_current else '▫️'} {tok['name'][:15]}{'...' if len(tok['name']) > 15 else ''}" 

            # All buttons for this account are now in a single row
            buttons.append([
                InlineKeyboardButton(
                    text=account_name_display,
                    callback_data=f"set_account_{i}" # This button still sets as current
                ),
                InlineKeyboardButton(
                    text=f"{status_emoji}", # Only emoji for status
                    callback_data=f"toggle_status_{i}"
                ),
                InlineKeyboardButton(
                    text="👁️", # Only emoji for view
                    callback_data=f"view_account_{i}"
                ),
                InlineKeyboardButton(
                    text="🗑️", # Only emoji for delete
                    callback_data=f"confirm_delete_{i}"
                )
            ])

        buttons.append([
            InlineKeyboardButton(text="🔙 Back", callback_data="settings_menu")
        ])

        current_text = f"Current: {current_token[:10]}..." if current_token else "None"
        await callback_query.message.edit_text(
            f"👤 <b>Manage Accounts</b>\n\n"
            f"🔹 = Current account\n"
            f"Active accounts are used for multi-token functions.\n\n"
            f"<b>Current:</b> <code>{current_text}</code>",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
            parse_mode="HTML"
        )
        return
    
    elif data.startswith("view_account_"):
        idx = int(data.split("_")[-1])
        tokens = get_tokens(user_id)
        if 0 <= idx < len(tokens):
            token = tokens[idx]["token"]
            info_card = get_info_card(user_id, token)
            if info_card:
                await callback_query.message.answer(
                    info_card,
                    parse_mode="HTML",
                    disable_web_page_preview=True
                )
                await callback_query.answer("📱 Account info displayed below")
            else:
                await callback_query.answer("❌ No information card found for this account.", show_alert=True)
        else:
            await callback_query.answer("❌ Invalid account selected.")
        return
    
    elif data.startswith("confirm_delete_"):
        idx = int(data.split("_")[-1])
        tokens = get_tokens(user_id)
        if 0 <= idx < len(tokens):
            account_name = tokens[idx]["name"]
            buttons = [
                [
                    InlineKeyboardButton(text="🗑️ Yes, Delete", callback_data=f"delete_account_{idx}"),
                    InlineKeyboardButton(text="❌ Cancel", callback_data="manage_accounts")
                ]
            ]
            await callback_query.message.edit_text(
                f"⚠️ <b>Confirm Deletion</b>\n\n"
                f"Are you sure you want to delete account:\n"
                f"<code>{account_name}</code>?\n\n"
                f"This action cannot be undone.",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
                parse_mode="HTML"
            )
        else:
            await callback_query.answer("❌ Invalid account selected.")
        return
        
        elif data.startswith("toggle_status_"):
        idx = int(data.split("_")[-1])
        tokens = get_tokens(user_id)
        if 0 <= idx < len(tokens):
            token = tokens[idx]["token"]
            old_status = tokens[idx].get("active", True)
            toggle_token_status(user_id, token)
            new_status = not old_status

            await callback_query.answer(
                f"{'✅ Activated' if new_status else '❌ Deactivated'} {tokens[idx]['name']}"
            )

            # Rebuild the manage accounts UI directly
            tokens = get_tokens(user_id)
            current_token = get_current_account(user_id)

            buttons = []
            for i, tok in enumerate(tokens):
                is_active = tok.get("active", True)
                status_emoji = "✅" if is_active else "❌"
                is_current = tok['token'] == current_token
                account_name_display = f"{'🔹' if is_current else '▫️'} {tok['name'][:15]}{'...' if len(tok['name']) > 15 else ''}"

                buttons.append([
                    InlineKeyboardButton(text=account_name_display, callback_data=f"set_account_{i}"),
                    InlineKeyboardButton(text=status_emoji, callback_data=f"toggle_status_{i}"),
                    InlineKeyboardButton(text="👁️", callback_data=f"view_account_{i}"),
                    InlineKeyboardButton(text="🗑️", callback_data=f"confirm_delete_{i}")
                ])

            buttons.append([
                InlineKeyboardButton(text="🔙 Back", callback_data="settings_menu")
            ])

            current_text = f"Current: {current_token[:10]}..." if current_token else "None"
            await callback_query.message.edit_text(
                f"👤 <b>Manage Accounts</b>\n\n"
                f"🔹 = Current account\n"
                f"Active accounts are used for multi-token functions.\n\n"
                f"<b>Current:</b> <code>{current_text}</code>",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
                parse_mode="HTML"
            )
        else:
            await callback_query.answer("❌ Invalid account selected.")
        return


    elif data == "toggle_spam_filter":
        new_state = not get_spam_filter(user_id)
        set_spam_filter(user_id, new_state)
        await callback_query.answer(
            f"🛡️ Spam Filter {'Enabled ✅' if new_state else 'Disabled ❌'}"
        )
        
        # Refresh settings menu
        settings_text = "⚙️ <b>Settings Menu</b>\n\nChoose an option below:"
        
        await callback_query.message.edit_text(
            settings_text,
            reply_markup=get_settings_menu(user_id),
            parse_mode="HTML"
        )
        return

    elif data.startswith("set_account_"):
        idx = int(data.split("_")[-1])
        tokens = get_tokens(user_id)
        if 0 <= idx < len(tokens):
            if not tokens[idx].get("active", True):
                await callback_query.answer("❌ This account is inactive. Activate it first.", show_alert=True)
                return
            set_current_account(user_id, tokens[idx]["token"])
            await callback_query.answer(f"✅ Set {tokens[idx]['name']} as current account")
            
            # Refresh the manage accounts view
            callback_query.data = "manage_accounts"
            await callback_handler(callback_query)
        else:
            await callback_query.answer("❌ Invalid account selected.")
        return

    elif data.startswith("delete_account_"):
        idx = int(data.split("_")[-1])
        tokens = get_tokens(user_id)
        if 0 <= idx < len(tokens):
            account_name = tokens[idx]["name"]
            delete_token(user_id, tokens[idx]["token"])
            await callback_query.message.edit_text(
                f"🗑️ <b>Account Deleted</b>\n\n"
                f"Account '<code>{account_name}</code>' has been deleted.",
                reply_markup=back_markup,
                parse_mode="HTML"
            )
        else:
            await callback_query.answer("❌ Invalid account selected.")
        return

    elif data == "back_to_menu":
        welcome_text = "🎯 <b>Meeff Bot Dashboard</b>\n\nChoose an option below to get started:"
        
        await callback_query.message.edit_text(
            welcome_text,
            reply_markup=start_markup,
            parse_mode="HTML"
        )
        return

    elif data == "start":
        if state.get("running", False):
            await callback_query.answer("⚠️ Requests are already running!")
        else:
            state["running"] = True
            state["total_added_friends"] = 0
            try:
                status_message = await callback_query.message.edit_text(
                    "🔄 <b>Initializing Requests</b>\n\n"
                    "Setting up friend requests...",
                    reply_markup=stop_markup,
                    parse_mode="HTML"
                )
                state["status_message_id"] = status_message.message_id
                state["pinned_message_id"] = status_message.message_id
                
                await bot.pin_chat_message(chat_id=user_id, message_id=state["status_message_id"])
                
                asyncio.create_task(run_requests(user_id, bot, TARGET_CHANNEL_ID))
                await callback_query.answer("🚀 Requests started!")
            except Exception as e:
                logging.error(f"Error while starting requests: {e}")
                await callback_query.message.edit_text(
                    "❌ <b>Failed to Start</b>\n\n"
                    "Failed to start requests. Please try again later.",
                    reply_markup=start_markup,
                    parse_mode="HTML"
                )
                state["running"] = False

    elif data == "start_all":
        if state.get("running", False):
            await callback_query.answer("⚠️ Another request is already running!")
        else:
            tokens = get_active_tokens(user_id)
            if not tokens:
                await callback_query.answer("❌ No active tokens found.", show_alert=True)
                return
        
            state["running"] = True
            state["total_added_friends"] = 0
        
            try:
                msg = await callback_query.message.edit_text(
                    f"🔄 <b>Starting Multi-Account Requests</b>\n\n"
                    f"📊 Active accounts: <code>{len(tokens)}</code>\n"
                    f"🚀 Initializing...",
                    reply_markup=stop_markup,
                    parse_mode="HTML"
                )
                state["status_message_id"] = msg.message_id
                state["pinned_message_id"] = msg.message_id
                
                await bot.pin_chat_message(chat_id=user_id, message_id=msg.message_id)
                
                asyncio.create_task(process_all_tokens(user_id, tokens, bot, TARGET_CHANNEL_ID))
                await callback_query.answer("🚀 Multi-account processing started!")
            except Exception as e:
                logging.error(f"Error starting all tokens: {e}")
                await callback_query.message.edit_text(
                    "❌ <b>Failed to Start</b>\n\n"
                    "Failed to start processing all tokens. Please try again later.",
                    reply_markup=start_markup,
                    parse_mode="HTML"
                )
                state["running"] = False

    elif data == "stop":
        if not state.get("running", False):
            await callback_query.answer("⚠️ Requests are not running!")
        else:
            state["running"] = False
            state["stopped"] = True  # Mark as user-stopped
            message_text = (
                f"⏹️ <b>Requests Stopped</b>\n\n"
                f"Total Added Friends: <code>{state.get('total_added_friends', 0)}</code>\n\n"
                f"Use the button below to start again."
            )
            await callback_query.message.edit_text(
                message_text,
                reply_markup=start_markup,
                parse_mode="HTML"
            )
            await callback_query.answer("⏹️ Requests stopped.")
            if state.get("pinned_message_id"):
                await bot.unpin_chat_message(chat_id=user_id, message_id=state["pinned_message_id"])
                state["pinned_message_id"] = None

    elif data == "all_countries":
        if state.get("running", False):
            await callback_query.answer("⚠️ Another process is already running!")
        else:
            state["running"] = True
            try:
                status_message = await callback_query.message.edit_text(
                    "🌍 <b>Starting All Countries Feature</b>\n\n"
                    "🔄 Initializing global search...",
                    reply_markup=stop_markup,
                    parse_mode="HTML"
                )
                state["status_message_id"] = status_message.message_id
                state["pinned_message_id"] = status_message.message_id
                state["stop_markup"] = stop_markup
                await bot.pin_chat_message(chat_id=user_id, message_id=status_message.message_id)
                asyncio.create_task(run_all_countries(user_id, state, bot, get_current_account))
                await callback_query.answer("🌍 All Countries feature started!")
            except Exception as e:
                logging.error(f"Error while starting All Countries feature: {e}")
                await callback_query.message.edit_text(
                    "❌ <b>Failed to Start</b>\n\n"
                    "Failed to start All Countries feature.",
                    reply_markup=start_markup,
                    parse_mode="HTML"
                )
                state["running"] = False

async def set_bot_commands():
    commands = [
        BotCommand(command="start", description="🎯 Start the bot"),
        BotCommand(command="lounge", description="💬 Send message in the lounge"),
        BotCommand(command="send_lounge_all", description="🔄 Send lounge message to ALL accounts"),
        BotCommand(command="chatroom", description="📨 Send message in chatrooms"),
        BotCommand(command="send_chat_all", description="🔄 Send chatroom message to ALL accounts"),
        BotCommand(command="invoke", description="🔧remove disabled accounts"),
        BotCommand(command="skip", description="⏭️ Unsubscribe"),
        BotCommand(command="settings", description="⚙️ bot settings"),
        BotCommand(command="add", description="➕ add a person by ID"),
        BotCommand(command="signup", description="⚙️Meeff account"),
        BotCommand(command="password", description="🔐Enter password for temporary access")
    ]
    await bot.set_my_commands(commands)

async def main():
    await set_bot_commands()
    dp.include_router(router)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
