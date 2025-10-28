import aiohttp
import json
import random
import itertools
import logging
import asyncio 
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from dateutil import parser
from device_info import get_or_create_device_info_for_email, get_or_create_device_info_for_token, get_api_payload_with_device_info, get_headers_with_device_info
from db import set_token, set_info_card, set_signup_config, get_signup_config, set_user_filters
from filters import get_nationality_keyboard

# Logging configuration
logger = logging.getLogger(__name__)

# Configuration constants (omitted for brevity)
DEFAULT_BIOS = [
    "Love traveling and meeting new people!",
    "Coffee lover and adventure seeker",
    "Passionate about music and good vibes",
    "Foodie exploring new cuisines",
    "Fitness enthusiast and nature lover",
]
DEFAULT_PHOTOS = (
    "https://meeffus.s3.amazonaws.com/profile/2025/06/16/"
    "20250616052423006_profile-1.0-bd262b27-1916-4bd3-9f1d-0e7fdba35268.jpg|"
    "https://meeffus.s3.amazonaws.com/profile/2025/06/16/"
    "20250616052438006_profile-1.0-349bf38c-4555-40cc-a322-e61afe15aa35.jpg"
)

# Global state
user_signup_states: Dict[int, Dict] = {}

# Inline Keyboard Menus (omitted for brevity)
SIGNUP_MENU = InlineKeyboardMarkup(inline_keyboard=[
    [
        InlineKeyboardButton(text="Sign Up", callback_data="signup_go"),
        InlineKeyboardButton(text="Sign In", callback_data="signin_go")
    ],
    [
        InlineKeyboardButton(text="Signup Config", callback_data="signup_settings")
    ],
    [InlineKeyboardButton(text="Back to Main Menu", callback_data="back_to_menu")]
])

VERIFY_ALL_BUTTON = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="Verify All Emails", callback_data="verify_accounts")],
    [InlineKeyboardButton(text="Back", callback_data="signup_menu")]
])

RETRY_VERIFY_BUTTON = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="Retry Pending Verification", callback_data="retry_pending")],
    [InlineKeyboardButton(text="Back", callback_data="signup_menu")]
])

BACK_TO_SIGNUP = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="Back", callback_data="signup_menu")]
])

BACK_TO_CONFIG = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="Back", callback_data="signup_settings")]
])

DONE_PHOTOS = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="Done", callback_data="signup_photos_done")],
    [InlineKeyboardButton(text="Back", callback_data="signup_menu")]
])

CONFIG_MENU_REVISED = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="Auto Signup: Turn OFF", callback_data="toggle_auto_signup")],
    [InlineKeyboardButton(text="Setup/Change Signup Details", callback_data="setup_signup_config")],
    [InlineKeyboardButton(text="Back", callback_data="signup_menu")]
])

FILTER_NATIONALITY_KB = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="All Countries", callback_data="signup_filter_nationality_all")],
    [
        InlineKeyboardButton(text="🇷🇺 RU", callback_data="signup_filter_nationality_RU"),
        InlineKeyboardButton(text="🇺🇦 UA", callback_data="signup_filter_nationality_UA"),
        InlineKeyboardButton(text="🇧🇾 BY", callback_data="signup_filter_nationality_BY"),
        InlineKeyboardButton(text="🇮🇷 IR", callback_data="signup_filter_nationality_IR"),
        InlineKeyboardButton(text="🇵🇭 PH", callback_data="signup_filter_nationality_PH")
    ],
    [
        InlineKeyboardButton(text="🇵🇰 PK", callback_data="signup_filter_nationality_PK"),
        InlineKeyboardButton(text="🇺🇸 US", callback_data="signup_filter_nationality_US"),
        InlineKeyboardButton(text="🇮🇳 IN", callback_data="signup_filter_nationality_IN"),
        InlineKeyboardButton(text="🇩🇪 DE", callback_data="signup_filter_nationality_DE"),
        InlineKeyboardButton(text="🇫🇷 FR", callback_data="signup_filter_nationality_FR")
    ],
    [
        # FIX APPLIED HERE: changed 'callback.data' to 'callback_data'
        InlineKeyboardButton(text="🇧🇷 BR", callback_data="signup_filter_nationality_BR"), 
        InlineKeyboardButton(text="🇨🇳 CN", callback_data="signup_filter_nationality_CN"),
        InlineKeyboardButton(text="🇯🇵 JP", callback_data="signup_filter_nationality_JP"),
        InlineKeyboardButton(text="🇰🇷 KR", callback_data="signup_filter_nationality_KR"),
        InlineKeyboardButton(text="🇨🇦 CA", callback_data="signup_filter_nationality_CA")
    ],
    [
        InlineKeyboardButton(text="🇦🇺 AU", callback_data="signup_filter_nationality_AU"),
        InlineKeyboardButton(text="🇮🇹 IT", callback_data="signup_filter_nationality_IT"),
        InlineKeyboardButton(text="🇪🇸 ES", callback_data="signup_filter_nationality_ES"),
        InlineKeyboardButton(text="🇿🇦 ZA", callback_data="signup_filter_nationality_ZA"),
        InlineKeyboardButton(text="🇹🇷 TR", callback_data="signup_filter_nationality_TR")
    ],
    [InlineKeyboardButton(text="Back", callback_data="signup_photos_done")]
])

def format_user_with_nationality(user: Dict) -> str:
    """Format user information into a displayable string with nationality and last active time."""
    def time_ago(dt_str: Optional[str]) -> str:
        if not dt_str:
            return "N/A"
        try:
            dt = parser.isoparse(dt_str)
            now = datetime.now(timezone.utc)
            diff = now - dt
            minutes = int(diff.total_seconds() // 60)
            if minutes < 1:
                return "just now"
            if minutes < 60:
                return f"{minutes} min ago"
            hours = minutes // 60
            if hours < 24:
                return f"{hours} hr ago"
            days = hours // 24
            return f"{days} day(s) ago"
        except Exception as e:
            logger.error(f"Error parsing date {dt_str}: {e}")
            return "unknown"

    last_active = time_ago(user.get("recentAt"))
    card = (
        f"<b>📱 Account Information</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>👤 Name:</b> {user.get('name', 'N/A')}\n"
        f"<b>🆔 ID:</b> <code>{user.get('_id', 'N/A')}</code>\n"
        f"<b>📝 Bio:</b> {user.get('description', 'N/A')}\n"
        f"<b>🎂 Birth Year:</b> {user.get('birthYear', 'N/A')}\n"
        f"<b>🌍 Country:</b> {user.get('nationalityCode', 'N/A')}\n"
        f"<b>📱 Platform:</b> {user.get('platform', 'N/A')}\n"
        f"<b>⭐ Score:</b> {user.get('profileScore', 'N/A')}\n"
        f"<b>📍 Distance:</b> {user.get('distance', 'N/A')} km\n"
        f"<b>🗣️ Languages:</b> {', '.join(user.get('languageCodes', [])) or 'N/A'}\n"
        f"<b>🕐 Last Active:</b> {last_active}\n"
    )

    if user.get('photoUrls'):
        card += f"<b>📸 Photos:</b> " + ' '.join([f"<a href='{url}'>📷</a>" for url in user.get('photoUrls', [])])
    
    if "email" in user:
        card += f"\n\n<b>📧 Email:</b> <code>{user['email']}</code>"
    if "password" in user:
        card += f"\n<b>🔐 Password:</b> <code>{user['password']}</code>"
    if "token" in user:
        card += f"\n<b>🔑 Token:</b> <code>{user['token']}</code>"
    
    return card

def generate_email_variations(base_email: str, count: int = 1000) -> List[str]:
    """Generate variations of an email address by adding dots to the username."""
    if '@' not in base_email:
        return []
    username, domain = base_email.split('@', 1)
    variations = {base_email}
    
    # Restrict max dots to prevent combinatorial explosion for long usernames
    max_dots = min(4, len(username) - 1)
    
    # Generate variations using dots
    for i in range(1, max_dots + 1):
        for positions in itertools.combinations(range(1, len(username)), i):
            if len(variations) >= count:
                return list(variations)
            new_username = list(username)
            # Insert dots starting from the end to keep indices correct
            for pos in reversed(positions):
                new_username.insert(pos, '.')
            variations.add(''.join(new_username) + '@' + domain)
            
    return list(variations)[:count]

def get_random_bio() -> str:
    """Return a random bio from the default bios list."""
    return random.choice(DEFAULT_BIOS)

async def check_email_exists(email: str) -> Tuple[bool, str]:
    """Check if an email is available for signup."""
    url = "https://api.meeff.com/user/checkEmail/v1"
    payload = {"email": email, "locale": "en"}
    headers = {
        'User-Agent': "okhttp/5.1.0",
        'Accept-Encoding': "gzip",
        'Content-Type': "application/json; charset=utf-8"
    }
    
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(url, json=payload, headers=headers) as response:
                resp_json = await response.json()
                if response.status == 406 or resp_json.get("errorMessage") == "This email is already in use.":
                    return False, resp_json.get("errorMessage", "This email is already in use.")
                return True, ""
        except Exception as e:
            logger.error(f"Error checking email {email}: {e}")
            return False, "Failed to check email availability."

async def select_available_emails(base_email: str, num_accounts: int, pending_emails: List[str], used_emails: List[str]) -> List[str]:
    """
    Select available email variations, prioritizing pending emails and excluding
    emails known to be in use.
    """
    available_emails = []
    
    # Emails that failed the availability check or sign-up (known to be used)
    used_emails_set = set(used_emails)
    
    # --- Check pending emails (that are not known to be used) ---
    pending_to_check = [e for e in pending_emails if e not in used_emails_set]
    pending_check_tasks = []
    for email in pending_to_check:
        pending_check_tasks.append((email, check_email_exists(email)))

    # Execute pending checks concurrently
    if pending_check_tasks:
        pending_results = await asyncio.gather(*[task for email, task in pending_check_tasks])
        for i, result in enumerate(pending_results):
            email, _ = pending_check_tasks[i]
            is_available, _ = result
            if is_available and len(available_emails) < num_accounts:
                available_emails.append(email)

    # --- Check new variations if needed ---
    if len(available_emails) < num_accounts:
        # Generate enough variations to check
        email_variations = generate_email_variations(base_email, num_accounts * 10)
        
        # Exclude: 1. Already available, 2. Pending, 3. Known Used
        new_variations = [
            e for e in email_variations 
            if e not in pending_emails and e not in available_emails and e not in used_emails_set
        ]
        
        new_check_tasks = []
        for email in new_variations:
            new_check_tasks.append((email, check_email_exists(email)))
        
        # Execute new checks concurrently
        if new_check_tasks:
            new_results = await asyncio.gather(*[task for email, task in new_check_tasks])

            for i, result in enumerate(new_results):
                email, availability_error = new_check_tasks[i]
                is_available, _ = result
                
                if is_available and len(available_emails) < num_accounts:
                    available_emails.append(email)
    
    return available_emails

def get_available_variation_count(base_email: Optional[str], used_emails: List[str]) -> Tuple[int, int]:
    """
    Calculates the total number of POTENTIAL variations and the number of 
    VARIATIONS AVAILABLE (Total - Used).
    """
    if not base_email:
        return 0, 0
    
    # Get all potential variations (up to 1000)
    all_variations = generate_email_variations(base_email, count=1000) 
    total_variations = len(all_variations)
    
    # Filter out emails known to be used
    used_emails_set = set(used_emails)
    available_variations = [e for e in all_variations if e not in used_emails_set]
    
    return total_variations, len(available_variations)

async def show_signup_preview(message: Message, user_id: int, state: Dict) -> None:
    """Show a preview of the signup configuration with exact emails to be used."""
    config = await get_signup_config(user_id) or {}
    if not all(k in config for k in ['email', 'password', 'gender', 'birth_year', 'nationality']):
        await message.edit_text(
            "<b>Configuration Incomplete</b>\n\nYou must set up all details in 'Signup Config' first.",
            reply_markup=SIGNUP_MENU,
            parse_mode="HTML"
        )
        return
    # Temporarily update message while running concurrent checks
    await message.edit_text("<b>Checking email availability concurrently...</b> This may take a moment.")
    
    num_accounts = state.get('num_accounts', 1)
    pending_emails = [acc['email'] for acc in state.get('pending_accounts', [])]
    used_emails = config.get("used_emails", []) 
    
    available_emails = await select_available_emails(
        config.get("email", ""), 
        num_accounts, 
        pending_emails, 
        used_emails 
    )
    
    state["selected_emails"] = available_emails
    filter_nat = state.get('filter_nationality', 'All Countries')
    email_list = '\n'.join([f"{i+1}. <code>{email}</code>{' (Pending)' if email in pending_emails else ''}" for i, email in enumerate(available_emails)]) if available_emails else "No available emails found!"
    preview_text = (
        f"<b>Signup Preview</b>\n\n"
        f"<b>Name:</b> {state.get('name', 'N/A')}\n"
        f"<b>Photos:</b> {len(state.get('photos', []))} uploaded\n"
        f"<b>Number of Accounts:</b> {num_accounts}\n"
        f"<b>Gender:</b> {config.get('gender', 'N/A')}\n"
        f"<b>Birth Year:</b> {config.get('birth_year', 'N/A')}\n"
        f"<b>Nationality:</b> {config.get('nationality', 'N/A')}\n"
        f"<b>Filter Nationality:</b> {filter_nat}\n\n"
        f"<b>Emails to be Used:</b>\n{email_list}\n\n"
        f"<b>Ready to create {len(available_emails)} of {num_accounts} requested account{'s' if num_accounts > 1 else ''}?</b>"
    )
    confirm_text = f"Create {len(available_emails)} Account{'s' if len(available_emails) != 1 else ''}"
    menu = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=confirm_text, callback_data="create_accounts_confirm")],
        [InlineKeyboardButton(text="Back", callback_data="signup_menu")]
    ])
    await message.edit_text(preview_text, reply_markup=menu, parse_mode="HTML")
    user_signup_states[user_id] = state

async def signup_settings_command(message: Message, is_callback: bool = False) -> None:
    """Display and manage signup configuration settings."""
    user_id = message.chat.id
    config = await get_signup_config(user_id) or {}
    auto_signup_status = config.get('auto_signup', False)
    base_email = config.get('email')
    
    # --- NEW LOGIC FOR CONFIG DISPLAY ---
    used_emails = config.get("used_emails", [])
    total_variations, available_count = get_available_variation_count(base_email, used_emails)
    used_count = len(used_emails)
    
    email_status_text = f"<b>Base Email:</b> <code>{base_email or 'Not set'}</code>\n"
    if base_email:
        email_status_text += f"<b>Available Variations:</b> {available_count} of {total_variations} total\n"
        email_status_text += f"<b>Used/Unavailable Emails:</b> {used_count}"
    # ------------------------------------
    
    config_text = (
        f"<b>Signup Configuration</b>\n\nSet default values and enable Auto Signup.\n\n"
        f"{email_status_text}\n" 
        f"<b>Password:</b> <code>{'*' * len(config.get('password', '')) if config.get('password') else 'Not set'}</code>\n"
        f"<b>Gender:</b> {config.get('gender', 'Not set')}\n"
        f"<b>Birth Year:</b> {config.get('birth_year', 'Not set')}\n"
        f"<b>Nationality:</b> {config.get('nationality', 'Not set')}\n"
        f"<b>Auto Signup:</b> {'ON' if auto_signup_status else 'OFF'}\n\n"
        "Turn <b>Auto Signup ON</b> to use these settings automatically."
    )
    menu = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"Auto Signup: {'Turn OFF' if auto_signup_status else 'Turn ON'}", callback_data="toggle_auto_signup")],
        [InlineKeyboardButton(text="Setup/Change Signup Details", callback_data="setup_signup_config")],
        [InlineKeyboardButton(text="Back", callback_data="signup_menu")]
    ])
    try:
        if is_callback:
            await message.edit_text(config_text, reply_markup=menu, parse_mode="HTML")
        else:
            await message.answer(config_text, reply_markup=menu, parse_mode="HTML")
    except Exception as e:
        logger.error(f"Error displaying signup settings: {e}")

async def signup_command(message: Message) -> None:
    """Handle the /signup command to initiate account creation."""
    user_signup_states[message.chat.id] = {"stage": "menu"}
    await message.answer(
        "<b>Account Creation</b>\n\nChoose an option:",
        reply_markup=SIGNUP_MENU,
        parse_mode="HTML"
    )

async def signup_callback_handler(callback: CallbackQuery) -> bool:
    """Handle callback queries for signup-related actions."""
    user_id = callback.from_user.id
    state = user_signup_states.get(user_id, {})
    data = callback.data

    if data == "signup_settings":
        await signup_settings_command(callback.message, is_callback=True)
    elif data == "toggle_auto_signup":
        config = await get_signup_config(user_id) or {}
        config['auto_signup'] = not config.get('auto_signup', False)
        await set_signup_config(user_id, config)
        await callback.answer(f"Auto Signup turned {'ON' if config['auto_signup'] else 'OFF'}")
        await signup_settings_command(callback.message, is_callback=True)
    elif data == "setup_signup_config":
        # Start configuration process, beginning with the email
        state["stage"] = "config_email"
        user_signup_states[user_id] = state
        await callback.message.edit_text(
            "<b>Setup Email</b>\n\nEnter your base Gmail address (e.g., yourname@gmail.com). This will be used to generate dot variations for multiple accounts.",
            reply_markup=BACK_TO_CONFIG,
            parse_mode="HTML"
        )
    elif data == "signup_go":
        config = await get_signup_config(user_id) or {}
        if not all(k in config for k in ['email', 'password', 'gender', 'birth_year', 'nationality']):
            await callback.message.edit_text(
                "<b>Configuration Incomplete</b>\n\nPlease set up all details in <b>Signup Config</b> first.",
                reply_markup=SIGNUP_MENU,
                parse_mode="HTML"
            )
        else:
            state["stage"] = "ask_num_accounts"
            user_signup_states[user_id] = state
            await callback.message.edit_text(
                "<b>Account Creation</b>\n\nEnter the number of accounts to create (1-30):",
                reply_markup=BACK_TO_SIGNUP,
                parse_mode="HTML"
            )
    elif data == "signup_photos_done":
        state["stage"] = "ask_filter_nationality"
        await callback.message.edit_text(
            "<b>Select Filter Nationality</b>\n\nChoose the nationality filter for requests:",
            reply_markup=FILTER_NATIONALITY_KB,
            parse_mode="HTML"
        )
    elif data.startswith("signup_filter_nationality_"):
        code = data.split("_")[-1] if len(data.split("_")) > 3 else ""
        state["filter_nationality"] = code if code != "all" else ""
        await show_signup_preview(callback.message, user_id, state)
    elif data == "create_accounts_confirm":
        # 1. Immediately update the message to acknowledge the command
        await callback.message.edit_text("<b>Creating Accounts Concurrently...</b>", parse_mode="HTML")
        
        config = await get_signup_config(user_id) or {}
        num_accounts = state.get("num_accounts", 1)
        selected_emails = state.get("selected_emails", [])
        used_emails = set(config.get("used_emails", [])) # Set for fast lookups
        
        if not selected_emails:
            await callback.message.edit_text(
                "<b>No Available Emails</b>\n\nNo valid email variations found. Please try a different base email in Signup Config.",
                reply_markup=SIGNUP_MENU,
                parse_mode="HTML"
            )
            return True
        
        # Prepare concurrent tasks
        signup_tasks = []
        accounts_to_create = []
        for email in selected_emails[:num_accounts]:
            acc_state = {
                "email": email,
                "password": config.get("password"),
                "name": state.get('name', 'User'),
                "gender": config.get("gender"),
                "desc": get_random_bio(),
                "photos": state.get("photos", []),
                "birth_year": config.get("birth_year", 2000),
                "nationality": config.get("nationality", "US")
            }
            # Only run sign-up if the email is not already marked as used (in case of a race condition)
            if email not in used_emails:
                signup_tasks.append(try_signup(acc_state, user_id)) 
                accounts_to_create.append(acc_state)
            
        # Execute tasks concurrently
        results = await asyncio.gather(*signup_tasks)
        
        # Process results
        created_accounts = []
        for i, res in enumerate(results):
            acc_state = accounts_to_create[i]
            if res.get("user", {}).get("_id"):
                # SUCCESS
                created_accounts.append({
                    "email": acc_state["email"],
                    "name": acc_state["name"],
                    "password": config.get("password")
                })
            elif "email address is already in use" in res.get("errorMessage", "").lower():
                # FAILURE: Mark this email as used
                used_emails.add(acc_state["email"])
            else:
                # FAILURE: Other errors (e.g., connection, bad data)
                logger.error(f"Sign-up failed for {acc_state['email']} with unknown error: {res.get('errorMessage', 'N/A')}")
        
        # --- CRITICAL: Update the used_emails list in the config DB ---
        if used_emails:
            config['used_emails'] = list(used_emails)
            await set_signup_config(user_id, config)
        
        state["created_accounts"] = created_accounts
        state["verified_accounts"] = []
        state["pending_accounts"] = created_accounts.copy()
        
        result_text = (
            f"<b>Account Creation Results</b>\n\n<b>Created:</b> {len(created_accounts)} account{'s' if len(created_accounts) != 1 else ''}\n\n"
        )
        if created_accounts:
            result_text += "<b>Created Accounts:</b>\n" + '\n'.join([
                f"• {a['name']} - <code>{a['email']}</code>" for a in created_accounts
            ])
        
        if len(selected_emails) > len(created_accounts):
             result_text += "\n\n⚠️ Some emails were already in use and have been skipped for future runs."
             
        result_text += "\n\nPlease verify all emails, then click the button below."
        
        # Final response, well within the timeout
        await callback.message.edit_text(
            result_text,
            reply_markup=VERIFY_ALL_BUTTON,
            parse_mode="HTML"
        )

    elif data == "verify_accounts" or data == "retry_pending":
        pending = state.get("pending_accounts", [])
        if not pending:
            await callback.message.edit_text(
                "<b>No Pending Accounts</b>\n\nAll accounts are either verified or none were created.",
                reply_markup=SIGNUP_MENU,
                parse_mode="HTML"
            )
            return True
            
        await callback.message.edit_text("<b>Verifying Accounts Concurrently...</b>", parse_mode="HTML")
        
        verified = state.get("verified_accounts", [])
        filter_nat = state.get("filter_nationality", "")
        
        # Prepare concurrent sign-in tasks
        signin_tasks = []
        for acc in pending:
            # The try_signin function contains throttling logic
            task = try_signin(acc["email"], acc["password"], user_id) 
            signin_tasks.append(task)
            
        # Execute all sign-in tasks concurrently
        results = await asyncio.gather(*signin_tasks)
        
        new_pending = []
        
        # Process results
        for i, res in enumerate(results):
            acc = pending[i]
            if res.get("accessToken") and res.get("user"):
                token = res["accessToken"]
                
                # DB operations remain sequential to ensure atomic updates per account
                await set_token(user_id, token, acc["name"], acc["email"])
                await set_user_filters(user_id, token, {"filterNationalityCode": filter_nat})
                
                res["user"].update({
                    "email": acc["email"],
                    "password": acc["password"],
                    "token": token
                })
                await set_info_card(user_id, token, format_user_with_nationality(res["user"]), acc["email"])
                verified.append(acc)
            else:
                new_pending.append(acc)
                
        state["verified_accounts"] = verified
        state["pending_accounts"] = new_pending
        
        # Final results message
        if not new_pending:
            result_text = (
                f"<b>Verification Results</b>\n\n"
                f"<b>All Accounts Verified:</b> {len(verified)} account{'s' if len(verified) != 1 else ''}\n\n"
                "All accounts have been successfully verified and saved."
            )
            reply_markup = SIGNUP_MENU
        else:
            result_text = (
                f"<b>Verification Results</b>\n\n"
                f"<b>Verified:</b> {len(verified)} account{'s' if len(verified) != 1 else ''}\n"
                f"<b>Pending Verification:</b> {len(new_pending)} account{'s' if len(new_pending) != 1 else ''}\n\n"
                "<b>Pending Accounts:</b>\n" + '\n'.join([f"• <code>{a['email']}</code>" for a in new_pending]) +
                "\n\nPlease verify these emails, then retry."
            )
            reply_markup = RETRY_VERIFY_BUTTON
            
        await callback.message.edit_text(
            result_text,
            reply_markup=reply_markup,
            parse_mode="HTML"
        )

    elif data == "signup_menu":
        state["stage"] = "menu"
        await callback.message.edit_text(
            "<b>Account Creation</b>\n\nChoose an option:",
            reply_markup=SIGNUP_MENU,
            parse_mode="HTML"
        )
    elif data == "signin_go":
        state["stage"] = "signin_email"
        await callback.message.edit_text(
            "<b>Sign In</b>\n\nEnter your email address:",
            reply_markup=BACK_TO_SIGNUP,
            parse_mode="HTML"
        )
    else:
        await callback.answer()
        return False
    
    user_signup_states[user_id] = state
    await callback.answer()
    return True

async def signup_message_handler(message: Message) -> bool:
    """Handle messages during the signup process."""
    user_id = message.from_user.id
    if user_id not in user_signup_states:
        return False
    state = user_signup_states.get(user_id, {})
    stage = state.get("stage", "")
    text = message.text.strip() if message.text else ""

    if stage.startswith("config_"):
        config = await get_signup_config(user_id) or {}
        if stage == "config_email":
            if '@' not in text:
                await message.answer("Invalid Email. Please try again:", reply_markup=BACK_TO_CONFIG, parse_mode="HTML")
                return True
            config["email"] = text
            # Clear used emails when changing the base email
            config["used_emails"] = [] 
            state["stage"] = "config_password"
            await message.answer("<b>Setup Password</b>\nEnter the password:", reply_markup=BACK_TO_CONFIG, parse_mode="HTML")
        elif stage == "config_password":
            config["password"] = text
            state["stage"] = "config_gender"
            await message.answer("<b>Setup Gender</b>\nEnter gender (M/F):", reply_markup=BACK_TO_CONFIG, parse_mode="HTML")
        elif stage == "config_gender":
            if text.upper() not in ("M", "F"):
                await message.answer("Invalid. Please enter M or F:", parse_mode="HTML")
                return True
            config["gender"] = text.upper()
            state["stage"] = "config_birth_year"
            await message.answer("<b>Setup Birth Year</b>\nEnter birth year (e.g., 2000):", reply_markup=BACK_TO_CONFIG, parse_mode="HTML")
        elif stage == "config_birth_year":
            try:
                year = int(text)
                if not 1950 <= year <= 2010:
                    raise ValueError()
                config["birth_year"] = year
                state["stage"] = "config_nationality"
                await message.answer(
                    "<b>Setup Nationality</b>\nEnter a 2-letter code (e.g., US, UK):",
                    reply_markup=BACK_TO_CONFIG,
                    parse_mode="HTML"
                )
            except ValueError:
                await message.answer("Invalid Year (1950-2010). Please try again:", parse_mode="HTML")
                return True
        elif stage == "config_nationality":
            if len(text) != 2:
                await message.answer("Invalid. Please enter a 2-letter code:", parse_mode="HTML")
                return True
            config["nationality"] = text.upper()
            state["stage"] = "menu"
            await message.answer("<b>Configuration Saved!</b>", parse_mode="HTML")
            await signup_settings_command(message)
        await set_signup_config(user_id, config)
    elif stage == "ask_num_accounts":
        try:
            num = int(text)
            if not 1 <= num <= 30:
                raise ValueError()
            state["num_accounts"] = num
            state["stage"] = "ask_name"
            await message.answer(
                "<b>Display Name</b>\nEnter the display name for the account(s):",
                reply_markup=BACK_TO_SIGNUP,
                parse_mode="HTML"
            )
        except ValueError:
            await message.answer("Invalid number (1-30). Please try again:", parse_mode="HTML")
            return True
    elif stage == "ask_name":
        state["name"] = text
        state["stage"] = "ask_photos"
        state["photos"] = []
        state["last_photo_message_id"] = None
        await message.answer(
            "<b>Profile Photos</b>\n\nSend up to 6 photos. Click 'Done' when finished.",
            reply_markup=DONE_PHOTOS,
            parse_mode="HTML"
        )
    elif stage == "ask_photos":
        if message.content_type != "photo":
            await message.answer("Please send a photo or click 'Done'.", reply_markup=DONE_PHOTOS, parse_mode="HTML")
            return True
        if len(state.get("photos", [])) >= 6:
            await message.answer("Photo limit reached (6). Click Done.", reply_markup=DONE_PHOTOS, parse_mode="HTML")
            return True
        photo_url = await upload_tg_photo(message)
        if photo_url:
            if "photos" not in state:
                state["photos"] = []
            state["photos"].append(photo_url)
            # Delete the previous photo message to keep the chat clean
            if state.get("last_photo_message_id"):
                try:
                    # Note: message.bot requires an aiogram Bot instance, which is typical
                    # but ensure your environment provides this context if running standalone.
                    await message.bot.delete_message(chat_id=user_id, message_id=state["last_photo_message_id"])
                except Exception as e:
                    logger.warning(f"Failed to delete previous photo message: {e}")
            # Send a new message with the updated count and Done button
            new_message = await message.answer(
                f"<b>Profile Photos</b>\n\nPhoto uploaded ({len(state['photos'])}/6). Send another or click 'Done'.",
                reply_markup=DONE_PHOTOS,
                parse_mode="HTML"
            )
            state["last_photo_message_id"] = new_message.message_id
        else:
            await message.answer("Upload Failed. Please try again.", reply_markup=DONE_PHOTOS, parse_mode="HTML")
    elif stage == "signin_email":
        state["signin_email"] = text
        state["stage"] = "signin_password"
        await message.answer(
            "<b>Password</b>\nEnter your password:",
            reply_markup=BACK_TO_SIGNUP,
            parse_mode="HTML"
        )
    elif stage == "signin_password":
        msg = await message.answer("<b>Signing In</b>...", parse_mode="HTML")
        # try_signin will generate/get device_info for this specific email
        res = await try_signin(state["signin_email"], text, user_id)
        if res.get("accessToken") and res.get("user"):
            creds = {"email": state["signin_email"], "password": text}
            await store_token_and_show_card(msg, res, creds)
        else:
            error_msg = res.get("errorMessage", "Unknown error.")
            await msg.edit_text(
                f"<b>Sign In Failed</b>\n\nError: {error_msg}",
                reply_markup=SIGNUP_MENU,
                parse_mode="HTML"
            )
        state["stage"] = "menu"
    else:
        return False
    
    user_signup_states[user_id] = state
    return True

async def upload_tg_photo(message: Message) -> Optional[str]:
    """Upload a Telegram photo to Meeff's server."""
    try:
        # Assuming message.bot is correctly configured in the bot environment
        file = await message.bot.get_file(message.photo[-1].file_id)
        file_url = f"https://api.telegram.org/file/bot{message.bot.token}/{file.file_path}"
        async with aiohttp.ClientSession() as session:
            async with session.get(file_url) as resp:
                if resp.status != 200:
                    return None
                return await meeff_upload_image(await resp.read())
    except Exception as e:
        logger.error(f"Error uploading Telegram photo: {e}")
        return None

async def meeff_upload_image(img_bytes: bytes) -> Optional[str]:
    """Upload an image to Meeff's S3 storage."""
    url = "https://api.meeff.com/api/upload/v1"
    payload = {"category": "profile", "count": 1, "locale": "en"}
    headers = {
        'User-Agent': "okhttp/5.1.0",
        'Accept-Encoding': "gzip",
        'Content-Type': "application/json; charset=utf-8"
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=json.dumps(payload), headers=headers) as resp:
                resp_json = await resp.json()
                data = resp_json.get("data", {})
                upload_info = data.get("uploadImageInfoList", [{}])[0]
                upload_url = data.get("Host")
                if not (upload_info and upload_url):
                    return None
                fields = {
                    k: upload_info.get(k) or data.get(k)
                    for k in ["X-Amz-Algorithm", "X-Amz-Credential", "X-Amz-Date", "Policy", "X-Amz-Signature"]
                }
                fields.update({
                    k: data.get(k)
                    for k in ["acl", "Content-Type", "x-amz-meta-uuid"]
                })
                fields["key"] = upload_info.get("key")
                if any(v is None for v in fields.values()):
                    return None
                form = aiohttp.FormData()
                for k, v in fields.items():
                    form.add_field(k, v)
                form.add_field('file', img_bytes, filename='photo.jpg', content_type='image/jpeg')
                async with session.post(upload_url, data=form) as s3resp:
                    return upload_info.get("uploadImagePath") if s3resp.status in (200, 204) else None
    except Exception as e:
        logger.error(f"Error uploading image to Meeff: {e}")
        return None

async def try_signup(state: Dict, telegram_user_id: int) -> Dict:
    """
    Attempt to sign up a new user with **throttling and robust error capture**.
    Now includes API init before signup for shadow ban prevention.
    """
    # CRITICAL: Introduce a randomized delay for throttling
    await asyncio.sleep(random.uniform(0.5, 1.5))

    # Step 1: Call API init before signup
    init_result = await call_api_init(state["email"], telegram_user_id)
    if "errorMessage" in init_result:
        logger.warning(f"API init failed for {state['email']}, continuing anyway: {init_result['errorMessage']}")

    # Step 2: Proceed with signup
    url = "https://api.meeff.com/user/register/email/v4"
    device_info = await get_or_create_device_info_for_email(telegram_user_id, state["email"])
    logger.warning(f"SIGN UP using Device ID: {device_info.get('device_unique_id')} for email {state['email']}")
    base_payload = {
        "providerId": state["email"],
        "providerToken": state["password"],
        "name": state["name"],
        "gender": state["gender"],
        "birthYear": state.get("birth_year", 2004),
        "nationalityCode": state.get("nationality", "US"),
        "description": state["desc"],
        "photos": "|".join(state.get("photos", [])) or DEFAULT_PHOTOS,
        "locale": "en",
        "color": "777777",
        "birthMonth": 3,
        "birthDay": 1,
        "languages": "en,es,fr",
        "levels": "5,1,1",
        "purpose": "PB000000,PB000001",
        "purposeEtcDetail": "",
        "interest": "IS000001,IS000002,IS000003,IS000004",
    }
    payload = get_api_payload_with_device_info(base_payload, device_info)
    headers = {'User-Agent': "okhttp/5.1.0", 'Content-Type': "application/json; charset=utf-8"}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, headers=headers) as response:
                if response.status != 200:
                    try:
                        resp_json = await response.json()
                        logger.error(f"Signup failed for {state['email']}: Status {response.status}, Error: {resp_json.get('errorMessage', 'Unknown')}")
                        return resp_json
                    except aiohttp.ContentTypeError:
                        error_text = await response.text()
                        logger.error(f"Signup failed for {state['email']}: Status {response.status}, Non-JSON Response: {error_text[:200]}")
                        return {"errorMessage": f"API Rejected Signup (Status {response.status}). Check full logs."}

                return await response.json()

    except Exception as e:
        logger.error(f"Error during signup for {state['email']}: {e}")
        return {"errorMessage": "Failed to register account due to connection error."}

async def call_blindmatch_login(email: str, telegram_user_id: int, token: str = None) -> Dict:
    """Call the blindmatch login endpoint."""
    url = "https://api.meeff.com/blindmatch/login/v2"
    device_info = await get_or_create_device_info_for_email(telegram_user_id, email)
    base_headers = {
        'User-Agent': "okhttp/5.1.0",
        'Accept-Encoding': "gzip",
        'Content-Type': "application/json; charset=utf-8"
    }
    if token:
        base_headers['meeff-access-token'] = token

    payload = {"locale": "en"}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, headers=base_headers) as response:
                if response.status == 200:
                    return await response.json()
                return {"errorMessage": f"Blindmatch login failed: {response.status}"}
    except Exception as e:
        logger.error(f"Blindmatch login error for {email}: {e}")
        return {"errorMessage": str(e)}

async def check_blocked_users(token: str, telegram_user_id: int) -> Dict:
    """Check if user is blocked by others."""
    url = "https://api.meeff.com/user/blockedbyuser/v1?locale=en"
    device_info = await get_or_create_device_info_for_token(telegram_user_id, token)
    base_headers = {
        'User-Agent': "okhttp/5.1.0",
        'Accept-Encoding': "gzip",
        'meeff-access-token': token
    }
    headers = get_headers_with_device_info(base_headers, device_info)

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    return await response.json()
                return {"errorMessage": f"Check blocked users failed: {response.status}"}
    except Exception as e:
        logger.error(f"Check blocked users error: {e}")
        return {"errorMessage": str(e)}

async def call_api_init(email: str, telegram_user_id: int) -> Dict:
    """Call the API init endpoint with device info."""
    url = "https://api.meeff.com/api/init/v2"
    device_info = await get_or_create_device_info_for_email(telegram_user_id, email)

    payload = {
        "platform": device_info["platform"],
        "version": device_info["app_version"],
        "locale": "en"
    }

    base_headers = {
        'User-Agent': "okhttp/5.1.0",
        'Accept-Encoding': "gzip",
        'Content-Type': "application/json; charset=utf-8"
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, headers=base_headers) as response:
                if response.status == 200:
                    return await response.json()
                return {"errorMessage": f"API init failed: {response.status}"}
    except Exception as e:
        logger.error(f"API init error for {email}: {e}")
        return {"errorMessage": str(e)}

async def try_signin(email: str, password: str, telegram_user_id: int) -> Dict:
    """
    Attempt to sign in with **throttling and robust error capture**.
    Now includes API init and blindmatch login for shadow ban prevention.
    """
    # CRITICAL: Introduce a randomized delay for throttling
    await asyncio.sleep(random.uniform(0.5, 1.5))

    # Step 1: Call API init
    init_result = await call_api_init(email, telegram_user_id)
    if "errorMessage" in init_result:
        logger.warning(f"API init failed for {email}, continuing anyway: {init_result['errorMessage']}")

    # Step 2: Main login
    url = "https://api.meeff.com/user/login/v4"
    device_info = await get_or_create_device_info_for_email(telegram_user_id, email)
    logger.warning(f"SIGN IN using Device ID: {device_info.get('device_unique_id')} for email {email}")
    base_payload = {"provider": "email", "providerId": email, "providerToken": password, "locale": "en"}
    payload = get_api_payload_with_device_info(base_payload, device_info)
    headers = {'User-Agent': "okhttp/5.1.0", 'Content-Type': "application/json; charset=utf-8"}

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, headers=headers) as response:
                if response.status != 200:
                    try:
                        resp_json = await response.json()
                        logger.error(f"Signin failed for {email}: Status {response.status}, Error: {resp_json.get('errorMessage', 'Unknown')}")
                        return resp_json
                    except aiohttp.ContentTypeError:
                        error_text = await response.text()
                        logger.error(f"Signin failed for {email}: Status {response.status}, Non-JSON Response: {error_text[:200]}")
                        return {"errorMessage": f"API Rejected Signin (Status {response.status}). Check full logs."}

                login_result = await response.json()

                # Step 3: Call blindmatch login after successful login
                if login_result.get("accessToken"):
                    blindmatch_result = await call_blindmatch_login(email, telegram_user_id, login_result["accessToken"])
                    if "errorMessage" in blindmatch_result:
                        logger.warning(f"Blindmatch login failed for {email}: {blindmatch_result['errorMessage']}")

                    # Step 4: Check blocked users
                    blocked_result = await check_blocked_users(login_result["accessToken"], telegram_user_id)
                    if "errorMessage" in blocked_result:
                        logger.warning(f"Check blocked users failed for {email}: {blocked_result['errorMessage']}")

                return login_result
    except Exception as e:
        logger.error(f"Error during signin for {email}: {e}")
        return {"errorMessage": "Failed to sign in due to connection error."}

async def store_token_and_show_card(msg_obj: Message, login_result: Dict, creds: Dict) -> None:
    """Store the access token and display the user card."""
    access_token = login_result.get("accessToken")
    user_data = login_result.get("user")
    if access_token and user_data:
        user_id = msg_obj.chat.id
        await set_token(user_id, access_token, user_data.get("name", creds.get("email")), creds.get("email"))
        user_data.update({
            "email": creds.get("email"),
            "password": creds.get("password"),
            "token": access_token
        })
        text = format_user_with_nationality(user_data)
        await set_info_card(user_id, access_token, text, creds.get("email"))
        await msg_obj.edit_text(
            "<b>Account Signed In & Saved!</b>\n\n" + text,
            parse_mode="HTML",
            disable_web_page_preview=True
        )
    else:
        error_msg = login_result.get("errorMessage", "Token or user data not received.")
        await msg_obj.edit_text(
            f"<b>Error</b>\n\nFailed to save account: {error_msg}",
            parse_mode="HTML"
        )
