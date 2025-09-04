import aiohttp
import json
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from db import set_token, get_tokens, set_info_card, get_info_card

def format_user_with_nationality(user):
    def time_ago(dt_str):
        if not dt_str:
            return "N/A"
        try:
            from dateutil import parser
            from datetime import datetime, timezone
            dt = parser.isoparse(dt_str)
            now = datetime.now(timezone.utc)
            diff = now - dt
            minutes = int(diff.total_seconds() // 60)
            if minutes < 1:
                return "just now"
            elif minutes < 60:
                return f"{minutes} min ago"
            hours = minutes // 60
            if hours < 24:
                return f"{hours} hr ago"
            days = hours // 24
            return f"{days} day(s) ago"
        except Exception:
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
        f"<b>🗣️ Languages:</b> {', '.join(user.get('languageCodes', []))}\n"
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

# Enhanced mobile-friendly keyboards
SIGNUP_MENU = InlineKeyboardMarkup(inline_keyboard=[
    [
        InlineKeyboardButton(text="🆕 Sign Up", callback_data="signup_go"),
        InlineKeyboardButton(text="🔐 Sign In", callback_data="signin_go")
    ],
    [InlineKeyboardButton(text="🔙 Back", callback_data="back_to_settings")]
])

VERIFY_BUTTON = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="✅ Verify Email", callback_data="signup_verify")],
    [InlineKeyboardButton(text="🔙 Back", callback_data="signup_menu")]
])

BACK_TO_SIGNUP = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="🔙 Back", callback_data="signup_menu")]
])

DONE_PHOTOS = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="✅ Done", callback_data="signup_photos_done")],
    [InlineKeyboardButton(text="🔙 Back", callback_data="signup_menu")]
])

user_signup_states = {}

DEFAULT_PHOTOS = (
    "https://meeffus.s3.amazonaws.com/profile/2025/06/16/20250616052423006_profile-1.0-bd262b27-1916-4bd3-9f1d-0e7fdba35268.jpg|"
    "https://meeffus.s3.amazonaws.com/profile/2025/06/16/20250616052438006_profile-1.0-349bf38c-4555-40cc-a322-e61afe15aa35.jpg"
)

async def check_email_exists(email):
    url = "https://api.meeff.com/user/checkEmail/v1"
    payload = {
        "email": email,
        "locale": "en"
    }
    headers = {
        'User-Agent': "okhttp/5.0.0-alpha.14",
        'Accept-Encoding': "gzip",
        'Content-Type': "application/json",
        'content-type': "application/json; charset=utf-8"
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload, headers=headers) as resp:
            status = resp.status
            try:
                resp_json = await resp.json()
            except Exception:
                resp_json = {}
            if status == 406 or resp_json.get("errorCode") == "AlreadyInUse":
                return False, resp_json.get("errorMessage", "This email address is already in use.")
            return True, ""

async def signup_command(message: Message):
    user_signup_states[message.chat.id] = {"stage": "menu"}
    await message.answer(
        "🎯 <b>Account Creation</b>\n\n"
        "Choose an option to get started:",
        reply_markup=SIGNUP_MENU,
        parse_mode="HTML"
    )

async def signup_callback_handler(callback: CallbackQuery):
    user_id = callback.from_user.id
    state = user_signup_states.get(user_id, {"stage": "menu"})
    
    if callback.data == "signup_go":
        state["stage"] = "ask_email"
        user_signup_states[user_id] = state
        await callback.message.edit_text(
            "📧 <b>Email Registration</b>\n\n"
            "Please enter your email address for registration:",
            reply_markup=BACK_TO_SIGNUP,
            parse_mode="HTML"
        )
        await callback.answer()
        return True
        
    if callback.data == "signup_menu":
        state["stage"] = "menu"
        await callback.message.edit_text(
            "🎯 <b>Account Creation</b>\n\n"
            "Choose an option to get started:",
            reply_markup=SIGNUP_MENU,
            parse_mode="HTML"
        )
        await callback.answer()
        return True
        
    if callback.data == "signin_go":
        state["stage"] = "signin_email"
        user_signup_states[user_id] = state
        await callback.message.edit_text(
            "🔐 <b>Sign In</b>\n\n"
            "Please enter your email address:",
            reply_markup=BACK_TO_SIGNUP,
            parse_mode="HTML"
        )
        await callback.answer()
        return True
        
    if callback.data == "signup_verify":
        creds = state.get("creds")
        if not creds:
            await callback.answer("❌ No signup info. Please sign up again.", show_alert=True)
            state["stage"] = "menu"
            await callback.message.edit_text(
                "🎯 <b>Account Creation</b>\n\n"
                "Choose an option to get started:",
                reply_markup=SIGNUP_MENU,
                parse_mode="HTML"
            )
            return True
            
        await callback.message.edit_text(
            "🔄 <b>Verifying Account</b>\n\n"
            "Checking verification and logging in...",
            reply_markup=None,
            parse_mode="HTML"
        )
        
        login_result = await try_signin(creds['email'], creds['password'])
        if login_result.get("accessToken"):
            await store_token_and_show_card(callback.message, login_result, creds)
        elif login_result.get("errorCode") == "NotVerified":
            await callback.message.edit_text(
                "⚠️ <b>Email Not Verified</b>\n\n"
                "Please check your inbox and click the verification link, then try again.",
                reply_markup=VERIFY_BUTTON,
                parse_mode="HTML"
            )
        else:
            await callback.message.edit_text(
                f"❌ <b>Login Failed</b>\n\n"
                f"Error: {login_result.get('errorMessage', 'Unknown error')}",
                reply_markup=SIGNUP_MENU,
                parse_mode="HTML"
            )
        return True
        
    if callback.data == "signup_photos_done":
        state["stage"] = "signup_submit"
        processing_msg = await callback.message.edit_text(
            "🔄 <b>Creating Account</b>\n\n"
            "Registering your account, please wait...",
            reply_markup=None,
            parse_mode="HTML"
        )
        
        signup_result = await try_signup(state)
        if signup_result.get("user", {}).get("_id"):
            state["creds"] = {
                "email": state["email"],
                "password": state["password"],
                "name": state["name"],
            }
            state["stage"] = "await_verify"
            await processing_msg.edit_text(
                "✅ <b>Account Created!</b>\n\n"
                "Please verify your email, then click the button below.",
                reply_markup=VERIFY_BUTTON,
                parse_mode="HTML"
            )
        else:
            err = signup_result.get("errorMessage", "Registration failed.")
            state["stage"] = "menu"
            await processing_msg.edit_text(
                f"❌ <b>Signup Failed</b>\n\n"
                f"Error: {err}",
                reply_markup=SIGNUP_MENU,
                parse_mode="HTML"
            )
        return True
        
    return False

async def signup_message_handler(message: Message):
    user_id = message.from_user.id
    if user_id not in user_signup_states:
        return False
    state = user_signup_states[user_id]
    if message.text and message.text.startswith("/"):
        return False

    # SIGNUP FLOW
    if state.get("stage") == "ask_email":
        email = message.text.strip()
        ok, msg = await check_email_exists(email)
        if not ok:
            await message.answer(
                f"❌ <b>Email Error</b>\n\n{msg}",
                reply_markup=BACK_TO_SIGNUP,
                parse_mode="HTML"
            )
            return True
        state["stage"] = "ask_password"
        state["email"] = email
        await message.answer(
            "🔐 <b>Password Setup</b>\n\n"
            "Enter a secure password for your account:",
            reply_markup=BACK_TO_SIGNUP,
            parse_mode="HTML"
        )
        return True
        
    if state.get("stage") == "ask_password":
        state["stage"] = "ask_name"
        state["password"] = message.text.strip()
        await message.answer(
            "👤 <b>Display Name</b>\n\n"
            "Enter your display name:",
            reply_markup=BACK_TO_SIGNUP,
            parse_mode="HTML"
        )
        return True
        
    if state.get("stage") == "ask_name":
        state["stage"] = "ask_gender"
        state["name"] = message.text.strip()
        await message.answer(
            "⚧️ <b>Gender Selection</b>\n\n"
            "Enter your gender (M/F):",
            reply_markup=BACK_TO_SIGNUP,
            parse_mode="HTML"
        )
        return True
        
    if state.get("stage") == "ask_gender":
        gender = message.text.strip().upper()
        if gender not in ("M", "F"):
            await message.answer(
                "⚠️ <b>Invalid Input</b>\n\n"
                "Please enter M or F for gender:",
                parse_mode="HTML"
            )
            return True
        state["stage"] = "ask_desc"
        state["gender"] = gender
        await message.answer(
            "📝 <b>Profile Description</b>\n\n"
            "Enter your profile description:",
            reply_markup=BACK_TO_SIGNUP,
            parse_mode="HTML"
        )
        return True
        
    if state.get("stage") == "ask_desc":
        state["stage"] = "ask_photos"
        state["desc"] = message.text.strip()
        state["photos"] = []
        await message.answer(
            "📸 <b>Profile Photos</b>\n\n"
            "Send up to 6 profile pictures one by one. Send each as a photo (not file).\n\n"
            "When done, click 'Done' or send /done.",
            reply_markup=DONE_PHOTOS,
            parse_mode="HTML"
        )
        return True
        
    if state.get("stage") == "ask_photos":
        if message.content_type == "photo":
            if len(state["photos"]) >= 6:
                await message.answer(
                    "📸 <b>Photo Limit Reached</b>\n\n"
                    "You have already sent 6 photos. Click Done to finish.",
                    parse_mode="HTML"
                )
                return True
                
            photo = message.photo[-1]
            file = await message.bot.get_file(photo.file_id)
            file_url = f"https://api.telegram.org/file/bot{message.bot.token}/{file.file_path}"
            
            async with aiohttp.ClientSession() as session:
                async with session.get(file_url) as resp:
                    img_bytes = await resp.read()
                    
            img_url = await meeff_upload_image(img_bytes)
            if img_url:
                state["photos"].append(img_url)
                await message.answer(
                    f"✅ <b>Photo Uploaded</b>\n\n"
                    f"Photos uploaded: {len(state['photos'])}/6",
                    parse_mode="HTML"
                )
            else:
                await message.answer(
                    "❌ <b>Upload Failed</b>\n\n"
                    "Failed to upload photo. Please try again.",
                    parse_mode="HTML"
                )
                
            if len(state["photos"]) == 6:
                await message.answer(
                    "📸 <b>All Photos Uploaded</b>\n\n"
                    "You've uploaded 6 photos. Click Done to finish.",
                    reply_markup=DONE_PHOTOS,
                    parse_mode="HTML"
                )
            return True
            
        elif message.text and message.text.strip().lower() == "/done":
            state["stage"] = "signup_submit"
            processing_msg = await message.answer(
                "🔄 <b>Creating Account</b>\n\n"
                "Registering your account, please wait...",
                reply_markup=None,
                parse_mode="HTML"
            )
            
            signup_result = await try_signup(state)
            if signup_result.get("user", {}).get("_id"):
                state["creds"] = {
                    "email": state["email"],
                    "password": state["password"],
                    "name": state["name"],
                }
                state["stage"] = "await_verify"
                await processing_msg.edit_text(
                    "✅ <b>Account Created!</b>\n\n"
                    "Please verify your email, then click the button below.",
                    reply_markup=VERIFY_BUTTON,
                    parse_mode="HTML"
                )
            else:
                err = signup_result.get("errorMessage", "Registration failed.")
                state["stage"] = "menu"
                await processing_msg.edit_text(
                    f"❌ <b>Signup Failed</b>\n\n"
                    f"Error: {err}",
                    reply_markup=SIGNUP_MENU,
                    parse_mode="HTML"
                )
            return True
        else:
            await message.answer(
                "📸 <b>Photo Required</b>\n\n"
                "Please send a photo or click Done to finish.",
                parse_mode="HTML"
            )
            return True

    # SIGNIN FLOW
    if state.get("stage") == "signin_email":
        state["stage"] = "signin_password"
        state["signin_email"] = message.text.strip()
        await message.answer(
            "🔐 <b>Password</b>\n\n"
            "Enter your password:",
            reply_markup=BACK_TO_SIGNUP,
            parse_mode="HTML"
        )
        return True
        
    if state.get("stage") == "signin_password":
        email = state["signin_email"]
        password = message.text.strip()
        processing_msg = await message.answer(
            "🔄 <b>Signing In</b>\n\n"
            "Please wait...",
            reply_markup=None,
            parse_mode="HTML"
        )
        
        login_result = await try_signin(email, password)
        if login_result.get("accessToken"):
            creds = {"email": email, "password": password}
            await store_token_and_show_card(processing_msg, login_result, creds)
            state["stage"] = "menu"
        else:
            err = login_result.get("errorMessage", "Sign in failed.")
            await processing_msg.edit_text(
                f"❌ <b>Sign In Failed</b>\n\n"
                f"Error: {err}",
                reply_markup=SIGNUP_MENU,
                parse_mode="HTML"
            )
            state["stage"] = "menu"
        return True

    return False

async def meeff_upload_image(img_bytes):
    url = "https://api.meeff.com/api/upload/v1"
    payload = {
        "category": "profile",
        "count": 1,
        "locale": "en"
    }
    headers = {
        'User-Agent': "okhttp/5.0.0-alpha.14",
        'Accept-Encoding': "gzip",
        'Content-Type': "application/json; charset=utf-8"
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=json.dumps(payload), headers=headers) as resp:
                resp_json = await resp.json()
                upload_info = resp_json.get("data", {}).get("uploadImageInfoList", [{}])[0]
                data = resp_json.get("data", {})
                if not upload_info or not data:
                    print("Meeff upload: missing upload_info or data.")
                    return None
                upload_url = data.get("Host")
                fields = {
                    "key": upload_info.get("key"),
                    "acl": data.get("acl"),
                    "Content-Type": data.get("Content-Type"),
                    "x-amz-meta-uuid": data.get("x-amz-meta-uuid"),
                    "X-Amz-Algorithm": upload_info.get("X-Amz-Algorithm") or data.get("X-Amz-Algorithm"),
                    "X-Amz-Credential": upload_info.get("X-Amz-Credential") or data.get("X-Amz-Credential"),
                    "X-Amz-Date": upload_info.get("X-Amz-Date") or data.get("X-Amz-Date"),
                    "Policy": upload_info.get("Policy") or data.get("Policy"),
                    "X-Amz-Signature": upload_info.get("X-Amz-Signature") or data.get("X-Amz-Signature"),
                }
                for k, v in fields.items():
                    if v is None:
                        print(f"Meeff S3 upload missing field: {k}")
                        return None
                form = aiohttp.FormData()
                for k, v in fields.items():
                    form.add_field(k, v)
                form.add_field('file', img_bytes, filename='photo.jpg', content_type='image/jpeg')
                async with session.post(upload_url, data=form) as s3resp:
                    if s3resp.status in (200, 204):
                        return upload_info.get("uploadImagePath")
                    else:
                        print(f"S3 upload failed: {s3resp.status} {await s3resp.text()}")
        return None
    except Exception as ex:
        print(f"Photo upload failed: {ex}")
        return None

async def try_signup(state):
    if state.get("photos"):
        photos_str = "|".join(state["photos"])
    else:
        photos_str = DEFAULT_PHOTOS
    url = "https://api.meeff.com/user/register/email/v4"
    payload = {
        "providerId": state["email"],
        "providerToken": state["password"],
        "os": "iOS 17.5.1",
        "platform": "ios",
        "device": "BRAND: Apple, MODEL: iPhone16,2, DEVICE: iPhone 15 Pro, PRODUCT: iPhone15Pro, DISPLAY: Super Retina XDR OLED, 6.1-inch",
        "pushToken": "dK_GLcrHTvSGxIbV7JCvuU:APA91bGenuineTokenForPushNotification9876543210",
        "deviceUniqueId": "78ef2140990fb55c",
        "deviceLanguage": "en",
        "deviceRegion": "US",
        "simRegion": "US",
        "deviceGmtOffset": "-0800",
        "deviceRooted": 0,
        "deviceEmulator": 0,
        "appVersion": "6.6.2",
        "name": state["name"],
        "gender": state["gender"],
        "color": "777777",
        "birthYear": 2004,
        "birthMonth": 3,
        "birthDay": 1,
        "nationalityCode": "US",
        "languages": "en,zh,ko,be,ru,uk",
        "levels": "5,1,1,1,1,1",
        "description": state["desc"],
        "photos": photos_str,
        "purpose": "PB000000,PB000001",
        "purposeEtcDetail": "",
        "interest": "IS000001,IS000002,IS000003,IS000004,IS000005,IS000006,IS000007,IS000008",
        "locale": "en"
    }
    headers = {
        'User-Agent': "okhttp/5.0.0-alpha.14",
        'Accept-Encoding': "gzip",
        'Content-Type': "application/json",
        'content-type': "application/json; charset=utf-8"
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload, headers=headers) as resp:
            return await resp.json()

async def try_signin(email, password):
    url = "https://api.meeff.com/user/login/v4"
    payload = {
        "provider": "email",
        "providerId": email,
        "providerToken": password,
        "os": "iOS 17.5.1",
        "platform": "ios",
        "device": "BRAND: Apple, MODEL: iPhone16,2, DEVICE: iPhone 15 Pro, PRODUCT: iPhone15Pro, DISPLAY: Super Retina XDR OLED, 6.1-inch",
        "pushToken": "dK_GLcrHTvSGxIbV7JCvuU:APA91bGenuineTokenForPushNotification9876543210",
        "deviceUniqueId": "78ef2140990fb55c",
        "deviceLanguage": "en",
        "deviceRegion": "US",
        "simRegion": "US",
        "deviceGmtOffset": "-0800",
        "deviceRooted": 0,
        "deviceEmulator": 0,
        "appVersion": "6.6.2",
        "locale": "en"
    }
    headers = {
        'User-Agent': "okhttp/5.0.0-alpha.14",
        'Accept-Encoding': "gzip",
        'Content-Type': "application/json",
        'content-type': "application/json; charset=utf-8"
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload, headers=headers) as resp:
            return await resp.json()

async def store_token_and_show_card(msg_obj, login_result, creds):
    access_token = login_result.get("accessToken")
    user_data = login_result.get("user")
    email = creds.get("email")
    
    if access_token:
        user_id = msg_obj.chat.id
        account_name = user_data.get("name") if user_data else creds.get("email")
        
        # Store token with email
        set_token(user_id, access_token, account_name, email)
        
        if user_data:
            user_data["email"] = creds.get("email")
            user_data["password"] = creds.get("password")
            user_data["token"] = access_token
            text = format_user_with_nationality(user_data)
            set_info_card(user_id, access_token, text, email)
            
            await msg_obj.edit_text(
                "✅ <b>Account Signed In!</b>\n\n"
                "Account successfully saved!\n\n" + text,
                parse_mode="HTML",
                disable_web_page_preview=True
            )
        else:
            await msg_obj.edit_text(
                "✅ <b>Account Signed In!</b>\n\n"
                "Account saved! (Email verification pending, full info not available yet.)",
                parse_mode="HTML"
            )
    else:
        await msg_obj.edit_text(
            "❌ <b>Error</b>\n\n"
            "Token not received, failed to save account.",
            parse_mode="HTML"
        )
