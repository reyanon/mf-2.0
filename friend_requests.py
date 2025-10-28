# friend_requests.py
# --------------------------------------------------------------
# Friend-request worker – ONLY endpoints added, everything else unchanged
# --------------------------------------------------------------

import asyncio
import aiohttp
import logging
import html
from aiogram import Bot
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from collections import defaultdict
from datetime import datetime, timezone
from dateutil import parser

# Local imports
from db import (
    get_individual_spam_filter, bulk_add_sent_ids,
    get_active_tokens, get_already_sent_ids
)
from filters import apply_filter_for_account, is_request_filter_enabled
from device_info import (
    get_or_create_device_info_for_token,
    get_headers_with_device_info
)

# ------------------------------------------------------------------
# Logging
# ------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ------------------------------------------------------------------
# Speed configuration (unchanged)
# ------------------------------------------------------------------
PER_USER_DELAY      = 0.5
PER_BATCH_DELAY     = 1
EMPTY_BATCH_DELAY   = 2
PER_ERROR_DELAY     = 5

# ------------------------------------------------------------------
# Global UI state
# ------------------------------------------------------------------
user_states = defaultdict(lambda: {
    "running": False,
    "status_message_id": None,
    "pinned_message_id": None,
    "total_added_friends": 0,
    "batch_index": 0,
    "stopped": False,
})

stop_markup = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="Stop Requests", callback_data="stop")]
])

# ------------------------------------------------------------------
# Helper – format user card
# ------------------------------------------------------------------
def format_user(user):
    def time_ago(dt_str):
        if not dt_str: return "N/A"
        try:
            dt = parser.isoparse(dt_str)
            now = datetime.now(timezone.utc)
            diff = now - dt
            mins = int(diff.total_seconds() // 60)
            if mins < 1: return "just now"
            if mins < 60: return f"{mins} min ago"
            hrs = mins // 60
            if hrs < 24: return f"{hrs} hr ago"
            days = hrs // 24
            return f"{days} day(s) ago"
        except Exception: return "unknown"

    last_active = time_ago(user.get("recentAt"))
    nationality = html.escape(user.get('nationalityCode', 'N/A'))
    height = html.escape(str(user.get('height', 'N/A')))
    if "|" in height:
        h, u = height.split("|", 1)
        height = f"{h.strip()} {u.strip()}"

    return (
        f"<b>Name:</b> {html.escape(user.get('name', 'N/A'))}\n"
        f"<b>ID:</b> <code>{html.escape(user.get('_id', 'N/A'))}</code>\n"
        f"<b>Nationality:</b> {nationality}\n"
        f"<b>Height:</b> {height}\n"
        f"<b>Description:</b> {html.escape(user.get('description', 'N/A'))}\n"
        f"<b>Birth Year:</b> {html.escape(str(user.get('birthYear', 'N/A')))}\n"
        f"<b>Platform:</b> {html.escape(user.get('platform', 'N/A'))}\n"
        f"<b>Profile Score:</b> {html.escape(str(user.get('profileScore', 'N/A')))}\n"
        f"<b>Distance:</b> {html.escape(str(user.get('distance', 'N/A')))} km\n"
        f"<b>Language Codes:</b> {html.escape(', '.join(user.get('languageCodes', [])))}\n"
        f"<b>Last Active:</b> {last_active}"
    )

# ------------------------------------------------------------------
# NEW: Session-initialisation helpers (exact traffic match)
# ------------------------------------------------------------------
async def _call_api_init(session: aiohttp.ClientSession, token: str):
    url = "https://api.meeff.com/api/init/v2"
    headers = {
        "meeff-access-token": token,
        "User-Agent": "okhttp/5.1.0",
        "Content-Type": "application/json; charset=utf-8",
        "accept-encoding": "gzip"
    }
    payload = {"platform": "android", "version": "6.7.1", "locale": "en"}
    try:
        async with session.post(url, json=payload, headers=headers) as resp:
            pass
    except Exception as e:
        logging.error(f"[INIT] error: {e}")

async def _call_blindmatch_login(session: aiohttp.ClientSession, token: str):
    url = "https://api.meeff.com/blindmatch/login/v2"
    headers = {
        "meeff-access-token": token,
        "User-Agent": "okhttp/5.1.0",
        "accept-encoding": "gzip"
    }
    payload = {"locale": "en"}
    try:
        async with session.post(url, json=payload, headers=headers) as resp:
            pass
    except Exception as e:
        logging.error(f"[BLINDMATCH] error: {e}")

async def _check_blocked_users(session: aiohttp.ClientSession, token: str, user_id: int):
    url = "https://api.meeff.com/user/blockedbyuser/v1?locale=en"
    device_info = await get_or_create_device_info_for_token(user_id, token)
    base_headers = {
        "meeff-access-token": token,
        "User-Agent": "okhttp/5.1.0",
        "accept-encoding": "gzip"
    }
    headers = get_headers_with_device_info(base_headers, device_info)
    try:
        async with session.get(url, headers=headers) as resp:
            pass
    except Exception as e:
        logging.error(f"[BLOCKED] error: {e}")

# ------------------------------------------------------------------
# Fetch users (explore endpoint)
# ------------------------------------------------------------------
async def fetch_users(session, token, user_id):
    url = "https://api.meeff.com/user/explore/v2?lng=-112.0613784790039&unreachableUserIds=&lat=33.437198638916016&locale=en"
    device_info = await get_or_create_device_info_for_token(user_id, token)
    base_headers = {
        "User-Agent": "okhttp/5.1.0",
        "meeff-access-token": token
    }
    headers = get_headers_with_device_info(base_headers, device_info)

    try:
        async with session.get(url, headers=headers) as resp:
            if resp.status == 401:
                logging.error(f"Token invalid (401) – {token[:8]}…")
                return None
            if resp.status == 429:
                logging.error("Rate-limited (429) while fetching users.")
                return None
            if resp.status != 200:
                logging.error(f"Fetch users failed: {resp.status}")
                return []
            data = await resp.json()
            return data.get("users", [])
    except Exception as e:
        logging.error(f"Fetch exception: {e}")
        return []

# ------------------------------------------------------------------
# Process a batch of users
# ------------------------------------------------------------------
async def process_users(session, users, token, user_id, bot, token_name,
                        already_sent_ids, lock):
    state = user_states[user_id]
    added = 0
    filtered = 0
    limit_reached = False

    is_spam_filter = await get_individual_spam_filter(user_id, "request")
    ids_to_persist = []
    device_info = await get_or_create_device_info_for_token(user_id, token)

    for user in users:
        if not state["running"]:
            break

        uid = user["_id"]

        # Spam-filter
        if is_spam_filter:
            async with lock:
                if uid in already_sent_ids:
                    filtered += 1
                    continue
                already_sent_ids.add(uid)

        await asyncio.sleep(PER_USER_DELAY)

        url = f"https://api.meeff.com/user/undoableAnswer/v5/?userId={uid}&isOkay=1"
        base_headers = {"meeff-access-token": token}
        headers = get_headers_with_device_info(base_headers, device_info)

        try:
            async with session.get(url, headers=headers) as resp:
                data = await resp.json()

                if data.get("errorCode") == "LikeExceeded":
                    logging.info(f"Daily like limit hit for {token_name}")
                    limit_reached = True
                    break

                # Persist for spam filter
                if is_spam_filter:
                    ids_to_persist.append(uid)

                # Send photo + caption
                details = format_user(user)
                photo_url = user.get("photoUrls", [None])[0]

                if photo_url:
                    await bot.send_photo(
                        chat_id=user_id,
                        photo=photo_url,
                        caption=details,
                        parse_mode="HTML"
                    )
                else:
                    await bot.send_message(
                        chat_id=user_id,
                        text=details,
                        parse_mode="HTML"
                    )

                added += 1
                state["total_added_friends"] += 1

        except Exception as e:
            logging.error(f"Request error for {uid}: {e}")

    # Bulk-persist spam IDs
    if ids_to_persist:
        await bulk_add_sent_ids(user_id, "request", ids_to_persist)

    return limit_reached, added, filtered

# ------------------------------------------------------------------
# Worker per token
# ------------------------------------------------------------------
async def _worker(token_obj, user_id, bot, session_sent_ids, lock):
    token = token_obj["token"]
    name = token_obj.get("name", f"Account {token[:8]}")
    empty_batches = 0

    async with aiohttp.ClientSession() as session:
        # ---- SESSION INITIALISATION (exact traffic order) ----
        await _call_api_init(session, token)
        await _call_blindmatch_login(session, token)
        await _check_blocked_users(session, token, user_id)

        while user_states[user_id]["running"]:
            try:
                # Apply any account-specific filter
                if is_request_filter_enabled(user_id):
                    await apply_filter_for_account(token, user_id)
                    await asyncio.sleep(1)

                users = await fetch_users(session, token, user_id)
                if users is None:                     # invalid token
                    break

                if not users or len(users) < 5:
                    empty_batches += 1
                    await asyncio.sleep(EMPTY_BATCH_DELAY)
                    if empty_batches >= 10:
                        logging.info(f"No more users for {name}")
                        break
                    continue

                empty_batches = 0
                limit_reached, batch_added, batch_filtered = await process_users(
                    session, users, token, user_id, bot, name,
                    session_sent_ids, lock
                )

                # Update UI counters
                token_status = user_states[user_id].setdefault("token_status", {})
                ts = token_status.setdefault(token, {"added": 0, "filtered": 0, "status": ""})
                ts["added"] += batch_added
                ts["filtered"] += batch_filtered
                ts["status"] = "Limit Full" if limit_reached else "Processing"

                if limit_reached:
                    break

                await asyncio.sleep(PER_BATCH_DELAY)

            except Exception as e:
                logging.error(f"Worker error ({name}): {e}")
                await asyncio.sleep(PER_ERROR_DELAY)

        # Final status
        ts = user_states[user_id]["token_status"].get(token, {})
        ts["status"] = "Stopped"

# ------------------------------------------------------------------
# Public entry point – **THIS IS WHAT main.py IMPORTS**
# ------------------------------------------------------------------
async def process_all_tokens(user_id, tokens, bot, target_channel_id,
                             initial_status_message=None):
    state = user_states[user_id]
    state.update({
        "running": True, "stopped": False,
        "total_added_friends": 0,
        "token_status": {}
    })

    # ---- UI message ----
    if initial_status_message:
        status_msg = initial_status_message
    else:
        status_msg = await bot.send_message(
            chat_id=user_id,
            text="AIO Starting…",
            parse_mode="HTML",
            reply_markup=stop_markup
        )
    state["status_message_id"] = status_msg.message_id
    await bot.pin_chat_message(chat_id=user_id,
                               message_id=status_msg.message_id,
                               disable_notification=True)
    state["pinned_message_id"] = status_msg.message_id

    # Spam-filter set
    session_sent_ids = await get_already_sent_ids(user_id, "request")
    lock = asyncio.Lock()

    # ---- UI refresh task ----
    async def _refresh_ui():
        last = ""
        while state["running"]:
            total = sum(s["added"] for s in state["token_status"].values())
            header = f"AIO Requests | <b>Added:</b> {total_total}"
            lines = [header, "", "<pre>Account   |Added |Filter|Status      </pre>"]
            for token, s in state["token_status"].items():
                disp = s.get("name", token[:10] + "…")[:10].ljust(10)
                lines.append(f"<pre>{disp} |{s['added']:>5} |{s['filtered']:>6}|{s['status']:<10}</pre>")
            cur = "\n".join(lines)
            if cur != last:
                try:
                    await bot.edit_message_text(
                        chat_id=user_id,
                        message_id=state["status_message_id"],
                        text=cur,
                        parse_mode="HTML",
                        reply_markup=stop_markup
                    )
                    last = cur
                except Exception as e:
                    if "not modified" not in str(e):
                        logging.error(f"UI update error: {e}")
            await asyncio.sleep(1)

    ui_task = asyncio.create_task(_refresh_ui())
    worker_tasks = [
        asyncio.create_task(_worker(tok, user_id, bot, session_sent_ids, lock))
        for tok in tokens
    ]

    await asyncio.gather(*worker_tasks, return_exceptions=True)

    # ---- Cleanup ----
    state["running"] = False
    await asyncio.sleep(1.1)
    ui_task.cancel()
    if state.get("pinned_message_id"):
        try:
            await bot.unpin_chat_message(chat_id=user_id,
                                         message_id=state["pinned_message_id"])
        except Exception:
            pass

    # ---- Final summary ----
    total = sum(s["added"] for s in state["token_status"].values())
    final = "Process Stopped" if state.get("stopped") else "AIO Completed"
    header = f"<b>{final}</b> | <b>Total Added:</b> {total}"
    lines = [header, "", "<pre>Account   |Added |Filter|Status      </pre>"]
    for token, s in state["token_status"].items():
        disp = s.get("name", token[:10] + "…")[:10].ljust(10)
        lines.append(f"<pre>{disp} |{s['added']:>5} |{s['filtered']:>6}|{s['status']}</pre>")

    await bot.edit_message_text(
        chat_id=user_id,
        message_id=state["status_message_id"],
        text="\n".join(lines),
        parse_mode="HTML"
    )
