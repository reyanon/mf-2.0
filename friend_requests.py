import asyncio
import aiohttp
import logging
import html
from aiogram import Bot, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from db import get_spam_filter, is_already_sent, add_sent_id, get_active_tokens, get_current_account, get_already_sent_ids # Import the new function
from collections import defaultdict
import time

# Initialize logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


# ✅ Speed configuration 
PER_USER_DELAY = 0.5      # Delay between each user added
PER_BATCH_DELAY = 2       # Delay between batches
EMPTY_BATCH_DELAY = 2     # Delay after empty batch
PER_ERROR_DELAY = 5       # Delay after errors


# Global state variables for friend requests
user_states = defaultdict(lambda: {
    "running": False,
    "status_message_id": None,
    "pinned_message_id": None,
    "total_added_friends": 0,
    "batch_index": 0
})

# Inline keyboards for friend request operations
stop_markup = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="Stop Requests", callback_data="stop")]
])

async def fetch_users(session, token):
    """Fetch users from the API for friend requests"""
    url = "https://api.meeff.com/user/explore/v2?lng=-112.0613784790039&unreachableUserIds=&lat=33.437198638916016&locale=en"
    headers = {"meeff-access-token": token, "Connection": "keep-alive"}
    try:
        async with session.get(url, headers=headers) as response:
            if response.status == 429:
                logging.error("Request limit exceeded while fetching users.")
                return None
            if response.status != 200:
                logging.error(f"Failed to fetch users: {response.status}")
                return []
            return (await response.json()).get("users", [])
    except Exception as e:
        logging.error(f"Fetch users failed: {e}")
        return []

def format_user_details(user):
    """Format user details for display"""
    return (
        f"<b>User ID:</b> {html.escape(str(user['_id']))}\n"
        f"<b>Name:</b> {html.escape(user.get('name', 'N/A'))}\n"
        f"<b>Description:</b> {html.escape(user.get('description', 'N/A'))}\n"
        f"<b>Birth Year:</b> {html.escape(str(user.get('birthYear', 'N/A')))}\n"
        f"<b>Distance:</b> {html.escape(str(user.get('distance', 'N/A')))} km\n"
        f"<b>Language Codes:</b> {html.escape(', '.join(user.get('languageCodes', [])))}\n"
        "Photos: " + ' '.join([f"<a href='{html.escape(url)}'>Photo</a>" for url in user.get('photoUrls', [])])
    )

async def process_users(session, users, token, user_id, bot, target_channel_id):
    """Process a batch of users and send friend requests"""
    state = user_states[user_id]
    batch_added_friends = 0

    tokens = get_active_tokens(user_id)
    token_name = "Default Account"
    for token_obj in tokens:
        if token_obj["token"] == token:
            token_name = token_obj.get("name", "Default Account")
            break

    already_sent_ids = set()
    if get_spam_filter(user_id):
        already_sent_ids = get_already_sent_ids(user_id, "request")

    for user in users:
        if not state["running"]:
            break

        if get_spam_filter(user_id) and user["_id"] in already_sent_ids:
            continue

        url = f"https://api.meeff.com/user/undoableAnswer/v5/?userId={user['_id']}&isOkay=1"
        headers = {"meeff-access-token": token, "Connection": "keep-alive"}

        async with session.get(url, headers=headers) as response:
            data = await response.json()

            if data.get("errorCode") == "LikeExceeded":
                logging.info("Daily like limit reached.")
                await bot.edit_message_text(
                    chat_id=user_id,
                    message_id=state["status_message_id"],
                    text=f"{token_name}: Daily limit reached. Total Added Friends: {state['total_added_friends']}. Try again tomorrow.",
                    reply_markup=None
                )
                return True

        if get_spam_filter(user_id):
            add_sent_id(user_id, "request", user["_id"])

        details = format_user_details(user)
        await bot.send_message(chat_id=user_id, text=details, parse_mode="HTML")
        batch_added_friends += 1
        state["total_added_friends"] += 1

        if state["running"]:
            await bot.edit_message_text(
                chat_id=user_id,
                message_id=state["status_message_id"],
                text=f"{token_name}: Friend request sending: {state['total_added_friends']}",
                reply_markup=stop_markup
            )

        await asyncio.sleep(PER_USER_DELAY)

    return False



async def run_requests(user_id, bot, target_channel_id):
    """Main function to run the request process"""
    state = user_states[user_id]
    state["total_added_friends"] = 0  # Reset counter
    state["batch_index"] = 0
    
    async with aiohttp.ClientSession() as session:
        while state["running"]:
            try:
                # Get current token
                token = get_current_account(user_id)
                if not token:
                    await bot.edit_message_text(
                        chat_id=user_id,
                        message_id=state["status_message_id"],
                        text="No active account found. Please set an account before starting requests.",
                        reply_markup=None
                    )
                    state["running"] = False
                    return

                # Get token name
                tokens = get_active_tokens(user_id)
                token_name = "Default Account"
                for token_obj in tokens:
                    if token_obj["token"] == token:
                        token_name = token_obj.get("name", "Default Account")
                        break

                # Update status with token name - always try to update for live feedback
                try:
                    await bot.edit_message_text(
                        chat_id=user_id,
                        message_id=state["status_message_id"],
                        text=f"{token_name}: Friend request sending: {state['total_added_friends']}",
                        reply_markup=stop_markup
                    )
                except Exception as e:
                    # Ignore "message is not modified" errors
                    if "message is not modified" not in str(e):
                        logging.error(f"Error updating status message: {e}")

                # Fetch users
                try:
                    users = await fetch_users(session, token)
                    state["batch_index"] += 1
                    
                    if not users or len(users) == 0:
                        logging.info(f"No users found for batch {state['batch_index']}. Trying again...")
                        await asyncio.sleep(EMPTY_BATCH_DELAY)  # Wait a bit before trying again
                        
                        # After several attempts with no users, we might need to stop
                        if state["batch_index"] > 10:  # Try up to 3 empty batches before giving up
                            try:
                                await bot.edit_message_text(
                                    chat_id=user_id,
                                    message_id=state["status_message_id"],
                                    text=f"{token_name}: No more users found. Total Added: {state['total_added_friends']}",
                                    reply_markup=None
                                )
                            except Exception as e:
                                # Ignore "message is not modified" errors
                                if "message is not modified" not in str(e):
                                    logging.error(f"Error updating status message: {e}")
                            state["running"] = False
                            break
                        continue
                    
                    # Process users
                    limit_reached = await process_users(session, users, token, user_id, bot, target_channel_id)
                    if limit_reached:
                        # Rate limit reached
                        state["running"] = False
                        break
                        
                    # Apply delay after each user is added
                    await asyncio.sleep(PER_USER_DELAY)
                        
                except Exception as e:
                    logging.error(f"Error fetching users: {e}")
                    await bot.send_message(
                        chat_id=user_id,
                        text=f"Error fetching users: {str(e)[:200]}. Trying again..."
                    )
                    await asyncio.sleep(PER_ERROR_DELAY)  # Wait a bit longer after an error
                    continue
                        
                await asyncio.sleep(PER_BATCH_DELAY)  # Wait between batches
                
            except Exception as e:
                logging.error(f"Error during processing: {e}")
                try:
                    await bot.edit_message_text(
                        chat_id=user_id,
                        message_id=state["status_message_id"],
                        text=f"An error occurred: {str(e)[:200]}. Attempting to continue...",
                        reply_markup=stop_markup
                    )
                except Exception as edit_error:
                    # Ignore "message is not modified" errors
                    if "message is not modified" not in str(edit_error):
                        logging.error(f"Error updating status message: {edit_error}")
                await asyncio.sleep(PER_ERROR_DELAY)  # Wait a bit before continuing after an error
                 
        # Always try to unpin when finished
        try:
            if state.get("pinned_message_id"):
                await bot.unpin_chat_message(chat_id=user_id, message_id=state["pinned_message_id"])
                state["pinned_message_id"] = None
        except Exception as e:
            logging.error(f"Failed to unpin message: {e}")
            
        # Send completion message
        await bot.send_message(
            user_id,
            f"✅ All done!\nTotal Added: {state.get('total_added_friends', 0)}"
        )

async def process_all_tokens(user_id, tokens, bot, target_channel_id):
    """Process friend requests for all tokens concurrently"""
    state = user_states[user_id]
    state["total_added_friends"] = 0
    state["start_time"] = time.time()
    state["running"] = True
    state["stopped"] = False

    # Initialize status message
    if not state.get("status_message_id"):
        status_message = await bot.send_message(
            chat_id=user_id,
            text="🔄 <b>Initializing Friend Requests...</b>",
            parse_mode="HTML",
            reply_markup=stop_markup
        )
        state["status_message_id"] = status_message.message_id
        try:
            await bot.pin_chat_message(chat_id=user_id, message_id=status_message.message_id, disable_notification=True)
            state["pinned_message_id"] = status_message.message_id
        except Exception as e:
            logging.error(f"Failed to pin message: {e}")

    token_status = {}

    async def _worker(token_obj, idx):
        name = token_obj.get("name", f"Account {idx}")
        token = token_obj["token"]
        added_count = 0
        filtered_count = 0
        empty_batches = 0
        status = "Processing"

        token_status[name] = (added_count, filtered_count, status)

        try:
            async with aiohttp.ClientSession() as session:
                while state["running"]:
                    try:
                        users = await fetch_users(session, token)
                        logging.info(f"Fetched {len(users) if users else 'None'} users for {name}, empty_batches={empty_batches}")
                        if users is None:
                            token_status[name] = (added_count, filtered_count, "No more users")
                            return added_count
                        if not users or len(users) < 5:
                            empty_batches += 1
                            token_status[name] = (added_count, filtered_count, "Processing")
                            await asyncio.sleep(EMPTY_BATCH_DELAY * (2 ** empty_batches))  # Exponential backoff
                            if empty_batches >= 10:
                                token_status[name] = (added_count, filtered_count, "No more users")
                                return added_count
                            continue
                        empty_batches = 0

                        limit_reached = await process_users(session, users, token, user_id, bot, target_channel_id)
                        if limit_reached:
                            token_status[name] = (added_count, filtered_count, "Limit Exceeded")
                            return added_count

                        token_status[name] = (added_count, filtered_count, "Processing")
                        await asyncio.sleep(PER_BATCH_DELAY)

                    except Exception as e:
                        logging.error(f"Error processing {name}: {e}")
                        token_status[name] = (added_count, filtered_count, "Retry")
                        await asyncio.sleep(PER_ERROR_DELAY)

                token_status[name] = (added_count, filtered_count, "Stopped")
                return added_count

        except Exception as e:
            logging.error(f"Worker failed for {name}: {e}")
            token_status[name] = (added_count, filtered_count, f"Failed: {str(e)[:20]}...")
            return added_count

    async def _refresh_ui():
        last_message = ""
        update_count = 0
        update_interval = 2
        force_update_interval = 5

        while state["running"]:
            try:
                lines = [
                    "🔄 <b>Friend Requests AIO Status</b>\n",
                    "<pre>Account   │Added │Filter│Status</pre>",
                ]

                any_processing = False
                for name, (added, filtered, status) in token_status.items():
                    if status == "Processing" or "Retry" in status:
                        any_processing = True
                    display = name[:10] + '…' if len(name) > 10 else name.ljust(10)
                    lines.append(f"<pre>{display} │{added:>5} │{filtered:>6}│{status}</pre>")

                elapsed = time.time() - state.get("start_time", time.time())
                total_added = state.get("total_added_friends", 0)
                speed_per_min = (total_added / elapsed) * 60 if elapsed > 0 else 0

                lines.append(f"\n<b>Total Added:</b> {total_added} | <b>Speed:</b> {speed_per_min:.2f}/min")
                lines.append(f"<b>Elapsed:</b> {int(elapsed//60)}m {int(elapsed%60)}s")

                if not any_processing and not state["running"]:
                    lines.append(f"\n✅ <b>Done!</b>")
                else:
                    spinners = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
                    spinner = spinners[update_count % len(spinners)]
                    lines.append(f"\n{spinner} <i>Live update...</i>")

                current_message = "\n".join(lines)
                update_count += 1
                force_update = update_count % force_update_interval == 0

                if current_message != last_message or force_update:
                    try:
                        await bot.edit_message_text(
                            chat_id=user_id,
                            message_id=state["status_message_id"],
                            text=current_message,
                            parse_mode="HTML",
                            reply_markup=stop_markup
                        )
                        last_message = current_message
                    except Exception as e:
                        if "message is not modified" not in str(e):
                            logging.error(f"Status update failed: {e}")

            except Exception as e:
                logging.error(f"UI updater exception: {e}")

            await asyncio.sleep(update_interval)

    # Initialize status
    for idx, token_obj in enumerate(tokens, 1):
        name = token_obj.get("name", f"Account {idx}")
        token_status[name] = (0, 0, "Queued")

    await bot.edit_message_text(
        chat_id=user_id,
        message_id=state["status_message_id"],
        text="🔄 <b>Friend Requests AIO Starting...</b>",
        parse_mode="HTML",
        reply_markup=stop_markup
    )

    # Start UI updater and workers
    ui_task = asyncio.create_task(_refresh_ui())
    worker_tasks = [asyncio.create_task(_worker(token_obj, idx)) for idx, token_obj in enumerate(tokens, 1)]
    results = await asyncio.gather(*worker_tasks, return_exceptions=True)

    # Set state to not running
    state["running"] = False
    await asyncio.sleep(1)
    ui_task.cancel()
    try:
        await ui_task
    except asyncio.CancelledError:
        pass

    # Unpin message
    try:
        if state.get("pinned_message_id"):
            await bot.unpin_chat_message(chat_id=user_id, message_id=state["pinned_message_id"])
            state["pinned_message_id"] = None
    except Exception as e:
        logging.error(f"Unpin failed: {e}")

    # Final status
    total_added = sum(result for result in results if isinstance(result, int))
    total_filtered = sum(filtered for _, (added, filtered, _) in token_status.items())
    await bot.send_message(
        user_id,
        f"{'⚠️ Process stopped' if state['stopped'] else '✅ Friend requests completed'}!\nTotal Added: {total_added}\nTotal Filtered: {total_filtered}"
    )
