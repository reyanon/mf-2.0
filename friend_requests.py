import asyncio
import aiohttp
import logging
import html
from aiogram import Bot, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from db import get_individual_spam_filter, bulk_add_sent_ids, get_active_tokens, get_current_account, get_already_sent_ids
from filters import apply_filter_for_account, is_request_filter_enabled
from collections import defaultdict
from dateutil import parser
from datetime import datetime, timezone
from device_info import get_or_create_device_info_for_token, get_headers_with_device_info

# Initialize logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Improved speed configuration with circuit breaker
PER_USER_DELAY = 0.3      # Reduced delay for better performance
PER_BATCH_DELAY = 0.8     # Reduced batch delay
EMPTY_BATCH_DELAY = 3     # Increased delay for empty batches
PER_ERROR_DELAY = 2       # Reduced error delay
MAX_RETRIES = 3           # Maximum retries per request
BATCH_SIZE = 20           # Process users in smaller batches
MAX_CONCURRENT_TOKENS = 10 # Limit concurrent token processing
TIMEOUT_SECONDS = 10      # Request timeout
CIRCUIT_BREAKER_THRESHOLD = 5  # Failures before circuit breaker opens

# Global state with better structure
user_states = defaultdict(lambda: {
    "running": False,
    "status_message_id": None,
    "pinned_message_id": None,
    "total_added_friends": 0,
    "batch_index": 0,
    "stopped": False,
    "last_update": datetime.now(),
    "error_count": 0,
    "circuit_breaker_open": False,
    "circuit_breaker_reset_time": None
})

# Rate limiting and circuit breaker
class CircuitBreaker:
    def __init__(self, failure_threshold=CIRCUIT_BREAKER_THRESHOLD, recovery_timeout=60):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time = None
        self.state = 'CLOSED'  # CLOSED, OPEN, HALF_OPEN

    def call(self, func):
        async def wrapper(*args, **kwargs):
            if self.state == 'OPEN':
                if self.last_failure_time and (datetime.now() - self.last_failure_time).seconds > self.recovery_timeout:
                    self.state = 'HALF_OPEN'
                else:
                    raise Exception("Circuit breaker is OPEN")

            try:
                result = await func(*args, **kwargs)
                if self.state == 'HALF_OPEN':
                    self.reset()
                return result
            except Exception as e:
                self.record_failure()
                raise e

        return wrapper

    def record_failure(self):
        self.failure_count += 1
        self.last_failure_time = datetime.now()
        if self.failure_count >= self.failure_threshold:
            self.state = 'OPEN'

    def reset(self):
        self.failure_count = 0
        self.state = 'CLOSED'
        self.last_failure_time = None

# Circuit breakers per user
circuit_breakers = defaultdict(lambda: CircuitBreaker())

# Inline keyboards
stop_markup = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="Stop Requests", callback_data="stop")]
])

async def fetch_users_with_retry(session, token, user_id, max_retries=MAX_RETRIES):
    """Fetch users with retry logic and timeout."""
    url = "https://api.meeff.com/user/explore/v2?lng=-112.0613784790039&unreachableUserIds=&lat=33.437198638916016&locale=en"
    
    device_info = await get_or_create_device_info_for_token(user_id, token)
    base_headers = {
        'User-Agent': "okhttp/4.12.0",
        'meeff-access-token': token
    }
    headers = get_headers_with_device_info(base_headers, device_info)
    
    for attempt in range(max_retries):
        try:
            timeout = aiohttp.ClientTimeout(total=TIMEOUT_SECONDS)
            async with session.get(url, headers=headers, timeout=timeout) as response:
                if response.status == 401:
                    logging.error(f"Token invalid: {token[:10]}...")
                    return None
                if response.status == 429:
                    wait_time = 2 ** attempt  # Exponential backoff
                    logging.warning(f"Rate limited, waiting {wait_time}s...")
                    await asyncio.sleep(wait_time)
                    continue
                if response.status != 200:
                    logging.error(f"HTTP {response.status}, attempt {attempt + 1}")
                    if attempt == max_retries - 1:
                        return []
                    await asyncio.sleep(1)
                    continue
                
                data = await response.json()
                return data.get("users", [])
                
        except asyncio.TimeoutError:
            logging.warning(f"Timeout on attempt {attempt + 1}")
            if attempt == max_retries - 1:
                return []
            await asyncio.sleep(1)
        except Exception as e:
            logging.error(f"Fetch error attempt {attempt + 1}: {e}")
            if attempt == max_retries - 1:
                return []
            await asyncio.sleep(1)
    
    return []

def format_user_optimized(user):
    """Optimized user formatting with better error handling."""
    def time_ago(dt_str):
        if not dt_str: 
            return "N/A"
        try:
            dt = parser.isoparse(dt_str)
            now = datetime.now(timezone.utc)
            diff = now - dt
            minutes = int(diff.total_seconds() // 60)
            if minutes < 1: return "just now"
            if minutes < 60: return f"{minutes} min ago"
            hours = minutes // 60
            if hours < 24: return f"{hours} hr ago"
            days = hours // 24
            return f"{days} day(s) ago"
        except Exception: 
            return "unknown"

    # Safe data extraction with defaults
    name = html.escape(str(user.get('name', 'N/A'))[:50])  # Limit length
    user_id = html.escape(str(user.get('_id', 'N/A')))
    nationality = html.escape(str(user.get('nationalityCode', 'N/A')))
    height = str(user.get('height', 'N/A'))
    
    if "|" in height:
        try:
            height_val, height_unit = height.split("|", 1)
            height = f"{height_val.strip()} {height_unit.strip()}"
        except:
            height = "N/A"
    height = html.escape(height)
    
    description = html.escape(str(user.get('description', 'N/A'))[:100])  # Limit length
    birth_year = html.escape(str(user.get('birthYear', 'N/A')))
    platform = html.escape(str(user.get('platform', 'N/A')))
    profile_score = html.escape(str(user.get('profileScore', 'N/A')))
    distance = html.escape(str(user.get('distance', 'N/A')))
    language_codes = html.escape(', '.join(user.get('languageCodes', [])[:3]))  # Limit languages
    last_active = time_ago(user.get("recentAt"))
        
    return (
        f"<b>Name:</b> {name}\n"
        f"<b>ID:</b> <code>{user_id}</code>\n"
        f"<b>Nationality:</b> {nationality}\n"
        f"<b>Height:</b> {height}\n"
        f"<b>Description:</b> {description}\n"
        f"<b>Birth Year:</b> {birth_year}\n"
        f"<b>Platform:</b> {platform}\n"
        f"<b>Profile Score:</b> {profile_score}\n"
        f"<b>Distance:</b> {distance} km\n"
        f"<b>Language Codes:</b> {language_codes}\n"
        f"<b>Last Active:</b> {last_active}"
    )

async def send_user_info_safe(bot, user_id, user_data, details):
    """Safely send user info with fallback options."""
    try:
        first_photo_url = user_data.get('photoUrls', [None])[0]
        
        if first_photo_url:
            # Try sending photo with timeout
            await asyncio.wait_for(
                bot.send_photo(
                    chat_id=user_id,
                    photo=first_photo_url,
                    caption=details,
                    parse_mode="HTML"
                ),
                timeout=5.0
            )
        else:
            await asyncio.wait_for(
                bot.send_message(
                    chat_id=user_id,
                    text=details,
                    parse_mode="HTML",
                    disable_web_page_preview=True
                ),
                timeout=5.0
            )
        return True
    except asyncio.TimeoutError:
        logging.warning("Message send timeout")
        return False
    except Exception as e:
        logging.error(f"Failed to send user info: {e}")
        return False

async def process_users_batch(session, users, token, user_id, bot, token_name, already_sent_ids, lock):
    """Process users in batches with improved error handling."""
    state = user_states[user_id]
    added_count = 0
    filtered_count = 0
    limit_reached = False
    
    is_spam_filter_enabled = await get_individual_spam_filter(user_id, "request")
    ids_to_persist = []
    device_info = await get_or_create_device_info_for_token(user_id, token)
    
    # Process in smaller batches to prevent hanging
    for i in range(0, len(users), BATCH_SIZE):
        if not state["running"]:
            break
            
        batch = users[i:i + BATCH_SIZE]
        semaphore = asyncio.Semaphore(5)  # Limit concurrent requests
        
        async def process_single_user(user):
            nonlocal added_count, filtered_count, limit_reached
            
            if not state["running"] or limit_reached:
                return
                
            async with semaphore:
                user_id_to_check = user["_id"]
                
                # Spam filter check
                if is_spam_filter_enabled:
                    async with lock:
                        if user_id_to_check in already_sent_ids:
                            filtered_count += 1
                            return
                        already_sent_ids.add(user_id_to_check)
                
                # Send friend request with circuit breaker
                url = f"https://api.meeff.com/user/undoableAnswer/v5/?userId={user_id_to_check}&isOkay=1"
                base_headers = {"meeff-access-token": token}
                headers = get_headers_with_device_info(base_headers, device_info)
                
                try:
                    circuit_breaker = circuit_breakers[user_id]
                    
                    @circuit_breaker.call
                    async def make_request():
                        timeout = aiohttp.ClientTimeout(total=TIMEOUT_SECONDS)
                        async with session.get(url, headers=headers, timeout=timeout) as response:
                            return await response.json()
                    
                    data = await make_request()
                    
                    if data.get("errorCode") == "LikeExceeded":
                        logging.info(f"Daily limit reached for {token_name}")
                        limit_reached = True
                        return
                    
                    if is_spam_filter_enabled:
                        ids_to_persist.append(user_id_to_check)
                    
                    # Send user info
                    details = format_user_optimized(user)
                    success = await send_user_info_safe(bot, user_id, user, details)
                    
                    if success:
                        added_count += 1
                        state["total_added_friends"] += 1
                    
                    await asyncio.sleep(PER_USER_DELAY)
                    
                except Exception as e:
                    logging.error(f"Error processing user {user_id_to_check}: {e}")
                    state["error_count"] += 1
                    await asyncio.sleep(PER_ERROR_DELAY)
        
        # Process batch concurrently
        tasks = [process_single_user(user) for user in batch]
        await asyncio.gather(*tasks, return_exceptions=True)
        
        # Check if we should stop due to errors or limits
        if limit_reached or state["error_count"] > 10:
            break
    
    # Persist spam filter IDs
    if is_spam_filter_enabled and ids_to_persist:
        try:
            await asyncio.wait_for(
                bulk_add_sent_ids(user_id, "request", ids_to_persist),
                timeout=5.0
            )
        except asyncio.TimeoutError:
            logging.warning("Timeout persisting spam filter IDs")
        except Exception as e:
            logging.error(f"Error persisting IDs: {e}")

    return limit_reached, added_count, filtered_count

async def update_status_safe(bot, user_id, message_id, text, markup=None):
    """Safely update status message with rate limiting."""
    state = user_states[user_id]
    now = datetime.now()
    
    # Rate limit status updates (max once per second)
    if state.get("last_update") and (now - state["last_update"]).total_seconds() < 1:
        return
    
    try:
        await asyncio.wait_for(
            bot.edit_message_text(
                chat_id=user_id,
                message_id=message_id,
                text=text,
                reply_markup=markup,
                parse_mode="HTML"
            ),
            timeout=3.0
        )
        state["last_update"] = now
    except asyncio.TimeoutError:
        logging.warning("Status update timeout")
    except Exception as e:
        if "message is not modified" not in str(e):
            logging.error(f"Status update failed: {e}")

async def run_requests_improved(user_id, bot, target_channel_id):
    """Improved main function with better error handling and stability."""
    state = user_states[user_id]
    state.update({
        "total_added_friends": 0, 
        "batch_index": 0, 
        "running": True, 
        "stopped": False,
        "error_count": 0,
        "last_update": None
    })
    
    token = await get_current_account(user_id)
    if not token:
        await update_status_safe(bot, user_id, state["status_message_id"], "No active account found.")
        state["running"] = False
        return

    tokens = await get_active_tokens(user_id)
    token_name = next((t.get("name", "Default") for t in tokens if t["token"] == token), "Default")
    
    already_sent_ids = await get_already_sent_ids(user_id, "request")
    lock = asyncio.Lock()
    
    connector = aiohttp.TCPConnector(limit=30, limit_per_host=10)
    timeout = aiohttp.ClientTimeout(total=30)
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        consecutive_empty_batches = 0
        
        while state["running"]:
            try:
                # Apply filters if enabled
                if is_request_filter_enabled(user_id):
                    await apply_filter_for_account(token, user_id)
                    await asyncio.sleep(1)
                
                # Update status
                await update_status_safe(
                    bot, user_id, state["status_message_id"],
                    f"{token_name}: Requests sent: {state['total_added_friends']}",
                    stop_markup
                )
                
                # Fetch users
                users = await fetch_users_with_retry(session, token, user_id)
                state["batch_index"] += 1
                
                if users is None:
                    await update_status_safe(
                        bot, user_id, state["status_message_id"],
                        f"{token_name}: Token is invalid (401 Unauthorized). Stopping."
                    )
                    state["running"] = False
                    break
                
                if not users:
                    consecutive_empty_batches += 1
                    logging.info(f"Empty batch {consecutive_empty_batches}/5 for batch {state['batch_index']}")
                    
                    if consecutive_empty_batches >= 5:
                        await update_status_safe(
                            bot, user_id, state["status_message_id"],
                            f"{token_name}: No more users found. Total: {state['total_added_friends']}"
                        )
                        state["running"] = False
                        break
                    
                    await asyncio.sleep(EMPTY_BATCH_DELAY)
                    continue
                
                consecutive_empty_batches = 0
                
                # Process users
                limit_reached, batch_added, batch_filtered = await process_users_batch(
                    session, users, token, user_id, bot, token_name, already_sent_ids, lock
                )
                
                if limit_reached:
                    await update_status_safe(
                        bot, user_id, state["status_message_id"],
                        f"{token_name}: Daily limit reached. Total: {state['total_added_friends']}"
                    )
                    state["running"] = False
                    break
                
                # Stop if too many errors
                if state["error_count"] > 20:
                    await update_status_safe(
                        bot, user_id, state["status_message_id"],
                        f"{token_name}: Too many errors. Stopping. Total: {state['total_added_friends']}"
                    )
                    state["running"] = False
                    break
                
                await asyncio.sleep(PER_BATCH_DELAY)
                
            except Exception as e:
                logging.error(f"Error in main loop: {e}")
                state["error_count"] += 1
                await asyncio.sleep(PER_ERROR_DELAY)
    
    # Cleanup
    if state.get("pinned_message_id"):
        try:
            await bot.unpin_chat_message(chat_id=user_id, message_id=state["pinned_message_id"])
        except Exception:
            pass
    
    status = "Stopped" if state.get("stopped") else "Completed"
    await bot.send_message(
        user_id, 
        f"✅ {status}! Total Added: {state.get('total_added_friends', 0)}"
    )

async def process_all_tokens_improved(user_id, tokens, bot, target_channel_id):
    """
    Processes all tokens using the "Passing the Baton" (distributed fetching) model.
    Accounts take turns fetching users and sorting them into nationality-specific queues.
    """
    # 1. ==================== SETUP ====================
    state = user_states[user_id]
    state.update({
        "total_added_friends": 0, "running": True, "stopped": False, "error_count": 0
    })

    status_message = await bot.send_message(chat_id=user_id, text="🔄 <b>AIO Team Setup...</b>", parse_mode="HTML", reply_markup=stop_markup)
    state["status_message_id"] = status_message.message_id
    
    try:
        await bot.pin_chat_message(chat_id=user_id, message_id=status_message.message_id, disable_notification=True)
        state["pinned_message_id"] = status_message.message_id
    except Exception as e:
        logging.error(f"Failed to pin message: {e}")

    # --- Shared Resources for the Team ---
    fetch_lock = asyncio.Lock() # The "baton" that only one account can hold to fetch users.
    consecutive_fetch_failures = 0 # Counter to stop if no new users are found.
    
    # Create a queue for each nationality filter, plus a "general" one.
    queues_by_nationality = {"general": asyncio.Queue(maxsize=200)}
    for t in tokens:
        nat_filter = t.get("nationality_filter")
        if nat_filter and nat_filter not in queues_by_nationality:
            queues_by_nationality[nat_filter] = asyncio.Queue(maxsize=100)

    # Load IDs from the database (for previous runs)
    db_sent_ids = await get_already_sent_ids(user_id, "request")
    # Track IDs processed in this session to prevent duplicate queueing
    processed_in_session = set() 
    
    token_status = {
        t["token"]: {"name": t.get("name", f"Acc {i+1}"), "added": 0, "status": "Queued"} 
        for i, t in enumerate(tokens)
    }

    # 2. ==================== THE WORKER LOGIC (Fetch & Add) ====================
    async def _worker(token_obj):
        nonlocal consecutive_fetch_failures
        token = token_obj["token"]
        name = token_status[token]["name"]
        my_nationality = token_obj.get("nationality_filter")
        my_queue_key = my_nationality if my_nationality else "general"
        my_queue = queues_by_nationality[my_queue_key]
        token_status[token]["status"] = "Starting"

        device_info = await get_or_create_device_info_for_token(user_id, token)
        base_headers = {"meeff-access-token": token}
        headers = get_headers_with_device_info(base_headers, device_info)

        async with aiohttp.ClientSession() as session:
            while state["running"]:
                try:
                    # -- Stage 1: Try to be a regular WORKER --
                    # Try to get a user from my assigned queue, but don't wait forever.
                    user = await asyncio.wait_for(my_queue.get(), timeout=2.0)

                    if user is None: # Shutdown signal
                        my_queue.put_nowait(None) # Put it back for others
                        break

                    token_status[token]["status"] = "Adding"
                    user_meeff_id = user["_id"]
                    
                    # Send friend request
                    url = f"https://api.meeff.com/user/undoableAnswer/v5/?userId={user_meeff_id}&isOkay=1"
                    async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as response:
                        data = await response.json()
                        if data.get("errorCode") == "LikeExceeded":
                            token_status[token]["status"] = "Limit Full"
                            my_queue.task_done()
                            return # This worker is done for the day

                        # On success, update UI and persist ID later
                        details = format_user_optimized(user)
                        if await send_user_info_safe(bot, user_id, user, details):
                            token_status[token]["added"] += 1
                            state["total_added_friends"] += 1

                    my_queue.task_done()
                    token_status[token]["status"] = "Waiting"
                    await asyncio.sleep(PER_USER_DELAY)

                except asyncio.TimeoutError:
                    # -- Stage 2: My queue is empty, try to become the SCOUT --
                    if fetch_lock.locked():
                        token_status[token]["status"] = "Queued..."
                        await asyncio.sleep(3) # Wait for the current scout to finish
                        continue
                    
                    # I am the first to notice, I will grab the baton!
                    async with fetch_lock:
                        # Double-check if someone else filled the queue while I was waiting for the lock.
                        if not my_queue.empty():
                            continue

                        token_status[token]["status"] = "Fetching..."
                        users = await fetch_users_with_retry(session, token, user_id)
                        
                        if not users:
                            consecutive_fetch_failures += 1
                            if consecutive_fetch_failures > len(tokens) * 2: # If we fail many times in a row
                                logging.warning("No new users found after many attempts. Shutting down.")
                                for q in queues_by_nationality.values():
                                    q.put_nowait(None) # Send shutdown signal to all workers
                            continue

                        # We found users! Reset failure counter and sort them.
                        consecutive_fetch_failures = 0
                        new_users_count = 0
                        for u in users:
                            uid = u["_id"]
                            if uid not in db_sent_ids and uid not in processed_in_session:
                                processed_in_session.add(uid)
                                user_nat = u.get("nationalityCode")
                                
                                # Sort user into the correct queue (bin)
                                if user_nat in queues_by_nationality:
                                    await queues_by_nationality[user_nat].put(u)
                                else:
                                    await queues_by_nationality["general"].put(u)
                                new_users_count += 1
                        logging.info(f"Team Fetch: {name} added {new_users_count} users to the queues.")

                except Exception as e:
                    logging.error(f"Worker {name} error: {e}")
                    token_status[token]["status"] = "Error"
                    await asyncio.sleep(PER_ERROR_DELAY)
                    
        token_status[token]["status"] = "Done"

# 3. ==================== UI REFRESHER ====================
    async def _refresh_ui_improved():
        last_message = ""
        while state["running"]:
            try:
                total_added_now = sum(status["added"] for status in token_status.values())
                
                q_summary_parts = []
                for nat, q in queues_by_nationality.items():
                    if q.qsize() > 0:
                        q_summary_parts.append(f"{nat.upper()}:{q.qsize()}")
                q_summary = " | ".join(q_summary_parts)
                
                header = f"🔄 <b>AIO Requests</b> | <b>Total Added:</b> {total_added_now}\n<pre>Queues: {q_summary}</pre>"
                
                # --- THIS IS THE PART THAT WAS CHANGED ---
                # New header for the new format
                lines = [header, ""] 
                
                for status in token_status.values():
                    name = status["name"]
                    # Format the name to a fixed 10-character length
                    display_name = name[:10].ljust(10) if len(name) <= 10 else name[:9] + '…'
                    
                    # New line format with fixed spacing
                    lines.append(f"<pre>{display_name}| sent : {status['added']:<4} | {status['status']}</pre>")
                # -----------------------------------------

                current_message = "\n".join(lines)
                if current_message != last_message:
                    await update_status_safe(bot, user_id, state["status_message_id"], current_message, stop_markup)
                    last_message = current_message
                
                await asyncio.sleep(1.5)
            except Exception as e:
                logging.error(f"UI refresh error: {e}")

    # 4. ==================== START & FINISH ====================
    ui_task = asyncio.create_task(_refresh_ui_improved())
    worker_tasks = [asyncio.create_task(_worker(td)) for td in tokens]
    
    await asyncio.gather(*worker_tasks, return_exceptions=True)

    # Cleanup
    state["running"] = False
    await asyncio.sleep(2)
    ui_task.cancel()
    
    if processed_in_session:
        logging.info(f"Persisting {len(processed_in_session)} new IDs to database.")
        await bulk_add_sent_ids(user_id, "request", list(processed_in_session))

    if state.get("pinned_message_id"):
        try: await bot.unpin_chat_message(chat_id=user_id, message_id=state["pinned_message_id"])
        except: pass

# Final Report
    total_added = sum(status["added"] for status in token_status.values())
    completion_status = "⚠️ Process Stopped" if state.get("stopped") else "✅ AIO Requests Completed"
    final_header = f"<b>{completion_status}</b> | <b>Total Added:</b> {total_added}"
    
    # --- UPDATE THIS PART AS WELL ---
    final_lines = [final_header, ""]
    for status in token_status.values():
        name = status["name"]
        display_name = name[:10].ljust(10) if len(name) <= 10 else name[:9] + '…'
        final_lines.append(f"<pre>{display_name}| sent : {status['added']:<4} | {status['status']}</pre>")
    # --------------------------------

    await update_status_safe(bot, user_id, state["status_message_id"], "\n".join(final_lines))
# Expose the improved functions for use in main.py
run_requests = run_requests_improved
process_all_tokens = process_all_tokens_improved
