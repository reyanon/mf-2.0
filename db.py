from pymongo import MongoClient
import datetime

# MongoDB connection
client = MongoClient("mongodb+srv://irexanon:xUf7PCf9cvMHy8g6@rexdb.d9rwo.mongodb.net/?retryWrites=true&w=majority&appName=RexDB")
db = client.meeff_bot

# Helper function to get a user's collection
def _get_user_collection(user_id):
    """Get the collection for a specific user"""
    print(f"User: {user_id}")  # Debug
    try:
        from main import CURRENT_CONNECTED_COLLECTION, ADMIN_USER_IDS
        print(f"CURRENT_CONNECTED_COLLECTION: {CURRENT_CONNECTED_COLLECTION}")  # Debug
        if user_id in ADMIN_USER_IDS and user_id in CURRENT_CONNECTED_COLLECTION:
            collection_name = CURRENT_CONNECTED_COLLECTION[user_id]
            print(f"Returning collection: {collection_name}")  # Debug
            return db[collection_name]
    except ImportError as e:
        print(f"Import error: {e}")  # Debug
    collection_name = f"user_{user_id}"
    print(f"Default collection: {collection_name}")  # Debug
    return db[collection_name]

# ... rest of db.py remains unchanged ...

# Helper function to ensure collection exists with basic structure
def _ensure_user_collection_exists(user_id):
    """Make sure user collection exists with default documents"""
    user_db = _get_user_collection(user_id)

    # Check if the collection is empty
    if user_db.count_documents({}) == 0:
        # Initialize with basic structure
        user_db.insert_one({"type": "metadata", "created_at": datetime.datetime.utcnow()})
        user_db.insert_one({"type": "tokens", "items": []})
        user_db.insert_one({"type": "settings", "current_token": None, "spam_filter": False})
        user_db.insert_one({"type": "sent_records", "data": {}})
        user_db.insert_one({"type": "filters", "data": {}})

# Set or update token for a user
def set_token(user_id, token, meeff_user_id, filters=None):
    _ensure_user_collection_exists(user_id)
    user_db = _get_user_collection(user_id)

    # Get current tokens
    tokens_doc = user_db.find_one({"type": "tokens"})
    tokens = tokens_doc.get("items", []) if tokens_doc else []

    # Check if token exists
    token_exists = False
    for i, t in enumerate(tokens):
        if t.get("token") == token:
            # Update existing token
            tokens[i]["name"] = meeff_user_id
            if filters:
                tokens[i]["filters"] = filters
            # Keep existing active status or default to True
            if "active" not in tokens[i]:
                tokens[i]["active"] = True
            token_exists = True
            break

    # Add new token if not exists
    if not token_exists:
        token_data = {"token": token, "name": meeff_user_id, "active": True}
        if filters:
            token_data["filters"] = filters
        tokens.append(token_data)

    # Update tokens in database
    user_db.update_one(
        {"type": "tokens"},
        {"$set": {"items": tokens}},
        upsert=True
    )

# Add new functions to toggle token active status
def toggle_token_status(user_id, token):
    _ensure_user_collection_exists(user_id)
    user_db = _get_user_collection(user_id)

    # Get current tokens
    tokens_doc = user_db.find_one({"type": "tokens"})
    if not tokens_doc:
        return False

    tokens = tokens_doc.get("items", [])
    status_changed = False

    # Find and toggle the token's active status
    for i, t in enumerate(tokens):
        if t.get("token") == token:
            current_status = t.get("active", True)
            tokens[i]["active"] = not current_status
            status_changed = True
            break

    if status_changed:
        # Update tokens in database
        user_db.update_one(
            {"type": "tokens"},
            {"$set": {"items": tokens}}
        )
        return True
    return False

# Add function to get active tokens only
def get_active_tokens(user_id):
    _ensure_user_collection_exists(user_id)
    user_db = _get_user_collection(user_id)

    tokens_doc = user_db.find_one({"type": "tokens"})
    if not tokens_doc:
        return []

    # Filter tokens that are active (or where active field doesn't exist)
    return [t for t in tokens_doc.get("items", []) if t.get("active", True)]

# Add function to get token status
def get_token_status(user_id, token):
    _ensure_user_collection_exists(user_id)
    user_db = _get_user_collection(user_id)

    tokens_doc = user_db.find_one({"type": "tokens"})
    if tokens_doc:
        for t in tokens_doc.get("items", []):
            if t.get("token") == token:
                return t.get("active", True)
    return None

# Get all tokens for a user
def get_tokens(user_id):
    _ensure_user_collection_exists(user_id)
    user_db = _get_user_collection(user_id)
    tokens_doc = user_db.find_one({"type": "tokens"})
    return tokens_doc.get("items", []) if tokens_doc else []

# List all tokens in the database
def list_tokens():
    result = []
    # Get all collection names
    collection_names = db.list_collection_names()

    # Filter for user collections
    user_collections = [name for name in collection_names if name.startswith("user_")]

    for collection_name in user_collections:
        user_id = int(collection_name.split("_")[1])  # Extract user_id from collection name
        user_db = db[collection_name]

        tokens_doc = user_db.find_one({"type": "tokens"})
        if tokens_doc:
            for token in tokens_doc.get("items", []):
                result.append({
                    "user_id": user_id,
                    "token": token.get("token"),
                    "name": token.get("name")
                })

    return result

# Set current account token for a user
def set_current_account(user_id, token):
    _ensure_user_collection_exists(user_id)
    user_db = _get_user_collection(user_id)

    user_db.update_one(
        {"type": "settings"},
        {"$set": {"current_token": token}},
        upsert=True
    )

# Get current account token for a user
def get_current_account(user_id):
    _ensure_user_collection_exists(user_id)
    user_db = _get_user_collection(user_id)

    settings = user_db.find_one({"type": "settings"})
    return settings.get("current_token") if settings else None

# Delete a token for a user
def delete_token(user_id, token):
    _ensure_user_collection_exists(user_id)
    user_db = _get_user_collection(user_id)

    # Get current tokens and filter out the one to delete
    tokens_doc = user_db.find_one({"type": "tokens"})
    if tokens_doc:
        tokens = tokens_doc.get("items", [])
        updated_tokens = [t for t in tokens if t.get("token") != token]

        # Update tokens in database
        user_db.update_one(
            {"type": "tokens"},
            {"$set": {"items": updated_tokens}}
        )

    # Check if this was the current token
    settings = user_db.find_one({"type": "settings"})
    if settings and settings.get("current_token") == token:
        user_db.update_one(
            {"type": "settings"},
            {"$set": {"current_token": None}}
        )

# Set filters for a specific user and token
def set_user_filters(user_id, token, filters):
    _ensure_user_collection_exists(user_id)
    user_db = _get_user_collection(user_id)

    # Get current tokens
    tokens_doc = user_db.find_one({"type": "tokens"})
    tokens = tokens_doc.get("items", []) if tokens_doc else []

    # Update the token's filters
    for i, t in enumerate(tokens):
        if t.get("token") == token:
            tokens[i]["filters"] = filters
            break

    # Update tokens in database
    user_db.update_one(
        {"type": "tokens"},
        {"$set": {"items": tokens}}
    )

# Get filters for a specific user and token
def get_user_filters(user_id, token):
    _ensure_user_collection_exists(user_id)
    user_db = _get_user_collection(user_id)

    tokens_doc = user_db.find_one({"type": "tokens"})
    if tokens_doc:
        for t in tokens_doc.get("items", []):
            if t.get("token") == token:
                return t.get("filters")
    return None

# Enable or disable spam filter for a user
def set_spam_filter(user_id, status: bool):
    _ensure_user_collection_exists(user_id)
    user_db = _get_user_collection(user_id)

    user_db.update_one(
        {"type": "settings"},
        {"$set": {"spam_filter": status}},
        upsert=True
    )

# Get spam filter status for a user
def get_spam_filter(user_id: int) -> bool:
    _ensure_user_collection_exists(user_id)
    user_db = _get_user_collection(user_id)

    settings = user_db.find_one({"type": "settings"})
    return settings.get("spam_filter", False) if settings else False

# Get all target IDs for a category for a user
def get_already_sent_ids(user_id, category):
    """Fetch all target_ids for a given user and category."""
    user_db = _get_user_collection(user_id)
    records_doc = user_db.find_one({"type": "sent_records"}, {"data." + category: 1})
    if records_doc and "data" in records_doc and category in records_doc["data"]:
        return set(records_doc["data"][category])
    return set()

# Record that we've sent a message/request to a target
def add_sent_id(user_id, category, target_id):
    """Record a target_id as sent for a user and category."""
    user_db = _get_user_collection(user_id)
    user_db.update_one(
        {"type": "sent_records"},
        {"$addToSet": {f"data.{category}": target_id}},
        upsert=True
    )

# Check if we've already sent to this target

async def is_already_sent(user_id, category, target_id, bulk=False):
    """Check if target_id(s) have already been recorded as sent"""
    user_db = _get_user_collection(user_id)
    
    if not bulk:
        # Single ID check
        records_doc = user_db.find_one({"type": "sent_records"}, {f"data.{category}": 1})
        if records_doc and "data" in records_doc and category in records_doc["data"]:
            sent_ids = records_doc["data"][category]
            if isinstance(sent_ids, (list, set)):
                return target_id in sent_ids
        return False
    else:
        # Bulk check - returns set of existing IDs
        records_doc = user_db.find_one({"type": "sent_records"}, {f"data.{category}": 1})
        if records_doc and "data" in records_doc and category in records_doc["data"]:
            existing_ids = records_doc["data"][category]
            if isinstance(existing_ids, (list, set)):
                return set(existing_ids)
        return set()

async def bulk_add_sent_ids(user_id, category, target_ids):
    """Record multiple target_ids as sent for a user and category"""
    if not target_ids:
        return
        
    user_db = _get_user_collection(user_id)
    user_db.update_one(
        {"type": "sent_records"},
        {"$addToSet": {f"data.{category}": {"$each": list(target_ids)}}},
        upsert=True
    )

async def has_valid_access(user_id):
    """Check if user has valid access to use the bot"""
    user_db = _get_user_collection(user_id)
    # Check if collection exists and has basic structure
    if user_db.count_documents({"type": "metadata"}) == 0:
        return False
    return True

def get_message_delay(user_id):
    # Return the delay in seconds for this user
    # You could store this in your database
    return 2  # Default 2 second delay



# Legacy functions for backward compatibility
def has_interacted(user_id, action_type, user_token):
    """Legacy function - checks a separate interactions collection"""
    interaction_record = db.interactions.find_one({
        "user_id": user_id,
        "action_type": action_type,
        "user_token": user_token
    })
    return interaction_record is not None

def log_interaction(user_id, action_type, user_token):
    """Legacy function - logs to a separate interactions collection"""
    interaction_data = {
        "user_id": user_id,
        "action_type": action_type,
        "user_token": user_token,
        "timestamp": datetime.datetime.utcnow()
    }
    db.interactions.insert_one(interaction_data)
