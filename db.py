from pymongo import MongoClient
import datetime

# MongoDB connection
client = MongoClient("mongodb+srv://irexanon:xUf7PCf9cvMHy8g6@rexdb.d9rwo.mongodb.net/?retryWrites=true&w=majority&appName=RexDB")
db = client.meeff_bot

# Helper function to get a user's collection and effective user_id
def _get_user_collection_and_id(user_id):
    """Get the collection and effective user_id for a specific user"""
    print(f"User: {user_id}")  # Debug
    try:
        from main import CURRENT_CONNECTED_COLLECTION, ADMIN_USER_IDS
        print(f"CURRENT_CONNECTED_COLLECTION: {CURRENT_CONNECTED_COLLECTION}")  # Debug
        if user_id in ADMIN_USER_IDS and user_id in CURRENT_CONNECTED_COLLECTION:
            collection_name = CURRENT_CONNECTED_COLLECTION[user_id]
            print(f"Returning collection: {collection_name}")  # Debug
            # Extract target_user_id from collection name (e.g., user_12345 -> 12345)
            target_user_id = int(collection_name.split("_")[1])
            return db[collection_name], target_user_id
    except ImportError as e:
        print(f"Import error: {e}")  # Debug
    collection_name = f"user_{user_id}"
    print(f"Default collection: {collection_name}")  # Debug
    return db[collection_name], user_id

# Helper function to ensure collection exists with basic structure
def _ensure_user_collection_exists(user_id):
    """Make sure user collection exists with default documents"""
    user_db, _ = _get_user_collection_and_id(user_id)

    # Check if the collection is empty
    if user_db.count_documents({}) == 0:
        user_db.insert_one({"type": "metadata", "created_at": datetime.datetime.utcnow()})
        user_db.insert_one({"type": "tokens", "items": []})
        user_db.insert_one({"type": "settings", "current_token": None, "spam_filter": False})
        user_db.insert_one({"type": "sent_records", "data": {}})
        user_db.insert_one({"type": "filters", "data": {}})

# Get all tokens for a user
def get_tokens(user_id):
    _ensure_user_collection_exists(user_id)
    user_db, effective_user_id = _get_user_collection_and_id(user_id)
    tokens_doc = user_db.find_one({"type": "tokens"})
    print(f"Fetching tokens for effective_user_id: {effective_user_id}, collection: {user_db.name}")  # Debug
    return tokens_doc.get("items", []) if tokens_doc else []

# Get active tokens only
def get_active_tokens(user_id):
    _ensure_user_collection_exists(user_id)
    user_db, effective_user_id = _get_user_collection_and_id(user_id)
    tokens_doc = user_db.find_one({"type": "tokens"})
    print(f"Fetching active tokens for effective_user_id: {effective_user_id}, collection: {user_db.name}")  # Debug
    if not tokens_doc:
        return []
    return [t for t in tokens_doc.get("items", []) if t.get("active", True)]

# Get current account token for a user
def get_current_account(user_id):
    _ensure_user_collection_exists(user_id)
    user_db, effective_user_id = _get_user_collection_and_id(user_id)
    settings = user_db.find_one({"type": "settings"})
    print(f"Fetching current account for effective_user_id: {effective_user_id}, collection: {user_db.name}")  # Debug
    return settings.get("current_token") if settings else None

# Set current account token for a user
def set_current_account(user_id, token):
    _ensure_user_collection_exists(user_id)
    user_db, effective_user_id = _get_user_collection_and_id(user_id)
    print(f"Setting current account for effective_user_id: {effective_user_id}, collection: {user_db.name}")  # Debug
    user_db.update_one(
        {"type": "settings"},
        {"$set": {"current_token": token}},
        upsert=True
    )

# Delete a token for a user
def delete_token(user_id, token):
    _ensure_user_collection_exists(user_id)
    user_db, effective_user_id = _get_user_collection_and_id(user_id)
    print(f"Deleting token for effective_user_id: {effective_user_id}, collection: {user_db.name}")  # Debug
    tokens_doc = user_db.find_one({"type": "tokens"})
    if tokens_doc:
        tokens = tokens_doc.get("items", [])
        updated_tokens = [t for t in tokens if t.get("token") != token]
        user_db.update_one(
            {"type": "tokens"},
            {"$set": {"items": updated_tokens}}
        )
    settings = user_db.find_one({"type": "settings"})
    if settings and settings.get("current_token") == token:
        user_db.update_one(
            {"type": "settings"},
            {"$set": {"current_token": None}}
        )

# Toggle token active status
def toggle_token_status(user_id, token):
    _ensure_user_collection_exists(user_id)
    user_db, effective_user_id = _get_user_collection_and_id(user_id)
    print(f"Toggling token status for effective_user_id: {effective_user_id}, collection: {user_db.name}")  # Debug
    tokens_doc = user_db.find_one({"type": "tokens"})
    if not tokens_doc:
        return False
    tokens = tokens_doc.get("items", [])
    status_changed = False
    for i, t in enumerate(tokens):
        if t.get("token") == token:
            current_status = t.get("active", True)
            tokens[i]["active"] = not current_status
            status_changed = True
            break
    if status_changed:
        user_db.update_one(
            {"type": "tokens"},
            {"$set": {"items": tokens}}
        )
        return True
    return False

# Get token status
def get_token_status(user_id, token):
    _ensure_user_collection_exists(user_id)
    user_db, effective_user_id = _get_user_collection_and_id(user_id)
    print(f"Checking token status for effective_user_id: {effective_user_id}, collection: {user_db.name}")  # Debug
    tokens_doc = user_db.find_one({"type": "tokens"})
    if tokens_doc:
        for t in tokens_doc.get("items", []):
            if t.get("token") == token:
                return t.get("active", True)
    return None

# Set filters for a specific user and token
def set_user_filters(user_id, token, filters):
    _ensure_user_collection_exists(user_id)
    user_db, effective_user_id = _get_user_collection_and_id(user_id)
    print(f"Setting filters for effective_user_id: {effective_user_id}, collection: {user_db.name}")  # Debug
    tokens_doc = user_db.find_one({"type": "tokens"})
    tokens = tokens_doc.get("items", []) if tokens_doc else []
    for i, t in enumerate(tokens):
        if t.get("token") == token:
            tokens[i]["filters"] = filters
            break
    user_db.update_one(
        {"type": "tokens"},
        {"$set": {"items": tokens}}
    )

# Get filters for a specific user and token
def get_user_filters(user_id, token):
    _ensure_user_collection_exists(user_id)
    user_db, effective_user_id = _get_user_collection_and_id(user_id)
    print(f"Getting filters for effective_user_id: {effective_user_id}, collection: {user_db.name}")  # Debug
    tokens_doc = user_db.find_one({"type": "tokens"})
    if tokens_doc:
        for t in tokens_doc.get("items", []):
            if t.get("token") == token:
                return t.get("filters")
    return None

# Enable or disable spam filter for a user
def set_spam_filter(user_id, status: bool):
    _ensure_user_collection_exists(user_id)
    user_db, effective_user_id = _get_user_collection_and_id(user_id)
    print(f"Setting spam filter for effective_user_id: {effective_user_id}, collection: {user_db.name}")  # Debug
    user_db.update_one(
        {"type": "settings"},
        {"$set": {"spam_filter": status}},
        upsert=True
    )

# Get spam filter status for a user
def get_spam_filter(user_id: int) -> bool:
    _ensure_user_collection_exists(user_id)
    user_db, effective_user_id = _get_user_collection_and_id(user_id)
    print(f"Getting spam filter for effective_user_id: {effective_user_id}, collection: {user_db.name}")  # Debug
    settings = user_db.find_one({"type": "settings"})
    return settings.get("spam_filter", False) if settings else False

# ... (other functions like get_already_sent_ids, add_sent_id, etc., remain unchanged unless they also need similar fixes)

# Update list_tokens to avoid user_id filtering issues
def list_tokens():
    result = []
    collection_names = db.list_collection_names()
    user_collections = [name for name in collection_names if name.startswith("user_")]
    for collection_name in user_collections:
        user_id = int(collection_name.split("_")[1])
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
