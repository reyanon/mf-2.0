# --------------------------------------------------------------
# Android device fingerprint generator for Meeff API calls
# --------------------------------------------------------------

import random
import string
from typing import Dict, Optional

# ------------------------------------------------------------------
# Helper – make MongoDB field names safe
# ------------------------------------------------------------------
def _sanitize_email_for_key(email: str) -> str:
    """Replace dots (invalid in MongoDB keys) with underscores."""
    return email.replace('.', '_')


# ------------------------------------------------------------------
# Device-ID & push-token generators
# ------------------------------------------------------------------
def generate_device_unique_id() -> str:
    """16-hex-character unique device identifier."""
    return ''.join(random.choices('0123456789abcdef', k=16))


def generate_push_token() -> str:
    """Meeff-style push token (11-char prefix + 70-char suffix)."""
    chars = string.ascii_letters + string.digits + '_-'
    part1 = ''.join(random.choices(chars, k=11))
    part2 = ''.join(random.choices(string.ascii_uppercase + string.digits, k=70))
    return f"{part1}:{part2}"


# ------------------------------------------------------------------
# Android device pools (real-world models seen in traffic)
# ------------------------------------------------------------------
DEVICE_BRANDS = [
    "Google", "Samsung", "Infinix", "OnePlus", "Xiaomi",
    "OPPO", "Vivo", "Realme", "Motorola", "Sony"
]

DEVICE_MODELS = [
    # Google Pixel
    "Pixel 9 Pro", "Pixel 9", "Pixel 8 Pro", "Pixel 8", "Pixel 7 Pro", "Pixel 7",
    # Samsung
    "Galaxy S24 Ultra", "Galaxy S24+", "Galaxy S24", "Galaxy S23 Ultra", "Galaxy S23",
    "Galaxy A55", "Galaxy A35",
    # Infinix (exact model from your screenshot)
    "Infinix X6858", "Infinix Note 40", "Infinix Zero 30",
    # Others
    "Redmi Note 13 Pro", "OnePlus 12", "OPPO Find X7", "Vivo X100", "Realme GT 6"
]

ANDROID_VERSIONS = ["15", "14", "13", "12"]          # Android 15 = API 35, etc.
APP_VERSIONS = ["6.7.1", "6.7.0"]                    # Exact versions from traffic


# ------------------------------------------------------------------
# Core generator – returns a dict with **every** field Meeff expects
# ------------------------------------------------------------------
def generate_device_info() -> Dict[str, str]:
    """Create a full Android device fingerprint."""
    brand   = random.choice(DEVICE_BRANDS)
    model   = random.choice(DEVICE_MODELS)
    os_ver  = random.choice(ANDROID_VERSIONS)
    app_ver = random.choice(APP_VERSIONS)

    # Some brands use a slightly different product string
    product = f"{model}-OP" if brand in ("Infinix", "OPPO", "Vivo", "Realme") else model

    # DISPLAY string – mimics the format seen in the official app
    build_num = random.randint(100, 999)
    patch     = random.randint(1, 99)
    display   = f"{model} {os_ver}.0.{build_num}SP{patch}"

    # Header value that goes into X-Device-Info
    device_header = f"{model}-Android{os_ver}-{app_ver}"

    return {
        # Payload fields
        "os":               f"Android v{os_ver}",
        "platform":         "android",
        "brand":            brand.upper(),
        "model":            model,
        "device":           f"{model}, DEVICE: {model.upper()}-OP",
        "product":          product,
        "display":          display,
        "appVersion":       app_ver,
        "deviceUniqueId":   generate_device_unique_id(),
        "pushToken":        generate_push_token(),
        "deviceLanguage":   "en",
        "deviceRegion":     "US",
        "simRegion":        random.choice(["US", "PK", "IN", "EU", "KR", "JP"]),
        "deviceGmtOffset":  random.choice(["+0500", "-0500", "+0000", "+0900", "-0800"]),
        "deviceRooted":     "0",
        "deviceEmulator":   "0",

        # Header helpers
        "device_info_header": device_header,
        "device_string": f"BRAND: {brand}, MODEL: {model}, DEVICE: {model}, PRODUCT: {product}"
    }


# ------------------------------------------------------------------
# Header / payload injectors (gzip added to match traffic)
# ------------------------------------------------------------------
def get_headers_with_device_info(base_headers: Dict[str, str],
                                 device_info: Dict[str, str]) -> Dict[str, str]:
    """Add X-Device-Info and gzip (exact match to captured traffic)."""
    headers = base_headers.copy()
    headers["X-Device-Info"] = device_info["device_info_header"]
    headers["accept-encoding"] = "gzip"
    return headers


def get_api_payload_with_device_info(base_payload: Dict,
                                     device_info: Dict[str, str]) -> Dict:
    """Inject every device field into a JSON payload."""
    payload = base_payload.copy()
    payload.update({
        "os":               device_info["os"],
        "platform":         device_info["platform"],
        "brand":            device_info["brand"],
        "model":            device_info["model"],
        "device":           device_info["device"],
        "product":          device_info["product"],
        "display":          device_info["display"],
        "appVersion":       device_info["appVersion"],
        "deviceUniqueId":   device_info["deviceUniqueId"],
        "pushToken":        device_info["pushToken"],
        "deviceLanguage":   device_info["deviceLanguage"],
        "deviceRegion":     device_info["deviceRegion"],
        "simRegion":        device_info["simRegion"],
        "deviceGmtOffset":  device_info["deviceGmtOffset"],
        "deviceRooted":     device_info["deviceRooted"],
        "deviceEmulator":   device_info["deviceEmulator"]
    })
    return payload


# ------------------------------------------------------------------
# Async DB helpers – unchanged from your original file
# ------------------------------------------------------------------
from db import _get_user_collection, _ensure_user_collection_exists

async def store_device_info_for_email(telegram_user_id: int,
                                      email: str,
                                      device_info: Dict[str, str]):
    await _ensure_user_collection_exists(telegram_user_id)
    sanitized = _sanitize_email_for_key(email)
    user_db = _get_user_collection(telegram_user_id)
    await user_db.update_one(
        {"type": "device_info"},
        {"$set": {f"data.{sanitized}": device_info}},
        upsert=True
    )


async def get_device_info_for_email(telegram_user_id: int,
                                   email: str) -> Optional[Dict[str, str]]:
    await _ensure_user_collection_exists(telegram_user_id)
    sanitized = _sanitize_email_for_key(email)
    user_db = _get_user_collection(telegram_user_id)
    doc = await user_db.find_one({"type": "device_info"})
    return doc.get("data", {}).get(sanitized) if doc else None


async def get_or_create_device_info_for_email(telegram_user_id: int,
                                              email: str) -> Dict[str, str]:
    info = await get_device_info_for_email(telegram_user_id, email)
    if not info:
        info = generate_device_info()
        await store_device_info_for_email(telegram_user_id, email, info)
    return info


async def store_device_info_for_token(telegram_user_id: int,
                                      token: str,
                                      device_info: Dict[str, str]):
    await _ensure_user_collection_exists(telegram_user_id)
    user_db = _get_user_collection(telegram_user_id)
    await user_db.update_one(
        {"type": "token_device_info"},
        {"$set": {f"data.{token}": device_info}},
        upsert=True
    )


async def get_device_info_for_token(telegram_user_id: int,
                                    token: str) -> Optional[Dict[str, str]]:
    await _ensure_user_collection_exists(telegram_user_id)
    user_db = _get_user_collection(telegram_user_id)
    doc = await user_db.find_one({"type": "token_device_info"})
    return doc.get("data", {}).get(token) if doc else None


async def get_or_create_device_info_for_token(telegram_user_id: int,
                                              token: str) -> Dict[str, str]:
    info = await get_device_info_for_token(telegram_user_id, token)
    if not info:
        info = generate_device_info()
        await store_device_info_for_token(telegram_user_id, token, info)
    return info
