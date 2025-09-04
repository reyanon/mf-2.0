import aiohttp
import logging

ONLINE_STATUS_URL = "https://api.meeff.com/user/updateOnlineStatus/v1"
HEADERS = {
    'User-Agent': "okhttp/4.12.0",
    'Accept-Encoding': "gzip",
    'content-type': "application/json; charset=utf-8",
    'X-Device-Info': "iPhone15Pro-iOS17.5.1-6.6.2"
}

async def set_online_status(token, is_online=True):
    """Set user online/offline status"""
    headers = HEADERS.copy()
    headers['meeff-access-token'] = token
    
    payload = {
        "isOnline": is_online,
        "locale": "en"
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(ONLINE_STATUS_URL, json=payload, headers=headers, timeout=10) as response:
                if response.status == 200:
                    status_text = "online" if is_online else "offline"
                    logging.info(f"✅ Successfully set status to {status_text}")
                    return True
                else:
                    logging.error(f"❌ Failed to set online status: {response.status}")
                    response_text = await response.text()
                    logging.error(f"Response: {response_text}")
                    return False
    except Exception as e:
        logging.error(f"❌ Exception setting online status: {e}")
        return False

async def set_multiple_accounts_online(tokens):
    """Set multiple accounts online concurrently"""
    tasks = []
    for token_data in tokens:
        token = token_data.get("token") if isinstance(token_data, dict) else token_data
        tasks.append(set_online_status(token, True))
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    successful = sum(1 for result in results if result is True)
    
    logging.info(f"Set {successful}/{len(tokens)} accounts online")
    return successful

async def refresh_user_location(token, lat=-3.7895238, lng=-38.5327365):
    """Refresh user location to appear more active"""
    headers = HEADERS.copy()
    headers['meeff-access-token'] = token
    
    payload = {
        "latitude": lat,
        "longitude": lng,
        "locale": "en"
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                "https://api.meeff.com/user/updateLocation/v1", 
                json=payload, 
                headers=headers, 
                timeout=10
            ) as response:
                if response.status == 200:
                    logging.info("✅ Location refreshed successfully")
                    return True
                else:
                    logging.error(f"❌ Failed to refresh location: {response.status}")
                    return False
    except Exception as e:
        logging.error(f"❌ Exception refreshing location: {e}")
        return False