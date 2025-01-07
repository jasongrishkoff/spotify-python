import redis.asyncio as redis
import json
import time
from typing import Optional, Dict
import logging

logger = logging.getLogger(__name__)

class RedisCache:
    def __init__(self, host='localhost', port=6379, db=0):
        self.redis = redis.Redis(host=host, port=port, db=db, decode_responses=True)
        # Separate lock keys for different token types and discovered hash
        self._lock_keys = {
            'playlist': 'spotify_playlist_token_lock',
            'track': 'spotify_track_token_lock',
            'artist': 'spotify_artist_token_lock',
            'discovered': 'spotify_discovered_hash_lock'  # Add discovered hash lock
        }
        
    async def get_token(self, token_type: str = 'playlist') -> Optional[Dict]:
        """Get token data from Redis based on token type."""
        try:
            key = f'spotify_{token_type}_token'
            data = await self.redis.get(key)
            if data:
                return json.loads(data)
        except Exception as e:
            logger.error(f"Redis get error for {token_type} token: {e}")
        return None
        
    async def save_token(self, token: str, proxy: Dict, token_type: str = 'playlist', hash_value: Optional[str] = None) -> bool:
        """Save token data to Redis with appropriate type and hash."""
        try:
            proxy_data = proxy.copy()
            proxy_data['created_at'] = int(time.time())
            
            data = {
                'access_token': token,
                'proxy': proxy_data
            }
            
            if hash_value:
                data['hash_value'] = hash_value
                
            key = f'spotify_{token_type}_token'
            await self.redis.set(key, json.dumps(data), ex=3600)  # 1 hour expiry
            return True
        except Exception as e:
            logger.error(f"Redis save error for {token_type} token: {e}")
            return False

    async def get_discovered_hash(self) -> Optional[str]:
        """Get the discovered-on hash from Redis."""
        try:
            return await self.redis.get('spotify_discovered_hash')
        except Exception as e:
            logger.error(f"Redis get discovered hash error: {e}")
            return None

    async def save_discovered_hash(self, hash_value: str) -> bool:
        """Save the discovered-on hash to Redis with 24h expiry."""
        try:
            await self.redis.set('spotify_discovered_hash', hash_value, ex=86400)  # 24 hour expiry
            logger.info("Saved discovered hash to Redis")
            return True
        except Exception as e:
            logger.error(f"Redis save discovered hash error: {e}")
            return False

    async def acquire_lock(self, token_type: str = 'playlist', timeout: int = 10) -> bool:
        """Acquire lock for token refresh or discovered hash generation."""
        lock_key = self._lock_keys.get(token_type, 'spotify_token_lock')
        acquired = await self.redis.set(
            lock_key,
            'locked',
            ex=timeout,
            nx=True
        ) is not None
        if acquired:
            logger.debug(f"Acquired lock for {token_type}")
        return acquired
        
    async def release_lock(self, token_type: str = 'playlist'):
        """Release lock for token or discovered hash."""
        lock_key = self._lock_keys.get(token_type, 'spotify_token_lock')
        await self.redis.delete(lock_key)
        logger.debug(f"Released lock for {token_type}")

    async def clear_token(self, token_type: str = 'playlist'):
        """Clear a specific token type from Redis."""
        key = f'spotify_{token_type}_token'
        await self.redis.delete(key)
        
    async def clear_all_tokens(self):
        """Clear all token types and discovered hash from Redis."""
        keys = [
            'spotify_playlist_token', 
            'spotify_track_token', 
            'spotify_artist_token',
            'spotify_discovered_hash'
        ]
        if keys:
            await self.redis.delete(*keys)
