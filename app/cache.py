import redis.asyncio as redis
import json
import time
from typing import Optional, Dict
import logging

logger = logging.getLogger(__name__)

class RedisCache:
    def __init__(self, host='localhost', port=6379, db=0):
        self.redis = redis.Redis(host=host, port=port, db=db, decode_responses=True)
        self._lock_key = "spotify_token_lock"
        self._artist_lock_key = "spotify_artist_token_lock"
        
    async def get_token(self, token_type: str = 'playlist') -> Optional[Dict]:
        try:
            key = 'spotify_artist_token' if token_type == 'artist' else 'spotify_token'
            data = await self.redis.get(key)
            if data:
                return json.loads(data)
        except Exception as e:
            logger.error(f"Redis get error: {e}")
        return None
        
    async def save_token(self, token: str, proxy: Dict, token_type: str = 'playlist', artist_hash: Optional[str] = None) -> bool:
        try:
            # Ensure created_at is included in proxy data
            proxy_data = proxy.copy()
            proxy_data['created_at'] = int(time.time())
            
            data = {
                'access_token': token,
                'proxy': proxy_data
            }
            
            # Add artist hash if provided
            if artist_hash:
                data['artist_hash'] = artist_hash
                
            key = 'spotify_artist_token' if token_type == 'artist' else 'spotify_token'
            await self.redis.set(key, json.dumps(data), ex=3600)  # 1 hour expiry
            return True
        except Exception as e:
            logger.error(f"Redis save error: {e}")
            return False

    async def acquire_lock(self, token_type: str = 'playlist', timeout: int = 10) -> bool:
        """Acquire lock for token refresh"""
        lock_key = self._artist_lock_key if token_type == 'artist' else self._lock_key
        return await self.redis.set(
            lock_key,
            'locked',
            ex=timeout,
            nx=True
        ) is not None
        
    async def release_lock(self, token_type: str = 'playlist'):
        """Release token refresh lock"""
        lock_key = self._artist_lock_key if token_type == 'artist' else self._lock_key
        await self.redis.delete(lock_key)

    async def get_discovered_hash(self) -> Optional[str]:
        """Get the discovered-on hash from Redis"""
        try:
            hash_value = await self.redis.get('spotify_discovered_hash')
            if hash_value:
                return hash_value
        except Exception as e:
            logger.error(f"Redis get discovered hash error: {e}")
        return None

    async def save_discovered_hash(self, hash_value: str) -> bool:
        """Save the discovered-on hash to Redis"""
        try:
            await self.redis.set('spotify_discovered_hash', hash_value, ex=3600*24)  # 24 hour expiry
            return True
        except Exception as e:
            logger.error(f"Redis save discovered hash error: {e}")
            return False
