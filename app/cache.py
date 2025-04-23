import redis.asyncio as redis
import asyncio
import json
import time
import logging
from typing import Optional, Dict, List, Tuple

logger = logging.getLogger(__name__)

class RedisCache:
    def __init__(self, host='localhost', port=6379, db=0):
        # Use a single Redis connection with a connection pool
        self.redis = redis.Redis(
            host=host,
            port=port,
            db=db,
            decode_responses=True,
            max_connections=10,  # Significantly reduced 
            socket_timeout=5,
            socket_connect_timeout=5,
            retry_on_timeout=True
        )
        # Separate lock keys for different token types and discovered hash
        self._lock_keys = {
            'playlist': 'spotify_playlist_token_lock',
            'track': 'spotify_track_token_lock',
            'artist': 'spotify_artist_token_lock',
            'discovered': 'spotify_discovered_hash_lock'
        }
        # Add a global semaphore to limit Redis operations
        self._semaphore = asyncio.Semaphore(15)
        
    async def get_token(self, token_type: str = 'playlist', backup_index: int = 0) -> Optional[Dict]:
        """
        Get token data from Redis based on token type and backup index.
        
        Args:
            token_type (str): Type of token ('playlist', 'track', 'artist')
            backup_index (int): Backup index (0 = current, 1-3 = backups)
            
        Returns:
            Optional[Dict]: Token data or None if not found
        """
        async with self._semaphore:
            try:
                key = f'spotify_{token_type}_token_{backup_index}'
                data = await self.redis.get(key)
                if data:
                    return json.loads(data)
            except Exception as e:
                logger.error(f"Redis get error for {token_type} token (backup {backup_index}): {str(e)}")
                await asyncio.sleep(0.5)  # Add delay on error
            return None
        
    async def save_token(self, token: str, proxy: Dict, token_type: str = 'playlist', 
                        client_token: Optional[str] = None, 
                        health_metrics: Optional[Dict] = None) -> bool:
        """
        Save token data to Redis with error handling and backup rotation.
        
        Args:
            token (str): Access token
            proxy (Dict): Proxy configuration
            token_type (str): Type of token
            client_token (Optional[str]): Client token if available
            health_metrics (Optional[Dict]): Health metrics for the token
            
        Returns:
            bool: Success status
        """
        async with self._semaphore:
            try:
                # Rotate existing tokens (shift each one to the next backup slot)
                for i in range(2, -1, -1):  # Starting from 2 down to 0
                    old_key = f'spotify_{token_type}_token_{i}'
                    new_key = f'spotify_{token_type}_token_{i+1}'
                    old_data = await self.redis.get(old_key)
                    
                    # If we have data in this slot, move it to the next backup slot
                    if old_data:
                        await self.redis.set(new_key, old_data, ex=3600)  # 1 hour expiry for backups too
                
                # Prepare new token data
                proxy_data = proxy.copy() if isinstance(proxy, dict) else proxy
                proxy_data['created_at'] = int(time.time())
                
                data = {
                    'access_token': token,
                    'proxy': proxy_data
                }
                
                if health_metrics:
                    data['health_metrics'] = health_metrics
                    
                if client_token:
                    data['client_token'] = client_token
                
                # Save the new token as current (index 0)
                key = f'spotify_{token_type}_token_0'
                await self.redis.set(key, json.dumps(data), ex=3600)  # 1 hour expiry
                
                return True
            except Exception as e:
                logger.error(f"Redis save error for {token_type} token: {str(e)}")
                return False

    async def get_discovered_hash(self) -> Optional[str]:
        """Get the discovered-on hash from Redis."""
        async with self._semaphore:
            try:
                return await self.redis.get('spotify_discovered_hash')
            except Exception as e:
                logger.error(f"Redis get discovered hash error: {str(e)}")
                return None

    async def save_discovered_hash(self, hash_value: str) -> bool:
        """Save the discovered-on hash to Redis with 24h expiry."""
        async with self._semaphore:
            try:
                await self.redis.set('spotify_discovered_hash', hash_value, ex=86400)  # 24 hour expiry
                logger.info("Saved discovered hash to Redis")
                return True
            except Exception as e:
                logger.error(f"Redis save discovered hash error: {str(e)}")
                return False

    async def update_token_health(self, token_type: str, backup_index: int, success: bool) -> bool:
        """
        Update token health metrics.
        
        Args:
            token_type (str): Type of token
            backup_index (int): Backup index of the token
            success (bool): Whether the operation using the token succeeded
            
        Returns:
            bool: Success status
        """
        async with self._semaphore:
            try:
                key = f'spotify_{token_type}_token_{backup_index}'
                data_str = await self.redis.get(key)
                
                if not data_str:
                    return False
                
                data = json.loads(data_str)
                
                # Initialize health metrics if not present
                if 'health_metrics' not in data:
                    data['health_metrics'] = {
                        'success_count': 0,
                        'failure_count': 0,
                        'consecutive_failures': 0,
                        'last_used': int(time.time())
                    }
                
                # Update metrics
                if success:
                    data['health_metrics']['success_count'] += 1
                    data['health_metrics']['consecutive_failures'] = 0
                else:
                    data['health_metrics']['failure_count'] += 1
                    data['health_metrics']['consecutive_failures'] += 1
                
                data['health_metrics']['last_used'] = int(time.time())
                
                # Calculate success rate
                total = data['health_metrics']['success_count'] + data['health_metrics']['failure_count']
                if total > 0:
                    data['health_metrics']['success_rate'] = data['health_metrics']['success_count'] / total
                else:
                    data['health_metrics']['success_rate'] = 1.0  # Default to 100% for new tokens
                
                # Save updated data
                await self.redis.set(key, json.dumps(data), ex=3600)
                return True
                
            except Exception as e:
                logger.error(f"Error updating token health for {token_type} (backup {backup_index}): {str(e)}")
                return False

    async def promote_backup_token(self, token_type: str, backup_index: int) -> bool:
        """
        Promote a backup token to current position.
        
        Args:
            token_type (str): Type of token
            backup_index (int): Backup index to promote (1-3)
            
        Returns:
            bool: Success status
        """
        if backup_index <= 0 or backup_index > 3:
            logger.error(f"Invalid backup index {backup_index} for promotion")
            return False
            
        async with self._semaphore:
            try:
                # Get the backup token
                backup_key = f'spotify_{token_type}_token_{backup_index}'
                backup_data_str = await self.redis.get(backup_key)
                
                if not backup_data_str:
                    logger.error(f"No backup token found at index {backup_index}")
                    return False
                
                # Get current token to shift down
                current_key = f'spotify_{token_type}_token_0'
                current_data_str = await self.redis.get(current_key)
                
                # Save backup as current
                await self.redis.set(current_key, backup_data_str, ex=3600)
                
                # Move current to backup's position
                if current_data_str:
                    await self.redis.set(backup_key, current_data_str, ex=3600)
                else:
                    await self.redis.delete(backup_key)
                
                logger.info(f"Promoted {token_type} backup {backup_index} to current position")
                return True
                
            except Exception as e:
                logger.error(f"Error promoting backup token for {token_type}: {str(e)}")
                return False

    async def get_all_tokens(self, token_type: str) -> List[Dict]:
        """
        Get all tokens (current + backups) for a specific type.
        
        Args:
            token_type (str): Type of token
            
        Returns:
            List[Dict]: List of token data (index 0 is current, 1-3 are backups)
        """
        async with self._semaphore:
            result = []
            for i in range(4):  # 0 = current, 1-3 = backups
                if token_data := await self.get_token(token_type, i):
                    token_data['backup_index'] = i
                    result.append(token_data)
            return result

    async def acquire_lock(self, token_type: str = 'playlist', timeout: int = 10) -> bool:
        """Acquire lock with connection handling."""
        async with self._semaphore:
            lock_key = self._lock_keys.get(token_type, 'spotify_token_lock')
            
            # Try a few times with backoff
            for attempt in range(3):
                try:
                    acquired = await self.redis.set(
                        lock_key,
                        'locked',
                        ex=timeout,
                        nx=True
                    ) is not None
                    
                    if acquired:
                        logger.debug(f"Acquired lock for {token_type}")
                        return True
                        
                    if attempt < 2:  # Don't sleep on last attempt
                        await asyncio.sleep(0.2 * (attempt + 1))
                except Exception as e:
                    logger.warning(f"Redis connection error on lock attempt {attempt+1}: {str(e)}")
                    await asyncio.sleep(0.5)
                    
            return False
        
    async def release_lock(self, token_type: str = 'playlist'):
        """Release lock with error handling."""
        async with self._semaphore:
            lock_key = self._lock_keys.get(token_type, 'spotify_token_lock')
            try:
                await self.redis.delete(lock_key)
                logger.debug(f"Released lock for {token_type}")
            except Exception as e:
                logger.error(f"Error releasing lock for {token_type}: {str(e)}")

    async def clear_token(self, token_type: str = 'playlist', backup_index: Optional[int] = None):
        """
        Clear a specific token type from Redis.
        
        Args:
            token_type (str): Type of token
            backup_index (Optional[int]): If provided, clear only this backup index;
                                         otherwise clear all tokens of this type
        """
        async with self._semaphore:
            try:
                if backup_index is not None:
                    # Clear specific backup
                    key = f'spotify_{token_type}_token_{backup_index}'
                    await self.redis.delete(key)
                else:
                    # Clear all backups of this type
                    for i in range(4):  # 0 = current, 1-3 = backups
                        key = f'spotify_{token_type}_token_{i}'
                        await self.redis.delete(key)
            except Exception as e:
                logger.error(f"Error clearing token {token_type}: {str(e)}")
        
    async def clear_all_tokens(self):
        """Clear all token types and discovered hash from Redis."""
        async with self._semaphore:
            try:
                # Clear all token types and backups
                token_types = ['playlist', 'artist', 'track']
                keys = []
                
                for token_type in token_types:
                    for i in range(4):  # 0 = current, 1-3 = backups
                        keys.append(f'spotify_{token_type}_token_{i}')
                
                # Add discovered hash key
                keys.append('spotify_discovered_hash')
                
                if keys:
                    await self.redis.delete(*keys)
            except Exception as e:
                logger.error(f"Error clearing all tokens: {str(e)}")
