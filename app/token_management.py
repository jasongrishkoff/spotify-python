import asyncio
import logging
import time
import random
from fastapi import BackgroundTasks
from typing import Optional, Dict, List
from .spotify import SpotifyAPI, ProxyConfig

logger = logging.getLogger(__name__)

class TokenManager:
    """
    Manages token generation and refreshing in a non-blocking way
    """
    def __init__(self, spotify_api, redis_cache):
        self.spotify_api = spotify_api
        self.redis_cache = redis_cache
        self.token_types = ['playlist', 'artist', 'track']
        self.refresh_tasks = {}
        self.refresh_in_progress = {token_type: False for token_type in self.token_types}
        self.refresh_interval = 120  # 2 minutes
        self.token_validity = 240  # 4 minutes
        self._startup_complete = False
    
    async def initialize(self):
        """Initialize the token manager and ensure we have valid tokens"""
        logger.info("Initializing token manager")
        
        # Try to get tokens from cache first
        initial_tokens = {}
        for token_type in self.token_types:
            if cached := await self.redis_cache.get_token(token_type):
                initial_tokens[token_type] = True
                logger.info(f"Found cached {token_type} token")
        
        # For any missing tokens, refresh them sequentially
        for token_type in self.token_types:
            if token_type not in initial_tokens:
                logger.info(f"No cached {token_type} token found, refreshing")
                await self._refresh_token(token_type)
                # Add delay between token refreshes to avoid overloading resources
                await asyncio.sleep(2)
        
        # Also ensure discovered hash is available
        if not await self.redis_cache.get_discovered_hash():
            logger.info("No discovered hash found, generating")
            await self._refresh_discovered_hash()
        
        self._startup_complete = True
        logger.info("Token manager initialization complete")
    
    async def start_background_refreshing(self):
        """Start background refreshing tasks for all token types"""
        if not self._startup_complete:
            logger.warning("Attempting to start background refreshing before initialization")
            await self.initialize()
        
        for token_type in self.token_types:
            self._schedule_refresh(token_type)
        
        # Schedule discovered hash refresh
        self._schedule_discovered_hash_refresh()
        
        logger.info("Background token refreshing started")
    
    def _schedule_refresh(self, token_type: str):
        """Schedule a token refresh with jitter"""
        if token_type in self.refresh_tasks and not self.refresh_tasks[token_type].done():
            logger.debug(f"Refresh task for {token_type} already scheduled")
            return
        
        # Calculate next refresh time with jitter (±30 seconds)
        jitter = random.uniform(-30, 30)
        next_refresh = self.refresh_interval + jitter
        
        logger.debug(f"Scheduling {token_type} token refresh in {next_refresh:.1f} seconds")
        
        # Create and store the task
        self.refresh_tasks[token_type] = asyncio.create_task(
            self._delayed_refresh(token_type, next_refresh)
        )
    
    def _schedule_discovered_hash_refresh(self):
        """Schedule a discovered hash refresh"""
        if 'discovered_hash' in self.refresh_tasks and not self.refresh_tasks['discovered_hash'].done():
            return
        
        # Refresh discovered hash every 2 hours
        next_refresh = 7200 + random.uniform(-300, 300)  # 2 hours ± 5 minutes
        
        logger.debug(f"Scheduling discovered hash refresh in {next_refresh/3600:.1f} hours")
        
        self.refresh_tasks['discovered_hash'] = asyncio.create_task(
            self._delayed_discovered_hash_refresh(next_refresh)
        )
    
    async def _delayed_refresh(self, token_type: str, delay: float):
        """Wait for the specified delay, then refresh the token"""
        try:
            await asyncio.sleep(delay)
            await self._refresh_token(token_type)
        except Exception as e:
            logger.error(f"Error in delayed refresh for {token_type}: {str(e)}")
        finally:
            # Schedule the next refresh regardless of success/failure
            self._schedule_refresh(token_type)
    
    async def _delayed_discovered_hash_refresh(self, delay: float):
        """Wait for the specified delay, then refresh the discovered hash"""
        try:
            await asyncio.sleep(delay)
            await self._refresh_discovered_hash()
        except Exception as e:
            logger.error(f"Error in delayed discovered hash refresh: {str(e)}")
        finally:
            # Schedule the next refresh
            self._schedule_discovered_hash_refresh()
    
    async def _refresh_token(self, token_type: str) -> bool:
        """Refresh a specific token type with improved locking and error handling"""
        # Check if refresh is already in progress to avoid duplicate work
        if self.refresh_in_progress[token_type]:
            logger.debug(f"Refresh for {token_type} already in progress, skipping")
            return False
        
        self.refresh_in_progress[token_type] = True
        success = False
        
        try:
            # Increase lock timeout to prevent premature lock expiration during browser automation
            if await self.redis_cache.acquire_lock(token_type, timeout=120):
                try:
                    logger.info(f"Starting {token_type} token refresh")
                    start_time = time.time()
                    
                    # Double check if another process refreshed the token while we were waiting
                    if await self._check_token_validity(token_type):
                        #logger.info(f"Existing {token_type} token is now valid, skipping refresh")
                        success = True
                    else:
                        # Call the appropriate token refresh method
                        if token_type == 'track':
                            success = await self.spotify_api._get_track_token()
                        else:
                            success = await self.spotify_api._get_token(token_type)
                        
                        duration = time.time() - start_time
                        logger.info(f"{token_type} token refresh {'succeeded' if success else 'failed'} in {duration:.2f}s")
                finally:
                    # Always release the lock
                    await self.redis_cache.release_lock(token_type)
            else:
                # Instead of warning, log at info level to reduce noise
                logger.info(f"Could not acquire lock for {token_type} token refresh - another process is refreshing")
        except Exception as e:
            logger.error(f"Error during {token_type} token refresh: {str(e)}")
            # If we hit connection limits, add a delay before retrying
            if "Too many connections" in str(e):
                logger.info(f"Waiting 5 seconds due to connection limits")
                await asyncio.sleep(5)
        finally:
            self.refresh_in_progress[token_type] = False
        
        return success
    
    async def _refresh_discovered_hash(self) -> bool:
        """Refresh the discovered-on hash"""
        try:
            if await self.redis_cache.acquire_lock('discovered', timeout=60):
                try:
                    logger.info("Starting discovered hash refresh")
                    
                    # Check if we already have a valid hash
                    if await self.redis_cache.get_discovered_hash():
                        logger.info("Existing discovered hash is still valid, skipping refresh")
                        return True
                    
                    start_time = time.time()
                    discovered_hash = await self.spotify_api._get_discovered_hash()
                    
                    if discovered_hash:
                        await self.redis_cache.save_discovered_hash(discovered_hash)
                        duration = time.time() - start_time
                        logger.info(f"Discovered hash refresh succeeded in {duration:.2f}s")
                        return True
                    else:
                        logger.error("Failed to get discovered hash")
                        return False
                finally:
                    await self.redis_cache.release_lock('discovered')
            else:
                logger.warning("Could not acquire lock for discovered hash refresh")
                return False
        except Exception as e:
            logger.error(f"Error during discovered hash refresh: {str(e)}")
            return False
    
    async def _check_token_validity(self, token_type: str) -> bool:
        """
        Check if a token is still valid (not expired)
        Returns True if token is valid, False if invalid or close to expiry
        """
        if cached := await self.redis_cache.get_token(token_type):
            proxy_data = cached.get('proxy', {})
            created_at = proxy_data.get('created_at', 0)
            age = int(time.time()) - created_at
            
            # Token is valid if it's less than 75% of token_validity seconds old
            # This ensures we refresh tokens before they expire
            return age < (self.token_validity * 0.75)
        
        return False
    
    async def ensure_token(self, token_type: str) -> bool:
        """
        Ensure we have a valid token for the specified type.
        This is a non-blocking check that avoids triggering too many refreshes.
        """
        # First check if we have a valid token
        try:
            if await self._check_token_validity(token_type):
                return True
        except Exception as e:
            logger.error(f"Error checking token validity: {str(e)}")
            # If we can't check validity, assume token is valid to prevent cascading failures
            return True
        
        # If token is invalid but refresh is in progress, wait longer
        if self.refresh_in_progress[token_type]:
            #logger.info(f"{token_type} token invalid but refresh in progress, waiting briefly")
            for _ in range(10):  # Wait up to 2 seconds
                await asyncio.sleep(0.2)
                try:
                    if await self._check_token_validity(token_type):
                        return True
                except Exception:
                    continue
            
            # If still not valid after waiting, return whatever we have
            return True  # Return true to prevent cascading failures
        
        # Use a semaphore to limit concurrent refresh attempts
        if not hasattr(self, '_refresh_semaphores'):
            self._refresh_semaphores = {
                token_type: asyncio.Semaphore(1) for token_type in self.token_types
            }
            
        # Try to acquire the semaphore without blocking
        if self._refresh_semaphores[token_type].locked():
            logger.info(f"Another {token_type} refresh already triggered, using existing token")
            return True
            
        # Trigger at most one refresh task
        try:
            async with self._refresh_semaphores[token_type]:
                logger.warning(f"No valid {token_type} token, triggering background refresh")
                
                # Use a global limit on concurrent refreshes across all token types
                if not hasattr(self, '_global_refresh_semaphore'):
                    self._global_refresh_semaphore = asyncio.Semaphore(2)
                
                if self._global_refresh_semaphore.locked():
                    logger.info("Too many concurrent token refreshes, deferring")
                    return True
                
                async with self._global_refresh_semaphore:
                    refresh_task = asyncio.create_task(self._refresh_token(token_type))
                    
                    # Store the task to prevent it from being garbage collected
                    if not hasattr(self, '_pending_refreshes'):
                        self._pending_refreshes = []
                    self._pending_refreshes.append(refresh_task)
                    
                    # Cleanup completed tasks
                    self._pending_refreshes = [t for t in self._pending_refreshes if not t.done()]
        except Exception as e:
            logger.error(f"Error triggering token refresh: {str(e)}")
        
        # Use the existing token even if it's expired
        return True

# Modify SpotifyAPI class to use the TokenManager
class SpotifyAPIEnhanced(SpotifyAPI):
    """Enhanced SpotifyAPI class with non-blocking token management"""
    
    def __init__(self, db_path: str = 'spotify_cache.db'):
        super().__init__(db_path)
        self.token_manager = None  # Will be set later
    
    def set_token_manager(self, token_manager):
        """Set the token manager instance"""
        self.token_manager = token_manager
    
    async def _ensure_auth(self, token_type: str = 'playlist') -> bool:
        """
        Enhanced auth method that uses the token manager instead of
        blocking for token generation
        """
        if not self.token_manager:
            # Fall back to original implementation if token manager not set
            return await super()._ensure_auth(token_type)
        
        # Use token manager's non-blocking check
        if await self.token_manager.ensure_token(token_type):
            # Get the token and proxy from cache
            if cached := await self.cache.get_token(token_type):
                self.access_token = cached['access_token']
                proxy_data = cached['proxy']
                if isinstance(proxy_data, str):
                    import json
                    proxy_data = json.loads(proxy_data)
                self.proxy = ProxyConfig(**proxy_data)
                return True
        
        return False

# Enhanced initialization for FastAPI app
async def setup_token_management(app, spotify_api, redis_cache):
    """Setup enhanced token management for the app"""
    # Create TokenManager
    token_manager = TokenManager(spotify_api, redis_cache)
    
    # If SpotifyAPI is enhanced version, set the token manager
    if hasattr(spotify_api, 'set_token_manager'):
        spotify_api.set_token_manager(token_manager)
    
    # Initialize on startup
    @app.on_event("startup")
    async def initialize_token_manager():
        await token_manager.initialize()
        await token_manager.start_background_refreshing()
    
    return token_manager
