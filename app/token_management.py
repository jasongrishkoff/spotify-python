# In app/token_management.py

import asyncio
import logging
import time
import random
import os
import json
from typing import Optional, Dict
from .spotify import SpotifyAPI, ProxyConfig
from fastapi_utils.tasks import repeat_every

logger = logging.getLogger(__name__)

class TokenManager:
    """
    Token manager that can operate in both leader and follower modes.
    
    In a multi-worker environment (Gunicorn), only one worker becomes the token 
    refresh leader while others operate as followers who only consume tokens.
    """
    
    def __init__(self, spotify_api, redis_cache):
        self.spotify_api = spotify_api
        self.redis_cache = redis_cache
        self.token_types = ['playlist', 'artist', 'track']
        self.refresh_tasks = {}
        self.is_leader = False
        self.leader_check_interval = 30  # Check leader status every 30 seconds
        self.instance_id = f"{os.getpid()}_{random.randint(1000, 9999)}"
        self.leader_key = "token_manager_leader"
        self.leader_lock_duration = 60  # 1 minute leadership lock
        self._startup_complete = False
        self.refresh_in_progress = {token_type: False for token_type in self.token_types}
        
        # Token refresh configuration
        self.refresh_interval = 240  # 4 minutes
        self.token_validity = 1800    # 30 minutes
        self.discovered_hash_refresh_interval = 1800  # 30 minutes 
        
        logger.info(f"Initializing token manager with instance ID: {self.instance_id}")

    async def initialize(self):
        """Initialize the token manager"""
        logger.info("Initializing token manager")
        
        # For each token type, check if we have a valid token
        for token_type in self.token_types:
            try:
                if cached := await self.redis_cache.get_token(token_type):
                    # Set the token in memory
                    self.spotify_api.access_token = cached['access_token']
                    
                    # Also set client token if available
                    if 'client_token' in cached:
                        self.spotify_api.client_token = cached['client_token']
                    
                    proxy_data = cached['proxy']
                    if isinstance(proxy_data, str):
                        import json
                        proxy_data = json.loads(proxy_data)
                    
                    self.spotify_api.proxy = ProxyConfig(**proxy_data)
                    logger.info(f"Loaded {token_type} token from cache")
                else:
                    logger.warning(f"No {token_type} token available in cache")
            except Exception as e:
                logger.error(f"Error initializing {token_type} token: {e}")
        
        # Initialize GraphQL hash manager
        if hasattr(self.spotify_api, 'hash_manager'):
            await self.spotify_api.hash_manager.initialize()
        
        self._startup_complete = True
        logger.info("Token manager initialization complete")

    async def start_background_refreshing(self):
        """
        Start background refreshing tasks.
        This maintains the original method name for backward compatibility.
        """
        await self.start()

    async def start(self):
        """Start the token manager"""
        if not self._startup_complete:
            await self.initialize()
        
        # Start background tasks
        # 1. Leadership election task
        asyncio.create_task(self.leadership_check_loop())
        
        logger.info("Token manager started")

    async def monitor_discovered_hash(self):
        """Proactively test and refresh the discovered hash if needed"""
        # Only run this if we're the leader
        if not self.is_leader:
            logger.debug("Not the leader, skipping discovered hash monitoring")
            return

        try:
            # Test current hash with a simple request
            test_artist_id = "3GBPw9NK25X1Wt2OUvOwY3"  # A reliable artist ID to test with

            # First check if we have a hash
            if not await self.redis_cache.get_discovered_hash():
                logger.warning("No discovered hash found, triggering refresh")
                await self.spotify_api._get_discovered_hash()
                return

            # Try to fetch artist data with current hash
            try:
                # Use a short timeout for test
                async with asyncio.timeout(5):
                    test_result = await self.spotify_api._fetch_discovered_on(test_artist_id)

                # Check if result is valid
                if not test_result or 'errors' in test_result:
                    logger.warning("Discovered hash test failed, triggering refresh")
                    # Delete old hash
                    await self.redis_cache.redis.delete('spotify_discovered_hash')
                    # Get new hash
                    await self.spotify_api._get_discovered_hash()
                else:
                    logger.debug("Discovered hash test successful")
            except Exception as e:
                logger.error(f"Error testing discovered hash: {e}")
                # Attempt refresh if test fails
                if self.is_leader:  # Double-check we're still the leader
                    await self.spotify_api._get_discovered_hash()

        except Exception as e:
            logger.error(f"Error in discovered hash monitoring: {e}")

    async def leadership_check_loop(self):
        """Periodically check and attempt to become the leader"""
        logger.info("Starting leadership check loop")
        
        while True:
            try:
                # Check if there's a current leader
                current_leader = await self.redis_cache.redis.get(self.leader_key)
                
                # Use a more reliable approach to determine whether we're the leader
                current_leader_str = None
                if current_leader is not None:
                    if isinstance(current_leader, bytes):
                        current_leader_str = current_leader.decode('utf-8')
                    else:
                        current_leader_str = str(current_leader)
                
                is_now_leader = False
                
                # No leader exists
                if current_leader_str is None:
                    # Try to claim leadership with NX option
                    result = await self.redis_cache.redis.set(
                        self.leader_key,
                        self.instance_id,
                        ex=self.leader_lock_duration,
                        nx=True
                    )
                    is_now_leader = result is not None and result  # Check Redis returned True for NX set
                    
                    if is_now_leader:
                        logger.info(f"Instance {self.instance_id} claimed leadership (no previous leader)")
                
                # We're already the leader, just renew
                elif current_leader_str == self.instance_id:
                    # Renew our leadership
                    await self.redis_cache.redis.set(
                        self.leader_key,
                        self.instance_id,
                        ex=self.leader_lock_duration
                    )
                    is_now_leader = True
                
                # Leadership state change detection
                if is_now_leader and not self.is_leader:
                    logger.info(f"Instance {self.instance_id} became the token refresh leader")
                    self.is_leader = True
                    self._start_refresh_tasks()
                    
                    # Schedule the first hash monitoring after becoming leader
                    asyncio.create_task(self.monitor_discovered_hash())
                    
                    # Log current state of tokens
                    for token_type in self.token_types:
                        if cached := await self.redis_cache.get_token(token_type):
                            proxy_data = cached['proxy']
                            created_at = proxy_data.get('created_at', 0) if isinstance(proxy_data, dict) else 0
                            age = int(time.time()) - created_at
                            logger.info(f"Current {token_type} token age: {age}s")
                elif not is_now_leader and self.is_leader:
                    logger.info(f"Instance {self.instance_id} is no longer the token refresh leader")
                    self.is_leader = False
                    self._stop_refresh_tasks()
                    
            except Exception as e:
                logger.error(f"Error in leadership check: {e}")
                
            # Check every 30 seconds
            await asyncio.sleep(self.leader_check_interval)

    def _start_refresh_tasks(self):
        """Start token refresh tasks when becoming the leader"""
        logger.info("Starting token refresh tasks")
        
        # Start token refresh tasks
        for token_type in self.token_types:
            if token_type not in self.refresh_tasks or self.refresh_tasks[token_type].done():
                self.refresh_tasks[token_type] = asyncio.create_task(
                    self._token_refresh_loop(token_type)
                )
                
        # Start discovered hash refresh task
        if 'discovered_hash' not in self.refresh_tasks or self.refresh_tasks['discovered_hash'].done():
            self.refresh_tasks['discovered_hash'] = asyncio.create_task(
                self._discovered_hash_refresh_loop()
            )

    def _stop_refresh_tasks(self):
        """Stop token refresh tasks when leadership is lost"""
        logger.info("Stopping token refresh tasks")
        
        for task_name, task in self.refresh_tasks.items():
            if not task.done():
                logger.info(f"Cancelling {task_name} refresh task")
                task.cancel()

    async def _token_refresh_loop(self, token_type):
        """Background task to refresh a specific token type"""
        logger.info(f"Starting {token_type} token refresh loop")
        
        while self.is_leader:
            try:
                # Check if token needs refreshing
                if await self._check_token_needs_refresh(token_type):
                    logger.info(f"Refreshing {token_type} token")
                    
                    # Track refresh status
                    self.refresh_in_progress[token_type] = True
                    
                    # Acquire lock with a longer timeout for browser operations
                    if await self.redis_cache.acquire_lock(token_type, timeout=120):
                        try:
                            # Double-check after acquiring lock
                            if await self._check_token_needs_refresh(token_type):
                                start_time = time.time()
                                
                                # Use the appropriate token refresh method
                                if token_type == 'track':
                                    success = await self.spotify_api._get_track_token()
                                else:
                                    success = await self.spotify_api._get_token(token_type)
                                    
                                duration = time.time() - start_time
                                
                                if success:
                                    logger.info(f"{token_type} token refresh succeeded in {duration:.2f}s")
                                else:
                                    logger.error(f"{token_type} token refresh failed in {duration:.2f}s")
                            else:
                                logger.info(f"{token_type} token was refreshed by another process")
                        finally:
                            # Always release the lock
                            await self.redis_cache.release_lock(token_type)
                            self.refresh_in_progress[token_type] = False
                    else:
                        logger.info(f"Could not acquire lock for {token_type} token refresh")
                        self.refresh_in_progress[token_type] = False
                
                # Sleep for next refresh interval with jitter
                jitter = random.uniform(-30, 30)
                next_refresh = self.refresh_interval + jitter
                
                logger.debug(f"Next {token_type} token refresh in {next_refresh:.1f}s")
                await asyncio.sleep(next_refresh)
                
            except asyncio.CancelledError:
                logger.info(f"{token_type} token refresh loop cancelled")
                self.refresh_in_progress[token_type] = False
                break
            except Exception as e:
                logger.error(f"Error in {token_type} token refresh loop: {e}")
                self.refresh_in_progress[token_type] = False
                await asyncio.sleep(60)  # 1 minute backoff on errors

    async def _discovered_hash_refresh_loop(self):
        """Background task to refresh the discovered-on hash"""
        logger.info("Starting discovered hash refresh loop")
        
        while self.is_leader:
            try:
                # Check if discovered hash exists
                discovered_hash = await self.redis_cache.get_discovered_hash()
                
                if not discovered_hash:
                    logger.info("Refreshing discovered-on hash")
                    
                    if await self.redis_cache.acquire_lock('discovered', timeout=60):
                        try:
                            start_time = time.time()
                            new_hash = await self.spotify_api._get_discovered_hash()
                            
                            if new_hash:
                                await self.redis_cache.save_discovered_hash(new_hash)
                                duration = time.time() - start_time
                                logger.info(f"Discovered hash refresh succeeded in {duration:.2f}s")
                            else:
                                logger.error("Failed to get discovered hash")
                        finally:
                            await self.redis_cache.release_lock('discovered')
                    else:
                        logger.info("Could not acquire lock for discovered hash refresh")
                
                # Sleep for next refresh interval with jitter (12 hours ± 1 hour)
                jitter = random.uniform(-3600, 3600)
                next_refresh = self.discovered_hash_refresh_interval + jitter
                
                logger.info(f"Next discovered hash refresh in {next_refresh/3600:.1f} hours")
                await asyncio.sleep(next_refresh)
                
            except asyncio.CancelledError:
                logger.info("Discovered hash refresh loop cancelled")
                break
            except Exception as e:
                logger.error(f"Error in discovered hash refresh loop: {e}")
                await asyncio.sleep(300)  # 5 minute backoff on errors

    async def _check_token_needs_refresh(self, token_type):
        """Check if a token needs refreshing"""
        # Check if token exists and is still valid
        if cached := await self.redis_cache.get_token(token_type):
            proxy_data = cached['proxy']
            created_at = proxy_data.get('created_at', 0) if isinstance(proxy_data, dict) else 0
            age = int(time.time()) - created_at
            
            # If token is >20% of its validity, refresh it
            if age > (self.token_validity * 0.15):
                logger.info(f"{token_type} token age ({age}s) > 15% of validity ({self.token_validity}s)")
                return True
                
            return False
        else:
            # No token, definitely needs refresh
            logger.info(f"No {token_type} token found, needs refresh")
            return True

    # Maintain the original method for backward compatibility
    async def _check_token_validity(self, token_type: str) -> bool:
        """
        Check if a token is still valid (not expired)
        """
        if cached := await self.redis_cache.get_token(token_type):
            proxy_data = cached.get('proxy', {})
            created_at = proxy_data.get('created_at', 0)
            age = int(time.time()) - created_at

            # Token is valid if it's less than 75% of token_validity seconds old
            return age < (self.token_validity * 0.75)

        return False

    async def ensure_token(self, token_type: str) -> bool:
        """
        Ensure a token is available for a specific type.
        This method is called by API endpoints to get a valid token.
        """
        try:
            # Check if we already have a valid token in memory
            if self.spotify_api.access_token and self.spotify_api.proxy:
                # We have a token in memory, still check cache to see if it's recent
                if cached := await self.redis_cache.get_token(token_type):
                    # If token exists in cache and is not too old, use it
                    self.spotify_api.access_token = cached['access_token']
                    
                    if 'client_token' in cached:
                        self.spotify_api.client_token = cached['client_token']
                        
                    proxy_data = cached['proxy']
                    if isinstance(proxy_data, str):
                        import json
                        proxy_data = json.loads(proxy_data)
                    self.spotify_api.proxy = ProxyConfig(**proxy_data)
                    
                    return True
                    
            # If we don't have a token in memory or our token is outdated, try to get one from Redis
            if cached := await self.redis_cache.get_token(token_type):
                self.spotify_api.access_token = cached['access_token']
                
                if 'client_token' in cached:
                    self.spotify_api.client_token = cached['client_token']
                    
                proxy_data = cached['proxy']
                if isinstance(proxy_data, str):
                    import json
                    proxy_data = json.loads(proxy_data)
                self.spotify_api.proxy = ProxyConfig(**proxy_data)
                
                return True
                
            # If we're the leader and no token is available, try to refresh immediately
            if self.is_leader:
                logger.warning(f"No {token_type} token available, immediate refresh needed")
                
                # Trigger immediate refresh
                if token_type == 'track':
                    return await self.spotify_api._get_track_token()
                else:
                    return await self.spotify_api._get_token(token_type)
                    
            # If we're not the leader, we just have to wait for the leader to refresh
            logger.warning(f"No {token_type} token available and we're not the leader")
            return False
                
        except Exception as e:
            logger.error(f"Error ensuring token for {token_type}: {e}")
            return False

    # Keep the original method name for backward compatibility
    async def refresh_token(self, token_type: str = 'playlist') -> bool:
        """For backward compatibility - now uses the leader-based approach"""
        # Only try to refresh if we're the leader
        if self.is_leader and not self.refresh_in_progress.get(token_type, False):
            logger.info(f"Leader initiating refresh for {token_type} token")
            
            # Use the appropriate token refresh method
            if token_type == 'track':
                return await self.spotify_api._get_track_token()
            else:
                return await self.spotify_api._get_token(token_type)
        else:
            # Just check if we have a valid token
            if cached := await self.redis_cache.get_token(token_type):
                self.spotify_api.access_token = cached['access_token']
                
                if 'client_token' in cached:
                    self.spotify_api.client_token = cached['client_token']
                    
                proxy_data = cached['proxy']
                if isinstance(proxy_data, str):
                    import json
                    proxy_data = json.loads(proxy_data)
                self.spotify_api.proxy = ProxyConfig(**proxy_data)
                
                return True
                
            return False

# Enhanced SpotifyAPI class - maintaining original name
class SpotifyAPIEnhanced(SpotifyAPI):
    """Enhanced SpotifyAPI class that uses TokenManager"""
    
    def __init__(self, db_path: str = 'spotify_cache.db'):
        super().__init__(db_path)
        self.token_manager = None  # Will be set later
        self._logger = logging.getLogger(__name__)
        
    @property
    def logger(self):
        """Property to access logger"""
        return self._logger
        
    def set_token_manager(self, token_manager):
        """Set the token manager instance"""
        self.token_manager = token_manager
        
    async def _ensure_auth(self, token_type: str = 'playlist') -> bool:
        """
        Enhanced auth method that uses the token manager.
        """
        if not self.token_manager:
            # Fall back to original implementation if token manager not set
            return await super()._ensure_auth(token_type)
            
        # Use token manager to ensure token
        return await self.token_manager.ensure_token(token_type)

# Setup function - keeping the original signature
async def setup_token_management(app, spotify_api, redis_cache):
    """Setup integrated token management for the app"""
    # Create TokenManager
    token_manager = TokenManager(spotify_api, redis_cache)

    # If SpotifyAPI is enhanced version, set the token manager
    if hasattr(spotify_api, 'set_token_manager'):
        spotify_api.set_token_manager(token_manager)

    # Initialize and start on startup
    @app.on_event("startup")
    async def initialize_token_manager():
        await token_manager.initialize()
        await token_manager.start()

        # Add a scheduled task to check discovered hash, but only the leader will execute it
        @repeat_every(seconds=600)  # Every 10 minutes
        async def scheduled_hash_monitoring():
            # Only the leader will perform the actual check
            await token_manager.monitor_discovered_hash()

        # Schedule the task
        asyncio.create_task(scheduled_hash_monitoring())

    return token_manager
