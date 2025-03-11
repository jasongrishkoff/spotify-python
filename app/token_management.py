# In app/token_management.py

import asyncio
import logging
import time
import random
import os
import json
from typing import Optional, Dict
from .spotify import SpotifyAPI, ProxyConfig

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
        self.token_validity = 480    # 8 minutes
        self.discovered_hash_refresh_interval = 12 * 3600  # 12 hours
        
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

    async def leadership_check_loop(self):
        """Periodically check and attempt to become the leader"""
        logger.info("Starting leadership check loop")
        
        while True:
            try:
                # Check if there's a current leader
                current_leader = await self.redis_cache.redis.get(self.leader_key)
                
                # Fix: Handle both string and bytes without assuming decode is needed
                if current_leader is None or current_leader == self.instance_id:
                    # No leader or we are the current leader
                    # Try to claim or renew leadership
                    result = await self.redis_cache.redis.set(
                        self.leader_key,
                        self.instance_id,
                        ex=self.leader_lock_duration,
                        nx=(current_leader is None)  # Only use NX if there's no current leader
                    )
                    
                    # If we weren't the leader before but are now, start refresh tasks
                    if not self.is_leader and (result or current_leader == self.instance_id):
                        logger.info(f"Instance {self.instance_id} became the token refresh leader")
                        self.is_leader = True
                        self._start_refresh_tasks()
                        
                        # Log current state of tokens
                        for token_type in self.token_types:
                            if cached := await self.redis_cache.get_token(token_type):
                                proxy_data = cached['proxy']
                                created_at = proxy_data.get('created_at', 0) if isinstance(proxy_data, dict) else 0
                                age = int(time.time()) - created_at
                                logger.info(f"Current {token_type} token age: {age}s")
                        
                elif self.is_leader:
                    # We were the leader but someone else has taken over
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
            
            # If token is >50% of its validity, refresh it
            if age > (self.token_validity * 0.5):
                logger.info(f"{token_type} token age ({age}s) > 50% of validity ({self.token_validity}s)")
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
    
    return token_manager
