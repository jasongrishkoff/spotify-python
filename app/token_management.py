# In app/token_management.py

import asyncio
import logging
import aiohttp
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

    async def force_token_refresh_on_startup(self):
        """
        Force refresh of tokens during startup if they're stale or invalid.
        This ensures we start with fresh tokens after a restart.
        """
        logger.info("Performing startup token validation and refresh")
        
        for token_type in self.token_types:
            try:
                # Get current token
                if cached := await self.redis_cache.get_token(token_type):
                    proxy_data = cached['proxy']
                    if isinstance(proxy_data, str):
                        import json
                        proxy_data = json.loads(proxy_data)
                    
                    created_at = proxy_data.get('created_at', 0)
                    age = int(time.time()) - created_at
                    
                    # Set the token for validation
                    self.spotify_api.access_token = cached['access_token']
                    
                    if 'client_token' in cached:
                        self.spotify_api.client_token = cached['client_token']
                        
                    self.spotify_api.proxy = ProxyConfig(**proxy_data)
                    
                    # If token is old (> 10 minutes) or fails validation, refresh it
                    if age > 600 or not await self._validate_token(token_type):
                        logger.info(f"Forcing refresh of stale/invalid {token_type} token on startup")
                        
                        # Clear the token
                        await self.redis_cache.clear_token(token_type)
                        self.spotify_api.access_token = None
                        self.spotify_api.client_token = None
                        self.spotify_api.proxy = None
                        
                        # Refresh it if we're the leader
                        if self.is_leader:
                            if token_type == 'track':
                                await self.spotify_api._get_track_token()
                            else:
                                await self.spotify_api._get_token(token_type)
                    else:
                        logger.info(f"{token_type} token validated successfully on startup")
                else:
                    # No token in cache, refresh if we're the leader
                    if self.is_leader:
                        logger.info(f"No {token_type} token in cache, refreshing on startup")
                        if token_type == 'track':
                            await self.spotify_api._get_track_token()
                        else:
                            await self.spotify_api._get_token(token_type)
            except Exception as e:
                logger.error(f"Error refreshing {token_type} token on startup: {e}")

    async def wait_for_tokens(self):
        """For non-leader workers: wait for tokens to become available"""
        logger.info("Non-leader worker waiting for tokens...")
        
        max_wait_time = 90  # 1.5 minutes max wait
        check_interval = 5   # Check every 5 seconds
        
        start_time = time.time()
        while time.time() - start_time < max_wait_time:
            all_tokens_available = True
            for token_type in self.token_types:
                if not await self.redis_cache.get_token(token_type):
                    all_tokens_available = False
                    break
                    
            if all_tokens_available:
                logger.info("All required tokens are available")
                return
                
            await asyncio.sleep(check_interval)
        
        logger.warning("Maximum wait time for tokens reached")

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
        """Periodically check and attempt to become the leader with improved reliability"""
        logger.info("Starting leadership check loop")
        
        while True:
            try:
                # Check if there's a current leader with heartbeat
                current_leader = await self.redis_cache.redis.get(self.leader_key)
                last_heartbeat = await self.redis_cache.redis.get(f"{self.leader_key}_heartbeat")
                
                current_leader_str = None
                if current_leader is not None:
                    current_leader_str = current_leader.decode('utf-8') if isinstance(current_leader, bytes) else str(current_leader)
                
                # Check if current leader has been inactive
                leader_inactive = False
                if last_heartbeat is not None:
                    try:
                        last_beat = float(last_heartbeat.decode('utf-8') if isinstance(last_heartbeat, bytes) else last_heartbeat)
                        if time.time() - last_beat > 90:  # Leader considered inactive after 90s without heartbeat
                            logger.warning(f"Leader {current_leader_str} appears inactive (no heartbeat for >90s)")
                            leader_inactive = True
                    except (ValueError, TypeError):
                        leader_inactive = True
                
                is_now_leader = False
                
                # No leader exists or leader is inactive
                if current_leader_str is None or leader_inactive:
                    # Try to claim leadership with NX option
                    result = await self.redis_cache.redis.set(
                        self.leader_key,
                        self.instance_id,
                        ex=self.leader_lock_duration,
                        nx=True
                    )
                    is_now_leader = result is not None and result  # Check Redis returned True for NX set
                    
                    if is_now_leader:
                        logger.info(f"Instance {self.instance_id} claimed leadership" + 
                                    (f" (previous leader {current_leader_str} inactive)" if leader_inactive else " (no previous leader)"))
                
                # We're already the leader, update heartbeat and renew
                elif current_leader_str == self.instance_id:
                    # Update heartbeat first
                    await self.redis_cache.redis.set(
                        f"{self.leader_key}_heartbeat",
                        str(time.time()),
                        ex=self.leader_lock_duration * 2  # Longer expiry for heartbeat
                    )
                    
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
                    
                    # Important: Clear in-progress flags to prevent deadlocks from previous leader
                    for token_type in self.token_types:
                        self.refresh_in_progress[token_type] = False
                    
                    self._start_refresh_tasks()
                    
                    # Verify all tokens immediately after becoming leader
                    asyncio.create_task(self._verify_all_tokens())
                    
                    # Schedule the first hash monitoring after becoming leader
                    asyncio.create_task(self.monitor_discovered_hash())
                    
                elif not is_now_leader and self.is_leader:
                    logger.info(f"Instance {self.instance_id} is no longer the token refresh leader")
                    self.is_leader = False
                    self._stop_refresh_tasks()
                    
            except Exception as e:
                logger.error(f"Error in leadership check: {e}")
                import traceback
                logger.error(f"Leadership check traceback: {traceback.format_exc()}")
                
            # Check every 30 seconds
            await asyncio.sleep(self.leader_check_interval)

    async def _verify_all_tokens(self):
        """Verify all tokens are valid after becoming leader"""
        logger.info("New leader verifying all tokens")
        
        for token_type in self.token_types:
            try:
                if cached := await self.redis_cache.get_token(token_type):
                    # Set token for validation
                    self.spotify_api.access_token = cached['access_token']
                    
                    if 'client_token' in cached:
                        self.spotify_api.client_token = cached['client_token']
                    
                    proxy_data = cached['proxy']
                    if isinstance(proxy_data, str):
                        proxy_data = json.loads(proxy_data)
                    
                    self.spotify_api.proxy = ProxyConfig(**proxy_data)
                    
                    # Check actual token validity
                    valid = await self._validate_token(token_type)
                    
                    if not valid:
                        logger.warning(f"New leader found invalid {token_type} token, triggering immediate refresh")
                        # Clear and queue refresh
                        await self.redis_cache.clear_token(token_type)
                        if token_type == 'track':
                            asyncio.create_task(self.spotify_api._get_track_token())
                        else:
                            asyncio.create_task(self.spotify_api._get_token(token_type))
                else:
                    logger.warning(f"New leader found missing {token_type} token, triggering immediate refresh")
                    # Token doesn't exist, trigger refresh
                    if token_type == 'track':
                        asyncio.create_task(self.spotify_api._get_track_token())
                    else:
                        asyncio.create_task(self.spotify_api._get_token(token_type))
            except Exception as e:
                logger.error(f"Error verifying {token_type} token: {e}")

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
        
        # Add a local semaphore to ensure only one refresh attempt per token type
        if not hasattr(self, '_refresh_semaphores'):
            self._refresh_semaphores = {}
        
        if token_type not in self._refresh_semaphores:
            self._refresh_semaphores[token_type] = asyncio.Semaphore(1)
        
        while self.is_leader:
            try:
                # Check if token needs refreshing
                if await self._check_token_needs_refresh(token_type):
                    # Use semaphore to prevent concurrent refreshes of the same type
                    if self._refresh_semaphores[token_type].locked():
                        logger.info(f"Another refresh for {token_type} token is already in progress")
                        await asyncio.sleep(10)
                        continue
                        
                    async with self._refresh_semaphores[token_type]:
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
                                        # Call _get_token as instance method, not class method
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
                import traceback
                logger.error(f"Traceback: {traceback.format_exc()}")
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
        Ensure a token is available and valid for a specific type.
        This method is called by API endpoints to get a valid token.
        Enhanced with additional validation and immediate refresh on failure.
        """
        try:
            # First try to get the token from cache
            if cached := await self.redis_cache.get_token(token_type):
                proxy_data = cached['proxy']
                if isinstance(proxy_data, str):
                    import json
                    proxy_data = json.loads(proxy_data)
                    
                created_at = proxy_data.get('created_at', 0)
                age = int(time.time()) - created_at
                
                # If token is relatively fresh (less than 4 minutes old), consider it valid
                if age < 240:
                    self.spotify_api.access_token = cached['access_token']
                    
                    if 'client_token' in cached:
                        self.spotify_api.client_token = cached['client_token']
                        
                    self.spotify_api.proxy = ProxyConfig(**proxy_data)
                    return True
                
                # For older tokens, do a lightweight validation
                # Set the current token and proxy values
                self.spotify_api.access_token = cached['access_token']
                
                if 'client_token' in cached:
                    self.spotify_api.client_token = cached['client_token']
                    
                self.spotify_api.proxy = ProxyConfig(**proxy_data)
                
                # Validate by making a simple request
                valid = await self._validate_token(token_type)
                if valid:
                    return True
                    
                # If validation failed, clear the token and continue to refresh
                logger.warning(f"{token_type} token failed validation, forcing refresh")
                self.spotify_api.access_token = None
                self.spotify_api.client_token = None
                self.spotify_api.proxy = None
                
                # Clear the invalid token from Redis
                await self.redis_cache.clear_token(token_type)
            
            # If we don't have a valid token, we need to get one
            if self.is_leader:
                logger.warning(f"No valid {token_type} token available, immediate refresh needed")
                
                # Trigger immediate refresh
                if token_type == 'track':
                    return await self.spotify_api._get_track_token()
                else:
                    return await self.spotify_api._get_token(token_type)
                    
            # If we're not the leader, we just have to wait for the leader to refresh
            logger.warning(f"No valid {token_type} token available and we're not the leader")
            
            # Since we're not the leader but urgently need a token, wait briefly and check again
            await asyncio.sleep(2)
            if cached := await self.redis_cache.get_token(token_type):
                # Another instance may have refreshed it
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
                
        except Exception as e:
            logger.error(f"Error ensuring token for {token_type}: {e}")
            return False

    async def _validate_token(self, token_type: str) -> bool:
        """
        Perform a lightweight validation of a token by making a simple request.
        """
        try:
            # Ensure we have a session
            if not self.spotify_api.session:
                self.spotify_api.session = aiohttp.ClientSession()
                
            # Ensure we have a token and proxy
            if not self.spotify_api.access_token or not self.spotify_api.proxy:
                return False
                
            # Different validation endpoints based on token type
            if token_type == 'playlist':
                url = "https://api.spotify.com/v1/me"
            elif token_type == 'artist':
                url = "https://api.spotify.com/v1/me"
            elif token_type == 'track':
                url = "https://api.spotify.com/v1/me"
            else:
                url = "https://api.spotify.com/v1/me"
                
            # Add client token to headers if available
            headers = {'Authorization': f'Bearer {self.spotify_api.access_token}'}
            if hasattr(self.spotify_api, 'client_token') and self.spotify_api.client_token:
                headers['client-token'] = self.spotify_api.client_token
                
            # Make a simple request with a short timeout
            async with self.spotify_api.session.get(
                url,
                headers=headers,
                proxy=self.spotify_api.proxy.url, 
                proxy_auth=self.spotify_api.proxy.auth,
                timeout=5
            ) as response:
                if response.status in {200, 204, 401, 403}:  
                    # 401/403 are expected responses for a valid token without proper permissions
                    # We just want to check that the proxy works and the token is accepted
                    return True
                else:
                    logger.warning(f"Token validation failed with status {response.status}")
                    return False
                    
        except Exception as e:
            logger.error(f"Error validating {token_type} token: {e}")
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

        return await self._get_token('track')

class SpotifyAPIEnhanced(SpotifyAPI):
    """Enhanced SpotifyAPI class that only consumes tokens from Redis"""
    
    def __init__(self, db_path: str = 'spotify_cache.db'):
        super().__init__(db_path)
        self._logger = logging.getLogger(__name__)
        
    @property
    def logger(self):
        """Property to access logger"""
        return self._logger
        
    async def _ensure_auth(self, token_type: str = 'playlist') -> bool:
        """Ensure we have a valid token with proper backoff and circuit breaking"""
        # Check circuit breaker first
        if not await self.circuit_breaker.can_execute():
            logger.warning(f"Circuit breaker open, skipping {token_type} token fetch")
            return False
            
        # Quick check if we already have a valid token
        if self.access_token and self.proxy:
            if await self._verify_proxy():
                return True
            else:
                # Proxy is invalid, clear it
                logger.warning("Proxy validation failed, clearing token and proxy")
                self.access_token = None
                self.client_token = None
                self.proxy = None

        # Check cache with progressive backoff
        max_retries = 3
        for attempt in range(max_retries):
            # Try from cache first
            if cached := await self.cache.get_token(token_type):
                try:
                    self.access_token = cached['access_token']
                    if 'client_token' in cached:
                        self.client_token = cached['client_token']
                    
                    proxy_data = cached['proxy']
                    if isinstance(proxy_data, str):
                        proxy_data = json.loads(proxy_data)
                    
                    self.proxy = ProxyConfig(**proxy_data)
                    
                    if await self._verify_proxy():
                        await self.circuit_breaker.record_success()
                        return True
                except Exception as e:
                    logger.error(f"Error loading cached token: {e}")
                    
            # Try to get a new token with lock
            try:
                # Only try to get lock on first attempt to prevent deadlock
                if attempt == 0 and await self.cache.acquire_lock(token_type, timeout=10):
                    try:
                        success = await self._get_token(token_type)
                        if success:
                            await self.circuit_breaker.record_success()
                            return True
                        else:
                            await self.circuit_breaker.record_failure()
                    finally:
                        await self.cache.release_lock(token_type)
                elif attempt > 0:
                    # On retry, check if another process got token while we waited
                    await asyncio.sleep(1)
                    # Continue to next iteration of the for loop
                    continue
            except Exception as e:
                logger.error(f"Error getting token: {e}")
                await self.circuit_breaker.record_failure()
            
            # Exponential backoff with jitter
            backoff_time = min(30, (2 ** attempt) * (0.5 + random.random()))
            logger.info(f"Token fetch failed, backing off for {backoff_time:.2f}s")
            await asyncio.sleep(backoff_time)
        
        # All attempts failed
        await self.circuit_breaker.record_failure()
        return False

    # Remove or replace these with stub methods:
    async def _get_token(self, token_type: str = 'playlist') -> bool:
        """No longer fetches tokens - tokens come from token_worker"""
        self.logger.warning("Token refresh attempted but is now handled by external worker")
        return False
    
    async def _get_track_token(self) -> bool:
        """No longer fetches tokens - tokens come from token_worker"""
        return await self._get_token('track')

async def setup_token_management(app, spotify_api, redis_cache):
    """Simplified setup for token consumption only"""
    
    @app.on_event("startup")
    async def initialize_token_cache():
        """Load tokens from Redis on startup"""
        logger.info("Initializing token cache")
        
        # Initialize hash manager if present
        if hasattr(spotify_api, 'hash_manager'):
            await spotify_api.hash_manager.initialize()
        
        # Load initial tokens
        token_types = ['playlist', 'artist', 'track']
        for token_type in token_types:
            try:
                if cached := await redis_cache.get_token(token_type):
                    # Set the token in memory
                    spotify_api.access_token = cached['access_token']
                    
                    if 'client_token' in cached:
                        spotify_api.client_token = cached['client_token']
                    
                    proxy_data = cached['proxy']
                    if isinstance(proxy_data, str):
                        import json
                        proxy_data = json.loads(proxy_data)
                    
                    spotify_api.proxy = ProxyConfig(**proxy_data)
                    logger.info(f"Loaded {token_type} token from cache")
                else:
                    logger.warning(f"No {token_type} token available in cache")
            except Exception as e:
                logger.error(f"Error initializing {token_type} token: {e}")
    
    return None  # No TokenManager instance needed anymore
