#!/usr/bin/env python3
import asyncio
import logging
import sys
import os
import time
import random
import json
import signal
from typing import Dict, Optional, List
import aiohttp

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('token_worker.log')
    ]
)
logger = logging.getLogger('token_worker')

# Import necessary components from the app
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.spotify import ProxyConfig, RateLimiter
from app.cache import RedisCache

class TokenWorker:
    """Dedicated worker for managing Spotify API tokens with improved reliability"""
    
    def __init__(self):
        self.redis_cache = RedisCache()
        self.running = False
        self.session = None
        self.token_types = ['playlist', 'artist', 'track']
        
        # Configuration
        self.token_refresh_interval = 180  # Changed from 240 to 180 seconds (10% of validity instead of 15%)
        self.hash_refresh_interval = 3600  # 1 hour
        self.token_validity = 1800  # 30 minutes
        self.shutdown_event = asyncio.Event()
        self.rate_limiter = RateLimiter(rate=10, max_concurrent=3)  # More conservative rate limits
        
        # Track the last refresh time for each token type
        self.last_refresh = {token_type: 0 for token_type in self.token_types}
        self.last_hash_refresh = 0
        
        # Track token health metrics
        self.token_health = {}
    
    async def start(self):
        """Start the token worker"""
        logger.info("Starting token worker")
        self.running = True
        self.session = aiohttp.ClientSession()
        
        # Setup signal handlers for graceful shutdown
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, self._handle_exit)
        
        # Start the main worker loop
        await self._worker_loop()
    
    def _handle_exit(self, signum, frame):
        """Handle exit signals"""
        logger.info(f"Received exit signal {signum}")
        self.running = False
        self.shutdown_event.set()

    async def _worker_loop(self):
        """Main worker loop that manages token and hash refresh"""
        try:
            # Initial token refresh at startup
            await self._refresh_all_tokens()
            
            # Track last cleanup time
            last_browser_cleanup = 0
            cleanup_interval = 300  # 5 minutes
            
            while self.running:
                try:
                    # Check and refresh tokens if needed
                    for token_type in self.token_types:
                        if await self._should_refresh_token(token_type):
                            await self._refresh_token(token_type)
                    
                    # Check and refresh GraphQL hashes if needed
                    if time.time() - self.last_hash_refresh > self.hash_refresh_interval:
                        await self._refresh_graphql_hashes()
                    
                    # Periodic browser cleanup
                    now = time.time()
                    if now - last_browser_cleanup > cleanup_interval:
                        await self._cleanup_orphaned_browsers()
                        last_browser_cleanup = now
                    
                    # Sleep with check for shutdown
                    for _ in range(10):  # Check shutdown every second
                        if self.shutdown_event.is_set():
                            break
                        await asyncio.sleep(1)
                
                except Exception as e:
                    logger.error(f"Error in worker loop: {e}", exc_info=True)
                    await asyncio.sleep(30)  # Longer sleep on error
        
        finally:
            # Cleanup on exit
            await self._cleanup_orphaned_browsers()  # Final cleanup
            if self.session and not self.session.closed:
                await self.session.close()
            logger.info("Token worker stopped")

    async def _should_refresh_token(self, token_type: str) -> bool:
        """Check if a token needs refreshing with improved logic"""
        # Check if we have a current token
        current_token = await self.redis_cache.get_token(token_type, 0)
        
        # Always refresh if we don't have a current token
        if not current_token:
            logger.info(f"No {token_type} token available, needs refresh")
            return True
        
        # Check token age
        now = time.time()
        
        # Check if token has reached refresh threshold (10% of validity)
        proxy_data = current_token['proxy']
        if isinstance(proxy_data, str):
            proxy_data = json.loads(proxy_data)
        
        created_at = proxy_data.get('created_at', 0)
        age = int(now) - created_at
        
        # Refresh if token is >10% of its validity period
        if age > (self.token_validity * 0.10):
            logger.info(f"{token_type} token age ({age}s) > 10% of validity, needs refresh")
            return True
            
        # Check health metrics if available
        if 'health_metrics' in current_token:
            metrics = current_token['health_metrics']
            
            # If the token has had multiple consecutive failures, refresh it
            if metrics.get('consecutive_failures', 0) >= 3:
                logger.warning(f"{token_type} token has {metrics['consecutive_failures']} consecutive failures, forcing refresh")
                return True
                
            # If success rate is low, refresh it
            if metrics.get('success_rate', 1.0) < 0.7 and metrics.get('failure_count', 0) > 5:
                logger.warning(f"{token_type} token has low success rate ({metrics['success_rate']:.2f}), forcing refresh")
                return True
        
        # Check if it's been a while since last refresh
        time_since_refresh = now - self.last_refresh[token_type]
        if time_since_refresh > self.token_refresh_interval:
            # Also validate the token
            if not await self._validate_token(token_type):
                logger.info(f"{token_type} token failed validation, needs refresh")
                return True
        
        return False
    
    async def _validate_token(self, token_type: str, backup_index: int = 0) -> bool:
        """
        Validate a token by making a simple request.
        
        Args:
            token_type (str): Type of token to validate
            backup_index (int): Backup index (0 = current, 1-3 = backups)
            
        Returns:
            bool: True if token is valid, False otherwise
        """
        try:
            if cached := await self.redis_cache.get_token(token_type, backup_index):
                access_token = cached.get('access_token')
                client_token = cached.get('client_token')
                
                proxy_data = cached['proxy']
                if isinstance(proxy_data, str):
                    proxy_data = json.loads(proxy_data)
                
                proxy = ProxyConfig(**proxy_data)
                
                # Select test endpoint based on token type
                if token_type == 'playlist':
                    test_id = "37i9dQZEVXcJZyENOWUFo7"  # Popular playlist
                    url = f"https://api.spotify.com/v1/playlists/{test_id}/tracks?limit=1"
                elif token_type == 'artist':
                    test_id = "4gzpq5DPGxSnKTe4SA8HAU"  # Popular artist
                    url = f"https://api.spotify.com/v1/artists/{test_id}"
                elif token_type == 'track':
                    test_id = "4cOdK2wGLETKBW3PvgPWqT"  # Popular track
                    url = f"https://api.spotify.com/v1/tracks/{test_id}"
                else:
                    url = "https://api.spotify.com/v1/me"
                
                headers = {'Authorization': f'Bearer {access_token}'}
                if client_token:
                    headers['client-token'] = client_token
                
                async with self.session.get(
                    url,
                    headers=headers,
                    proxy=proxy.url,
                    proxy_auth=proxy.auth,
                    timeout=10
                ) as response:
                    # Check if response is successful
                    success = response.status == 200
                    
                    # Update token health metrics
                    await self.redis_cache.update_token_health(token_type, backup_index, success)
                    
                    if success:
                        logger.debug(f"{token_type} token (backup {backup_index}) validated successfully")
                        return True
                    
                    # Log error details
                    logger.warning(f"{token_type} token (backup {backup_index}) validation failed with status {response.status}")
                    if response.status in {401, 407}:
                        error_body = await response.text()
                        logger.warning(f"Error response: {error_body[:200]}")
                    
                    return False
            
            return False
        
        except Exception as e:
            logger.error(f"Error validating {token_type} token (backup {backup_index}): {e}")
            # Update token health metrics for failure
            await self.redis_cache.update_token_health(token_type, backup_index, False)
            return False
    
    async def _refresh_all_tokens(self):
        """Refresh all token types"""
        logger.info("Performing initial token refresh for all types")
        
        for token_type in self.token_types:
            try:
                await self._refresh_token(token_type)
            except Exception as e:
                logger.error(f"Error during initial {token_type} token refresh: {e}", exc_info=True)
        
        # Also refresh GraphQL hashes
        await self._refresh_graphql_hashes()
    
    async def _refresh_token(self, token_type: str):
        """
        Refresh a specific token type with improved reliability.
        
        This method captures a new token, validates it before saving,
        and implements the token rotation system.
        """
        logger.info(f"Refreshing {token_type} token")
        
        # Try to acquire lock to prevent multiple workers from refreshing simultaneously
        if not await self.redis_cache.acquire_lock(token_type, timeout=120):
            logger.info(f"Another process is already refreshing {token_type} token")
            return
        
        try:
            # Only allow one refresh at a time across all token types using the rate limiter
            await self.rate_limiter.acquire()
            
            # Get a new proxy
            proxy = await self._get_proxy()
            if not proxy:
                logger.error("Failed to get proxy for token refresh")
                return
            
            # Capture tokens using browser automation
            access_token, client_token, operation_hashes = await self._capture_tokens(proxy)
            
            if access_token:
                # Create initial health metrics for the new token
                health_metrics = {
                    'success_count': 0,
                    'failure_count': 0,
                    'consecutive_failures': 0,
                    'success_rate': 1.0,
                    'last_used': int(time.time())
                }
                
                # Before saving, validate the new token
                valid = await self._validate_specific_token(token_type, access_token, client_token, proxy)
                
                if valid:
                    # Token is valid, save it (this will shift existing tokens to backup slots)
                    success = await self.redis_cache.save_token(
                        access_token,
                        proxy.__dict__,
                        token_type=token_type,
                        client_token=client_token,
                        health_metrics=health_metrics
                    )
                    
                    if success:
                        logger.info(f"Successfully refreshed and validated {token_type} token")
                        self.last_refresh[token_type] = time.time()
                        
                        # Update GraphQL hashes if available
                        if operation_hashes:
                            for op_name, hash_value in operation_hashes.items():
                                if hash_value:
                                    await self._save_graphql_hash(op_name, hash_value)
                    else:
                        logger.error(f"Failed to save {token_type} token to Redis")
                else:
                    logger.error(f"New {token_type} token failed validation, not saving")
            else:
                logger.error(f"Failed to get {token_type} token")
                
                # If we failed to get a new token, check if any backups are valid
                await self._try_promote_valid_backup(token_type)
        
        except Exception as e:
            logger.error(f"Error refreshing {token_type} token: {e}", exc_info=True)
        
        finally:
            # Release resources
            self.rate_limiter.release()
            await self.redis_cache.release_lock(token_type)
    
            return False
    
    async def _validate_specific_token(self, token_type: str, access_token: str, client_token: Optional[str], proxy: ProxyConfig) -> bool:
        """
        Validate a specific token before saving it.

        Args:
            token_type (str): Type of token
            access_token (str): Access token to validate
            client_token (Optional[str]): Client token
            proxy (ProxyConfig): Proxy to use

        Returns:
            bool: True if token is valid, False otherwise
        """
        try:
            # Select test endpoint based on token type
            if token_type == 'playlist':
                test_id = "37i9dQZEVXcJZyENOWUFo7"  # Popular playlist
                url = f"https://api.spotify.com/v1/playlists/{test_id}/tracks?limit=1"
            elif token_type == 'artist':
                test_id = "4gzpq5DPGxSnKTe4SA8HAU"  # Popular artist
                url = f"https://api.spotify.com/v1/artists/{test_id}"
            elif token_type == 'track':
                test_id = "4cOdK2wGLETKBW3PvgPWqT"  # Popular track
                url = f"https://api.spotify.com/v1/tracks/{test_id}"
            else:
                url = "https://api.spotify.com/v1/me"

            headers = {'Authorization': f'Bearer {access_token}'}
            if client_token:
                headers['client-token'] = client_token

            async with self.session.get(
                url,
                headers=headers,
                proxy=proxy.url,
                proxy_auth=proxy.auth,
                timeout=10
            ) as response:
                # Check if response is successful
                if response.status == 200:
                    logger.info(f"New {token_type} token validated successfully")
                    return True

                # Log error details
                logger.warning(f"New {token_type} token validation failed with status {response.status}")
                if response.status in {401, 407}:
                    error_body = await response.text()
                    logger.warning(f"Error response: {error_body[:200]}")

                return False

        except Exception as e:
            logger.error(f"Error validating new {token_type} token: {e}")
            return False

    async def _try_promote_valid_backup(self, token_type: str) -> bool:
        """
        Try to promote a valid backup token to current position.
        
        Args:
            token_type (str): Type of token
            
        Returns:
            bool: True if a backup was promoted, False otherwise
        """
        # Check all backup slots
        for backup_index in range(1, 4):  # 1, 2, 3
            if await self.redis_cache.get_token(token_type, backup_index):
                # Validate the backup
                if await self._validate_token(token_type, backup_index):
                    logger.info(f"Found valid {token_type} backup at index {backup_index}, promoting")
                    success = await self.redis_cache.promote_backup_token(token_type, backup_index)
                    return success
        
        logger.warning(f"No valid {token_type} backups found")
        return False

    async def _cleanup_orphaned_browsers(self):
        """Clean up any orphaned browser processes"""
        try:
            # Import psutil within function to avoid global import
            import psutil
            
            chrome_processes = []
            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                try:
                    if any(browser in proc.info['name'].lower()
                          for browser in ['chrome', 'chromium']):
                        if any(arg for arg in (proc.info['cmdline'] or [])
                              if '--headless' in str(arg)):
                            chrome_processes.append(proc)
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue

            for proc in chrome_processes:
                try:
                    # Don't kill our own process
                    if proc.pid != os.getpid():
                        proc.terminate()
                        try:
                            proc.wait(timeout=3)
                        except psutil.TimeoutExpired:
                            proc.kill()
                        logger.info(f"Terminated browser process {proc.pid}")
                except psutil.NoSuchProcess:
                    continue
                except Exception as e:
                    logger.error(f"Error killing process {proc.pid}: {e}")

            if chrome_processes:
                logger.info(f"Cleaned up {len(chrome_processes)} chrome processes")

        except Exception as e:
            logger.error(f"Error in browser cleanup: {e}")

    async def _get_proxy(self) -> Optional[ProxyConfig]:
        """Get a new proxy from the proxy service with WebShare fallback"""
        # First try SubmitHub API
        for attempt in range(2):  # Reduced attempts for SubmitHub
            try:
                if not self.session:
                    self.session = aiohttp.ClientSession()
                    
                async with self.session.get('https://api.submithub.com/api/proxy', timeout=10) as response:
                    if response.status == 200:
                        try:
                            proxy_data = await response.json()
                            
                            # Validate all required fields exist
                            address = proxy_data.get('proxy_address')
                            ports = proxy_data.get('ports', {})
                            http_port = ports.get('http') if isinstance(ports, dict) else None
                            username = proxy_data.get('username')
                            password = proxy_data.get('password')
                            
                            if not all([address, http_port, username, password]):
                                logger.warning(f"SubmitHub proxy data missing required fields (attempt {attempt+1})")
                                await asyncio.sleep(1)
                                continue
                            
                            # Create proxy config
                            proxy = ProxyConfig(
                                address=address,
                                port=http_port,
                                username=username,
                                password=password,
                                created_at=int(time.time())
                            )
                            
                            # Test the proxy before returning it
                            if await self._test_proxy(proxy):
                                logger.info(f"Got working proxy from SubmitHub: {proxy}")
                                return proxy
                            else:
                                logger.warning(f"SubmitHub proxy validation failed (attempt {attempt+1})")
                        except Exception as e:
                            logger.error(f"Error processing SubmitHub proxy data: {e}")
                    else:
                        logger.warning(f"SubmitHub proxy request failed with status: {response.status}")
                
            except aiohttp.ClientConnectorError as e:
                logger.warning(f"Connection error to SubmitHub (attempt {attempt+1}): {e}")
            except Exception as e:
                logger.error(f"Error getting proxy from SubmitHub (attempt {attempt+1}): {e}")
            
            # Short backoff before retry
            if attempt < 1:
                await asyncio.sleep(2)
        
        # If SubmitHub fails, fall back to WebShare API
        logger.info("SubmitHub proxy acquisition failed, falling back to WebShare API")
        
        # WebShare API credentials
        webshare_key = 'fd9e64adac30d6f46be5ad88b19fffbc42027418'  # 5TB jason@submithub.com
        webshare_url = 'https://proxy.webshare.io/api/proxy/list/?page=1'
        
        for attempt in range(3):
            try:
                headers = {'Authorization': f'Token {webshare_key}'}
                
                async with self.session.get(webshare_url, headers=headers, timeout=15) as response:
                    if response.status == 200:
                        try:
                            data = await response.json()
                            
                            # Check if we have results
                            if not data.get('results') or len(data['results']) == 0:
                                logger.warning(f"No proxies returned from WebShare (attempt {attempt+1})")
                                await asyncio.sleep(2)
                                continue
                            
                            # Pick a random proxy from the list
                            proxies = data['results']
                            proxy_data = random.choice(proxies)
                            
                            # Extract required fields from WebShare format
                            address = proxy_data.get('proxy_address')
                            ports = proxy_data.get('ports', {})
                            http_port = ports.get('http') if isinstance(ports, dict) else None
                            username = proxy_data.get('username')
                            password = proxy_data.get('password')
                            
                            if not all([address, http_port, username, password]):
                                logger.warning(f"WebShare proxy data missing required fields (attempt {attempt+1})")
                                await asyncio.sleep(2)
                                continue
                            
                            # Create proxy config
                            proxy = ProxyConfig(
                                address=address,
                                port=http_port,
                                username=username,
                                password=password,
                                created_at=int(time.time())
                            )
                            
                            # Test the proxy before returning it
                            if await self._test_proxy(proxy):
                                logger.info(f"Got working proxy from WebShare: {proxy}")
                                return proxy
                            else:
                                logger.warning(f"WebShare proxy validation failed (attempt {attempt+1})")
                        except Exception as e:
                            logger.error(f"Error processing WebShare proxy data: {e}")
                    else:
                        logger.warning(f"WebShare API request failed with status: {response.status}")
                        
            except Exception as e:
                logger.error(f"Error getting proxy from WebShare (attempt {attempt+1}): {e}")
            
            # Backoff before retry
            await asyncio.sleep(2 * (attempt + 1))
        
        logger.error("All proxy acquisition attempts failed (both SubmitHub and WebShare)")
        return None

    async def _test_proxy(self, proxy: ProxyConfig) -> bool:
        """Test if a proxy is working"""
        try:
            test_url = "https://httpbin.org/ip"
            async with self.session.get(
                test_url,
                proxy=proxy.url,
                proxy_auth=proxy.auth,
                timeout=10
            ) as response:
                if response.status == 200:
                    return True
                logger.warning(f"Proxy test failed with status: {response.status}")
                return False
        except Exception as e:
            logger.warning(f"Proxy test error: {e}")
            return False
    
    async def _capture_tokens(self, proxy: ProxyConfig):
        """
        Capture Spotify tokens and operation hashes using browser automation.
        Updated to handle v2 API and POST requests.
        """
        from playwright.async_api import async_playwright
        
        logger.info("Starting token capture process")
        access_token = None
        client_token = None
        operation_hashes = {}
        token_event = asyncio.Event()
        operations_seen = set()
        
        # Operations we want to capture
        operations_to_capture = {
            'fetchPlaylist', 
            'queryArtistOverview', 
            'getTrack', 
            'queryArtistDiscoveredOn'
        }
        
        try:
            async with async_playwright() as p:
                browser_args = [
                    '--no-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-blink-features=AutomationControlled',
                    '--disable-web-security',
                    '--disable-features=IsolateOrigins,site-per-process',
                    '--disable-setuid-sandbox',
                    '--no-default-browser-check',
                    '--no-first-run',
                    '--no-zygote',
                    '--disable-gpu'
                ]
                
                browser = await p.chromium.launch(
                    proxy=proxy.to_playwright_config(),
                    headless=True,
                    args=browser_args
                )
                
                try:
                    # Create context with credible user agent
                    context = await browser.new_context(
                        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
                        viewport={"width": 1280, "height": 800},
                        locale="en-US",
                        timezone_id="America/New_York",
                        permissions=["geolocation"],
                        java_script_enabled=True
                    )
                    
                    page = await context.new_page()
                    
                    # Set up network request interceptor that handles both v1 and v2 APIs
                    async def intercept_request(route):
                        nonlocal access_token, client_token, operation_hashes
                        request = route.request
                        url = request.url
                        
                        if "api-partner.spotify.com" in url:
                            headers = request.headers
                            
                            # Extract tokens from headers
                            if "authorization" in headers and headers["authorization"].startswith("Bearer "):
                                current_token = headers["authorization"].replace("Bearer ", "")
                                
                                if not access_token or access_token != current_token:
                                    access_token = current_token
                                    logger.info(f"Found access token: {access_token[:15]}...")
                            
                            if "client-token" in headers:
                                current_client_token = headers["client-token"]
                                
                                if not client_token or client_token != current_client_token:
                                    client_token = current_client_token
                                    logger.info(f"Found client token: {client_token[:15]}...")
                            
                            # Extract GraphQL operation hashes from both GET and POST requests
                            try:
                                # For v1 API (GET requests with URL parameters)
                                if "operationName=" in url and "extensions=" in url:
                                    import urllib.parse
                                    parsed_url = urllib.parse.urlparse(url)
                                    query_params = urllib.parse.parse_qs(parsed_url.query)
                                    
                                    if 'operationName' in query_params:
                                        operation_name = query_params['operationName'][0]
                                        
                                        # Only process operations we're interested in
                                        if operation_name in operations_to_capture:
                                            operations_seen.add(operation_name)
                                            
                                            # Parse extensions to get hash
                                            if 'extensions' in query_params:
                                                extensions_str = query_params['extensions'][0]
                                                try:
                                                    extensions = json.loads(extensions_str)
                                                    if 'persistedQuery' in extensions and 'sha256Hash' in extensions['persistedQuery']:
                                                        hash_value = extensions['persistedQuery']['sha256Hash']
                                                        
                                                        # Store the hash for this operation
                                                        operation_hashes[operation_name] = hash_value
                                                        logger.info(f"Found hash for {operation_name}: {hash_value[:10]}...")
                                                except json.JSONDecodeError:
                                                    pass
                                
                                # For v2 API (POST requests with JSON body)
                                elif request.method == "POST" and "/pathfinder/v2/query" in url:
                                    post_data = request.post_data
                                    if post_data:
                                        try:
                                            data = json.loads(post_data)
                                            operation_name = data.get('operationName')
                                            
                                            if operation_name in operations_to_capture:
                                                operations_seen.add(operation_name)
                                                
                                                # Extract hash from extensions
                                                extensions = data.get('extensions', {})
                                                if extensions and 'persistedQuery' in extensions:
                                                    hash_value = extensions['persistedQuery'].get('sha256Hash')
                                                    if hash_value:
                                                        # Store the hash for this operation
                                                        operation_hashes[operation_name] = hash_value
                                                        logger.info(f"Found hash for {operation_name} from POST: {hash_value[:10]}...")
                                        except json.JSONDecodeError:
                                            pass
                            except Exception as hash_e:
                                logger.error(f"Error extracting hash: {hash_e}")
                            
                            # Signal that we've found both tokens
                            if access_token and client_token:
                                token_event.set()
                        
                        # Continue the request
                        await route.continue_()
                    
                    # Set up the route handler
                    await page.route("**/*", intercept_request)
                    
                    # Navigate to Spotify
                    logger.info("Navigating to Spotify main page...")
                    try:
                        await page.goto('https://open.spotify.com',
                                       timeout=30000,  # Shorter timeout
                                       wait_until='domcontentloaded')  # Don't wait for full page load
                    except Exception as e:
                        logger.warning(f"Initial navigation error: {e}, trying simplified approach")
                        # Try a different approach if the main navigation fails
                        await page.set_content("<html><body></body></html>")
                        await page.evaluate("""() => {
                            window.location.href = "https://open.spotify.com";
                        }""")
                        await page.wait_for_load_state('domcontentloaded', timeout=30000)
                    
                    # Wait for network activity to settle
                    await page.wait_for_load_state('networkidle', timeout=30000)
                    
                    # Wait for both tokens with timeout
                    try:
                        await asyncio.wait_for(token_event.wait(), timeout=30)
                        logger.info("Successfully extracted tokens")
                    except asyncio.TimeoutError:
                        logger.warning("Timeout waiting for tokens")
                    
                    # Now visit specific pages to capture hashes for each operation type
                    if 'fetchPlaylist' not in operations_seen:
                        logger.info("Navigating to playlist page to capture hash...")
                        await page.goto('https://open.spotify.com/playlist/37i9dQZEVXcJZyENOWUFo7', timeout=30000)
                        await page.wait_for_load_state('networkidle', timeout=30000)
                        await asyncio.sleep(3)
                    
                    if 'getTrack' not in operations_seen:
                        logger.info("Navigating to track page to capture hash...")
                        await page.goto('https://open.spotify.com/track/4cOdK2wGLETKBW3PvgPWqT', timeout=30000)
                        await page.wait_for_load_state('networkidle', timeout=30000)
                        await asyncio.sleep(3)
                    
                    if 'queryArtistOverview' not in operations_seen:
                        logger.info("Navigating to artist page to capture hash...")
                        await page.goto('https://open.spotify.com/artist/3GBPw9NK25X1Wt2OUvOwY3', timeout=30000)
                        await page.wait_for_load_state('networkidle', timeout=30000)
                        await asyncio.sleep(3)
                    
                    if 'queryArtistDiscoveredOn' not in operations_seen:
                        logger.info("Navigating to discovered-on page to capture hash...")
                        await page.goto('https://open.spotify.com/artist/3GBPw9NK25X1Wt2OUvOwY3/discovered-on', timeout=30000)
                        await page.wait_for_load_state('networkidle', timeout=30000)
                        await asyncio.sleep(3)
                
                finally:
                    # Clean up browser resources
                    try:
                        if context:
                            await context.close()
                        await browser.close()
                        logger.info("Browser closed")
                    except Exception as e:
                        logger.error(f"Error closing browser: {e}")
        
        except Exception as e:
            logger.error(f"Error in token capture: {e}", exc_info=True)
        
        return access_token, client_token, operation_hashes

    async def _refresh_graphql_hashes(self):
        """Refresh GraphQL operation hashes"""
        logger.info("Refreshing GraphQL operation hashes")
        
        # Check discovered-on hash specifically
        await self._refresh_discovered_hash()
        
        # Update timestamp
        self.last_hash_refresh = time.time()
    
    async def _refresh_discovered_hash(self):
        """Refresh the discovered-on hash specifically"""
        if await self.redis_cache.get_discovered_hash():
            logger.info("Discovered hash already exists, testing it")
            # Test the hash with a real request
            if await self._test_discovered_hash():
                logger.info("Discovered hash is valid")
                return
        
        logger.info("Refreshing discovered-on hash")
        
        # Try to acquire lock
        if not await self.redis_cache.acquire_lock('discovered', timeout=60):
            logger.info("Another process is already refreshing discovered hash")
            return
        
        try:
            # Get a proxy
            proxy = await self._get_proxy()
            if not proxy:
                logger.error("Failed to get proxy for hash refresh")
                return
            
            # Use the browser to get the hash
            discovered_hash = await self._capture_discovered_hash(proxy)
            
            if discovered_hash:
                # Save the hash to Redis
                success = await self.redis_cache.save_discovered_hash(discovered_hash)
                if success:
                    logger.info(f"Successfully saved discovered hash: {discovered_hash[:10]}...")
                else:
                    logger.error("Failed to save discovered hash to Redis")
            else:
                logger.error("Failed to capture discovered hash")
        
        except Exception as e:
            logger.error(f"Error refreshing discovered hash: {e}", exc_info=True)
        
        finally:
            await self.redis_cache.release_lock('discovered')
    
    async def _capture_discovered_hash(self, proxy: ProxyConfig) -> Optional[str]:
        """Capture the discovered-on hash using browser automation, updated for v2 API"""
        from playwright.async_api import async_playwright
        
        logger.info("Starting discovered hash capture")
        discovered_hash = None
        
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(
                    proxy=proxy.to_playwright_config(),
                    headless=True,
                    args=['--no-sandbox', '--disable-dev-shm-usage']
                )
                
                try:
                    context = await browser.new_context(
                        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
                    )
                    page = await context.new_page()
                    
                    hash_event = asyncio.Event()
                    
                    async def handle_request(route):
                        nonlocal discovered_hash
                        request = route.request
                        
                        # Check for discovered-on requests in both v1 and v2 paths
                        if 'queryArtistDiscoveredOn' in request.url:
                            logger.info(f"Found queryArtistDiscoveredOn request via {request.method}")
                            
                            # Handle POST requests (v2 API)
                            if request.method == 'POST':
                                try:
                                    post_data = request.post_data
                                    if post_data:
                                        try:
                                            data = json.loads(post_data)
                                            if data.get('extensions', {}).get('persistedQuery', {}).get('sha256Hash'):
                                                discovered_hash = data['extensions']['persistedQuery']['sha256Hash']
                                                logger.info(f"Found discovered hash from POST body: {discovered_hash}")
                                                hash_event.set()
                                        except json.JSONDecodeError:
                                            pass
                                except Exception as e:
                                    logger.error(f"Error parsing POST body: {e}")
                            
                            # Handle GET requests (v1 API)
                            else:
                                try:
                                    import urllib.parse
                                    parsed_url = urllib.parse.urlparse(request.url)
                                    query_params = urllib.parse.parse_qs(parsed_url.query)
                                    
                                    if 'extensions' in query_params:
                                        extensions_str = query_params['extensions'][0]
                                        try:
                                            extensions = json.loads(urllib.parse.unquote(extensions_str))
                                            if extensions.get('persistedQuery', {}).get('sha256Hash'):
                                                discovered_hash = extensions['persistedQuery']['sha256Hash']
                                                logger.info(f"Found discovered hash from URL: {discovered_hash}")
                                                hash_event.set()
                                        except json.JSONDecodeError:
                                            pass
                                except Exception as e:
                                    logger.error(f"Error parsing URL: {e}")
                        
                        await route.continue_()
                    
                    # Listen to network requests
                    await page.route("**/*", handle_request)
                    
                    # Visit artist discovered-on page
                    artist_id = "3GBPw9NK25X1Wt2OUvOwY3"  # Popular artist
                    await page.goto(f'https://open.spotify.com/artist/{artist_id}/discovered-on', timeout=60000)
                    
                    # Wait for hash with timeout
                    try:
                        await asyncio.wait_for(hash_event.wait(), timeout=30)
                    except asyncio.TimeoutError:
                        logger.error("Timeout waiting for discovered hash")
                
                finally:
                    await browser.close()
        
        except Exception as e:
            logger.error(f"Error capturing discovered hash: {e}", exc_info=True)
        
        return discovered_hash

    async def _test_discovered_hash(self) -> bool:
        """Test if the current discovered hash is valid with support for v2 API"""
        try:
            discovered_hash = await self.redis_cache.get_discovered_hash()
            if not discovered_hash:
                return False
            
            # Get a token for artist operations
            if cached := await self.redis_cache.get_token('artist'):
                access_token = cached.get('access_token')
                client_token = cached.get('client_token')
                
                proxy_data = cached['proxy']
                if isinstance(proxy_data, str):
                    proxy_data = json.loads(proxy_data)
                
                proxy = ProxyConfig(**proxy_data)
                
                # Test artist ID
                artist_id = "3GBPw9NK25X1Wt2OUvOwY3"
                
                # Try v2 endpoint first
                url = "https://api-partner.spotify.com/pathfinder/v2/query"
                
                variables = {
                    'uri': f'spotify:artist:{artist_id}',
                    'locale': ''
                }
                
                # Create payload for POST request
                payload = {
                    'operationName': 'queryArtistDiscoveredOn',
                    'variables': variables,
                    'extensions': {
                        'persistedQuery': {
                            'version': 1,
                            'sha256Hash': discovered_hash
                        }
                    }
                }
                
                headers = {
                    'Authorization': f'Bearer {access_token}',
                    'content-type': 'application/json',
                    'accept': 'application/json',
                    'app-platform': 'WebPlayer'
                }
                
                if client_token:
                    headers['client-token'] = client_token
                
                # Use POST request for v2 API
                async with self.session.post(
                    url,
                    json=payload,
                    headers=headers,
                    proxy=proxy.url,
                    proxy_auth=proxy.auth,
                    timeout=10
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        if 'errors' in data:
                            error_msg = data.get('errors', [{}])[0].get('message', '')
                            logger.warning(f"GraphQL error testing discovered hash: {error_msg}")
                            return False
                        return True
                    
                    # If v2 fails, try v1 as fallback
                    if response.status != 200:
                        logger.warning(f"v2 API test failed with status {response.status}, trying v1 as fallback")
                        
                        # Use the old v1 endpoint with GET request
                        v1_url = "https://api-partner.spotify.com/pathfinder/v1/query"
                        
                        v1_params = {
                            'operationName': 'queryArtistDiscoveredOn',
                            'variables': json.dumps(variables),
                            'extensions': json.dumps({
                                'persistedQuery': {
                                    'version': 1,
                                    'sha256Hash': discovered_hash
                                }
                            })
                        }
                        
                        async with self.session.get(
                            v1_url,
                            params=v1_params,
                            headers=headers,
                            proxy=proxy.url,
                            proxy_auth=proxy.auth,
                            timeout=10
                        ) as v1_response:
                            if v1_response.status == 200:
                                v1_data = await v1_response.json()
                                if 'errors' not in v1_data:
                                    logger.info("v1 API test succeeded")
                                    return True
                    
                    logger.warning(f"Discovered hash test failed with status {response.status}")
                    return False
            
            return False
        
        except Exception as e:
            logger.error(f"Error testing discovered hash: {e}")
            return False
 
    async def _save_graphql_hash(self, operation_name: str, hash_value: str):
        """Save a GraphQL operation hash to Redis"""
        try:
            # Only save if not empty and different from current
            if hash_value:
                # Store in Redis with 30-day expiry
                await self.redis_cache.redis.set(
                    f"graphql_hash_{operation_name}",
                    hash_value,
                    ex=86400 * 30  # 30 days
                )
                logger.info(f"Saved {operation_name} hash to Redis: {hash_value[:10]}...")
        except Exception as e:
            logger.error(f"Error saving {operation_name} hash: {e}")

async def main():
    """Main entry point for the token worker"""
    worker = TokenWorker()
    await worker.start()

if __name__ == "__main__":
    asyncio.run(main())
