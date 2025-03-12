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
    """Dedicated worker for managing Spotify API tokens"""
    
    def __init__(self):
        self.redis_cache = RedisCache()
        self.running = False
        self.session = None
        self.token_types = ['playlist', 'artist', 'track']
        
        # Configuration
        self.token_refresh_interval = 240  # 4 minutes
        self.hash_refresh_interval = 3600  # 1 hour
        self.token_validity = 1800  # 30 minutes
        self.shutdown_event = asyncio.Event()
        self.rate_limiter = RateLimiter(rate=10, max_concurrent=3)  # More conservative rate limits
        
        # Track the last refresh time for each token type
        self.last_refresh = {token_type: 0 for token_type in self.token_types}
        self.last_hash_refresh = 0
    
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
            
            while self.running:
                try:
                    # Check and refresh tokens if needed
                    for token_type in self.token_types:
                        if await self._should_refresh_token(token_type):
                            await self._refresh_token(token_type)
                    
                    # Check and refresh GraphQL hashes if needed
                    if time.time() - self.last_hash_refresh > self.hash_refresh_interval:
                        await self._refresh_graphql_hashes()
                    
                    # Sleep with check for shutdown
                    for _ in range(10):  # Check shutdown every second
                        if self.shutdown_event.is_set():
                            break
                        await asyncio.sleep(1)
                
                except Exception as e:
                    logger.error(f"Error in worker loop: {e}", exc_info=True)
                    await asyncio.sleep(30)  # Longer sleep on error
        
        finally:
            # Cleanup
            if self.session and not self.session.closed:
                await self.session.close()
            logger.info("Token worker stopped")
    
    async def _should_refresh_token(self, token_type: str) -> bool:
        """Check if a token needs refreshing"""
        # Always refresh if we don't have a token
        if not await self.redis_cache.get_token(token_type):
            logger.info(f"No {token_type} token available, needs refresh")
            return True
        
        # Check token age
        now = time.time()
        time_since_refresh = now - self.last_refresh[token_type]
        
        # Refresh if it's been more than refresh_interval since last refresh
        if time_since_refresh > self.token_refresh_interval:
            # Also validate the token
            if not await self._validate_token(token_type):
                logger.info(f"{token_type} token failed validation, needs refresh")
                return True
                
            if cached := await self.redis_cache.get_token(token_type):
                proxy_data = cached['proxy']
                if isinstance(proxy_data, str):
                    proxy_data = json.loads(proxy_data)
                
                created_at = proxy_data.get('created_at', 0)
                age = int(now) - created_at
                
                # If token is >15% of its validity period, refresh it
                if age > (self.token_validity * 0.15):
                    logger.info(f"{token_type} token age ({age}s) > 15% of validity, needs refresh")
                    return True
        
        return False
    
    async def _validate_token(self, token_type: str) -> bool:
        """Validate a token by making a simple request"""
        try:
            if cached := await self.redis_cache.get_token(token_type):
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
                    if response.status == 200:
                        logger.debug(f"{token_type} token validated successfully")
                        return True
                    
                    # Log error details
                    logger.warning(f"{token_type} token validation failed with status {response.status}")
                    if response.status in {401, 407}:
                        error_body = await response.text()
                        logger.warning(f"Error response: {error_body[:200]}")
                    
                    return False
            
            return False
        
        except Exception as e:
            logger.error(f"Error validating {token_type} token: {e}")
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
        """Refresh a specific token type"""
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
                # Save the token to Redis
                success = await self.redis_cache.save_token(
                    access_token,
                    proxy.__dict__,
                    token_type=token_type,
                    client_token=client_token
                )
                
                if success:
                    logger.info(f"Successfully refreshed {token_type} token")
                    self.last_refresh[token_type] = time.time()
                    
                    # Update GraphQL hashes if available
                    if operation_hashes:
                        for op_name, hash_value in operation_hashes.items():
                            if hash_value:
                                await self._save_graphql_hash(op_name, hash_value)
                else:
                    logger.error(f"Failed to save {token_type} token to Redis")
            else:
                logger.error(f"Failed to get {token_type} token")
        
        except Exception as e:
            logger.error(f"Error refreshing {token_type} token: {e}", exc_info=True)
        
        finally:
            # Release resources
            self.rate_limiter.release()
            await self.redis_cache.release_lock(token_type)
    
    async def _get_proxy(self) -> Optional[ProxyConfig]:
        """Get a new proxy from the proxy service"""
        for attempt in range(3):
            try:
                if not self.session:
                    self.session = aiohttp.ClientSession()
                    
                async with self.session.get('https://api.submithub.com/api/proxy', timeout=15) as response:
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
                                logger.warning(f"Proxy data missing required fields (attempt {attempt+1})")
                                await asyncio.sleep(2 * (attempt + 1))
                                continue
                            
                            # Create proxy config manually
                            proxy = ProxyConfig(
                                address=address,
                                port=http_port,
                                username=username,
                                password=password,
                                created_at=int(time.time())
                            )
                            
                            # Test the proxy before returning it
                            if await self._test_proxy(proxy):
                                logger.info(f"Got working proxy: {proxy}")
                                return proxy
                            else:
                                logger.warning(f"Proxy validation failed, retrying... (attempt {attempt+1})")
                        except Exception as e:
                            logger.error(f"Error processing proxy data (attempt {attempt+1}): {e}")
                    else:
                        logger.warning(f"Failed to get proxy, status: {response.status} (attempt {attempt+1})")
                
            except Exception as e:
                logger.error(f"Error getting proxy (attempt {attempt+1}): {e}")
            
            # Backoff before retry
            await asyncio.sleep(2 * (attempt + 1))
        
        logger.error("All proxy acquisition attempts failed")
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
        Extracted and simplified from SpotifyAPI class.
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
                        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
                        viewport={"width": 1280, "height": 800},
                        locale="en-US",
                        timezone_id="America/New_York",
                        permissions=["geolocation"],
                        java_script_enabled=True
                    )
                    
                    page = await context.new_page()
                    
                    # Set up network request interceptor
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
                            
                            # Extract GraphQL operation hashes from URL
                            try:
                                if "operationName=" in url and "extensions=" in url:
                                    # Parse URL to get operationName
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
        """Capture the discovered-on hash using browser automation"""
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
                    context = await browser.new_context()
                    page = await context.new_page()
                    
                    hash_event = asyncio.Event()
                    
                    async def handle_request(route):
                        nonlocal discovered_hash
                        request = route.request
                        if 'queryArtistDiscoveredOn' in request.url:
                            logger.info(f"Found queryArtistDiscoveredOn request")
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
                                            logger.info(f"Found discovered hash: {discovered_hash[:10]}...")
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
        """Test if the current discovered hash is valid"""
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
                
                url = "https://api-partner.spotify.com/pathfinder/v1/query"
                
                variables = {
                    'uri': f'spotify:artist:{artist_id}',
                    'locale': ''
                }
                
                params = {
                    'operationName': 'queryArtistDiscoveredOn',
                    'variables': json.dumps(variables),
                    'extensions': json.dumps({
                        'persistedQuery': {
                            'version': 1,
                            'sha256Hash': discovered_hash
                        }
                    })
                }
                
                headers = {
                    'Authorization': f'Bearer {access_token}',
                    'content-type': 'application/json',
                    'accept': 'application/json',
                    'app-platform': 'WebPlayer'
                }
                
                if client_token:
                    headers['client-token'] = client_token
                
                async with self.session.get(
                    url,
                    params=params,
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
