import asyncio
import urllib.parse
import aiohttp
import json
import time
import logging
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
from contextlib import asynccontextmanager
from playwright.async_api import async_playwright
from .database import AsyncDatabase
from .cache import RedisCache

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ProxyPool:
    def __init__(self, max_size=5):
        self.proxies = []
        self.max_size = max_size
        self._lock = asyncio.Lock()
        self.last_fetch_time = 0
        self.fetch_cooldown = 5  # Seconds between proxy fetch attempts
        self.logger = logging.getLogger(__name__)

    async def get_proxy(self, session=None):
        async with self._lock:
            # Try to use existing proxies first
            working_proxies = [p for p in self.proxies if time.time() - p.created_at < 1800]

            if working_proxies:
                return random.choice(working_proxies)

            # Rate limit proxy fetching
            current_time = time.time()
            if current_time - self.last_fetch_time < self.fetch_cooldown:
                await asyncio.sleep(self.fetch_cooldown - (current_time - self.last_fetch_time))

            self.last_fetch_time = time.time()

            # If no working proxies, get a new one
            new_proxy = await self._fetch_new_proxy(session)
            if new_proxy:
                self.proxies.append(new_proxy)

                # Maintain max pool size
                if len(self.proxies) > self.max_size:
                    self.proxies.pop(0)

                return new_proxy

            # If we still have old proxies, better to use those than nothing
            if self.proxies:
                self.logger.warning("Using expired proxy as fallback")
                return self.proxies[-1]

            return None

    async def _fetch_new_proxy(self, session=None):
        # Add jitter to prevent hammering proxy service
        await asyncio.sleep(random.uniform(0.1, 1.0))

        needs_close = False
        if not session:
            needs_close = True
            session = aiohttp.ClientSession()

        try:
            async with session.get('https://api.submithub.com/api/proxy', timeout=15) as response:
                if response.status == 200:
                    proxy_data = await response.json()
                    proxy = ProxyConfig.from_response(proxy_data)
                    proxy.created_at = int(time.time())

                    # Test the proxy
                    if await self._test_proxy(proxy, session):
                        self.logger.info(f"New proxy acquired and validated: {proxy}")
                        return proxy
                    else:
                        self.logger.warning(f"New proxy failed validation: {proxy}")
                        return None
                else:
                    self.logger.error(f"Failed to get proxy, status: {response.status}")
                    return None
        except Exception as e:
            self.logger.error(f"Error fetching proxy: {e}")
            return None
        finally:
            if needs_close:
                await session.close()

    async def _test_proxy(self, proxy, session=None):
        needs_close = False
        if not session:
            needs_close = True
            session = aiohttp.ClientSession()

        try:
            test_url = "https://httpbin.org/ip"
            async with session.get(
                test_url,
                proxy=proxy.url,
                proxy_auth=proxy.auth,
                timeout=5
            ) as response:
                return response.status == 200
        except Exception:
            return False
        finally:
            if needs_close and session:
                await session.close()

@dataclass
class ProxyConfig:
    address: str
    port: int
    username: str
    password: str
    created_at: int = 0  

    @classmethod
    def from_response(cls, response: Dict) -> 'ProxyConfig':
        return cls(
                address=response['proxy_address'],
                port=response['ports']['http'],
                username=response['username'],
                password=response['password']
                )

    @property
    def url(self) -> str:
        return f"http://{self.address}:{self.port}"

    @property
    def auth(self) -> aiohttp.BasicAuth:
        return aiohttp.BasicAuth(self.username, self.password)

    def to_playwright_config(self) -> Dict:
        return {
                'server': self.url,
                'username': self.username,
                'password': self.password
                }

    def __str__(self):
        return f"Proxy({self.address}:{self.port})"

@dataclass
class RateLimiter:
    rate: int  # requests per second
    max_concurrent: int

    def __init__(self, rate: int = 250, max_concurrent: int = 50):
        self.rate = rate
        self.max_concurrent = max_concurrent
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._request_times: List[float] = []

    async def acquire(self):
        await self._semaphore.acquire()
        now = time.time()

        # Remove old timestamps
        self._request_times = [t for t in self._request_times if now - t < 1.0]

        if len(self._request_times) >= self.rate:
            delay = 1.0 - (now - self._request_times[0])
            if delay > 0:
                await asyncio.sleep(delay)

        self._request_times.append(time.time())

    def release(self):
        self._semaphore.release()

class GraphQLHashManager:
    """
    Manages GraphQL operation hashes for Spotify API queries
    """
    def __init__(self, redis_cache):
        self.redis_cache = redis_cache
        self.logger = logging.getLogger(__name__)
        
        # Define the operations we want to track hashes for
        self.operations = {
            #'fetchPlaylistMetadata',
            'fetchPlaylist',
            'queryArtistOverview',
            'getTrack',
            'queryArtistDiscoveredOn'
        }
        
        # Fallback hashes to use if no dynamic hash is available
        self.fallback_hashes = {
            #'fetchPlaylistMetadata': 'b2a084f6dcb11b3c8ab327dd79c9d8ac270f3b90691e8a249fad18b6f241df4a',
            'fetchPlaylist': 'b2a084f6dcb11b3c8ab327dd79c9d8ac270f3b90691e8a249fad18b6f241df4a',
            'queryArtistOverview': '4bc52527bb77a5f8bbb9afe491e9aa725698d29ab73bff58d49169ee29800167',
            'getTrack': '26cd58ab86ebba80196c41c3d48a4324c619e9a9d7df26ecca22417e0c50c6a4',
            'queryArtistDiscoveredOn': '994dde7e4c15f5ed5bae63716cfbda9fdf75bca5f534142da3397fc2596be62b'
        }
        
        # Map the operation names to endpoints for clearer logging
        self.operation_to_endpoint = {
            #'fetchPlaylistMetadata': 'playlist',
            'fetchPlaylist': 'playlist',
            'queryArtistOverview': 'artist',
            'getTrack': 'track',
            'queryArtistDiscoveredOn': 'discovered-on'
        }
        
        # Store captured hashes in memory for quick access
        self.hashes = {}
        
    async def initialize(self):
        """Load any saved hashes from Redis on startup"""
        for operation in self.operations:
            try:
                hash_value = await self.redis_cache.redis.get(f"graphql_hash_{operation}")
                if hash_value:
                    hash_value = hash_value.decode('utf-8') if isinstance(hash_value, bytes) else hash_value
                    self.hashes[operation] = hash_value
                    self.logger.info(f"Loaded {operation} hash from Redis: {hash_value[:10]}...")
            except Exception as e:
                self.logger.error(f"Error loading {operation} hash from Redis: {e}")
    
    async def get_hash(self, operation_name: str) -> str:
        """
        Get the hash for a given operation name.
        
        1. Try in-memory cache first
        2. Try Redis cache next
        3. Fall back to default hash
        """
        # First check in-memory cache
        if operation_name in self.hashes:
            return self.hashes[operation_name]
            
        # Next try Redis
        try:
            hash_value = await self.redis_cache.redis.get(f"graphql_hash_{operation_name}")
            if hash_value:
                hash_value = hash_value.decode('utf-8') if isinstance(hash_value, bytes) else hash_value
                self.hashes[operation_name] = hash_value
                return hash_value
        except Exception as e:
            self.logger.error(f"Error fetching {operation_name} hash from Redis: {e}")
        
        # Finally use fallback
        if operation_name in self.fallback_hashes:
            #self.logger.warning(f"Using fallback hash for {operation_name}")
            return self.fallback_hashes[operation_name]
            
        # If all else fails, return None
        self.logger.error(f"No hash available for {operation_name}")
        return None
        
    async def save_hash(self, operation_name: str, hash_value: str):
        """Save a hash value to both memory and Redis"""
        if not operation_name or not hash_value:
            return
            
        # Store in memory
        self.hashes[operation_name] = hash_value
        
        # Store in Redis with 30-day expiry
        try:
            await self.redis_cache.redis.set(
                f"graphql_hash_{operation_name}",
                hash_value,
                ex=86400 * 30  # 30 days
            )
            endpoint = self.operation_to_endpoint.get(operation_name, operation_name)
            self.logger.info(f"Saved {endpoint} hash to Redis: {hash_value}")
        except Exception as e:
            self.logger.error(f"Error saving {operation_name} hash to Redis: {e}")
    
    async def update_hashes(self, operation_hashes: Dict[str, str]):
        """Update multiple hashes at once"""
        for operation, hash_value in operation_hashes.items():
            if operation in self.operations:
                await self.save_hash(operation, hash_value)
                
    async def is_hash_valid(self, operation_name: str, hash_value: str) -> bool:
        """Verify if a hash is valid without making actual API calls"""
        # We could implement hash verification if needed
        # For now, just check if it's not empty
        return bool(hash_value)

class DiscoveredHashCircuitBreaker:
    def __init__(self):
        self.consecutive_failures = 0
        self.failure_threshold = 5
        self.hash_refresh_in_progress = False
        self.last_refresh_time = 0
        self.min_refresh_interval = 60  # Don't refresh more than once per minute

    async def record_failure(self, spotify_api, redis_cache):
        """Record a failure and trigger hash refresh if needed"""
        self.consecutive_failures += 1

        # If we've hit the threshold and we're not already refreshing
        if (self.consecutive_failures >= self.failure_threshold and 
            not self.hash_refresh_in_progress and 
            time.time() - self.last_refresh_time > self.min_refresh_interval):

            self.hash_refresh_in_progress = True
            try:
                logger.warning("Circuit breaker triggered - refreshing discovered hash")
                new_hash = await spotify_api._get_discovered_hash()
                if new_hash:
                    await redis_cache.save_discovered_hash(new_hash)
                    self.consecutive_failures = 0
                    logger.info("Successfully refreshed discovered hash via circuit breaker")
                self.last_refresh_time = time.time()
            except Exception as e:
                logger.error(f"Failed to refresh hash via circuit breaker: {e}")
            finally:
                self.hash_refresh_in_progress = False

    def record_success(self):
        """Record a successful request"""
        self.consecutive_failures = 0

class TokenCircuitBreaker:
    def __init__(self, failure_threshold=10, cooldown_period=60):
        self.failure_count = 0
        self.failure_threshold = failure_threshold
        self.cooldown_period = cooldown_period
        self.is_open = False
        self.last_failure_time = 0
        self._lock = asyncio.Lock()
        self.logger = logging.getLogger(__name__)
        
    async def record_failure(self):
        async with self._lock:
            self.failure_count += 1
            self.last_failure_time = time.time()
            
            if self.failure_count >= self.failure_threshold and not self.is_open:
                self.is_open = True
                self.logger.warning(f"Circuit breaker opened - cooling off for {self.cooldown_period}s")
    
    async def can_execute(self):
        async with self._lock:
            # Reset if cooldown period has passed
            if self.is_open and time.time() - self.last_failure_time > self.cooldown_period:
                self.is_open = False
                self.failure_count = 0
                self.logger.info("Circuit breaker reset after cooldown")
                
            return not self.is_open
    
    async def record_success(self):
        async with self._lock:
            if self.failure_count > 0:
                self.failure_count = max(0, self.failure_count - 1)

class SpotifyAPI:
    MAX_RETRIES = 3

    def __init__(self, db_path: str = 'spotify_cache.db'):
        self.db = AsyncDatabase(db_path)
        self.rate_limiter = RateLimiter()
        self.session: Optional[aiohttp.ClientSession] = None
        self._init_task = None
        self.access_token: Optional[str] = None
        self.client_token: Optional[str] = None
        self.proxy: Optional[ProxyConfig] = None
        self.cache = RedisCache()
        self.discovered_hash_circuit_breaker = DiscoveredHashCircuitBreaker()
        self.hash_manager = GraphQLHashManager(self.cache)
        self.circuit_breaker = TokenCircuitBreaker(failure_threshold=8, cooldown_period=15)
        self.proxy_pool = ProxyPool()
        self._proxy_validation_cache = {}  # Store validation timestamps
        self._proxy_validation_ttl = 300   # Validate every 5 minutes (adjust as needed)

    async def _ensure_initialized(self):
        if self._init_task is None:
            self._init_task = asyncio.create_task(self._initialize())
        await self._init_task

    async def _initialize(self):
        await self.db._init_db()
        self.session = aiohttp.ClientSession()
        # Initialize hash manager
        await self.hash_manager.initialize()

    async def close(self):
        """Properly close all resources"""
        if self.session:
            try:
                await self.session.close()
                logger.info("Session closed successfully")
            except Exception as e:
                logger.error(f"Error closing session: {e}")

    async def _get_proxy(self) -> Optional[ProxyConfig]:
        """Get a proxy from the proxy pool"""
        if self.proxy and time.time() - self.proxy.created_at < 1800:
            return self.proxy
            
        self.proxy = await self.proxy_pool.get_proxy(self.session)
        return self.proxy

    @asynccontextmanager
    async def _browser_session(self, retry_count: int = 0):
        """Managed browser session with proper cleanup"""
        browser = None
        context = None
        try:
            logger.info(f"Starting browser (attempt {retry_count + 1}/{self.MAX_RETRIES})")
            async with async_playwright() as p:
                browser_args = [
                    '--no-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-gpu',
                    '--disable-features=IsolateOrigins,site-per-process',
                    '--disable-web-security',
                    '--disable-blink-features=AutomationControlled',
                    '--disable-extensions',
                    '--disable-backgrounding-occluded-windows',
                    '--disk-cache-size=0',
                    '--media-cache-size=0',
                    '--aggressive-cache-discard',
                    '--disable-cache',
                    '--disable-application-cache',
                    '--disable-offline-load-stale-cache',
                    '--disable-software-rasterizer',
                    '--disable-background-networking',
                    '--ignore-certificate-errors'
                ]
                
                browser = await p.chromium.launch(
                        proxy=self.proxy.to_playwright_config() if self.proxy else None,
                        headless=True,
                        args=browser_args,
                        timeout=60000  # Increase timeout to 60s for slow environments
                        )
                # Create the context immediately after launching the browser
                context = await browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
                    viewport={"width": 1280, "height": 800},
                    locale="en-US",
                    timezone_id="America/New_York",
                    permissions=["geolocation"],
                    java_script_enabled=True,
                    bypass_csp=True
                )
                yield browser
        except Exception as e:
            logger.error(f"Browser session error: {str(e)}")
            raise
        finally:
            try:
                # Only close what's still open - use try/except for each operation
                if context:
                    try:
                        # Close context first - this will automatically close all associated pages
                        await context.close()
                    except Exception as e:
                        logger.warning(f"Error closing browser context: {str(e)}")
                if browser:
                    try:
                        await browser.close()
                    except Exception as e:
                        logger.warning(f"Error closing browser: {str(e)}")
                logger.info("Browser cleanup completed")
            except Exception as e:
                logger.error(f"Error during browser cleanup: {str(e)}")

    async def make_api_request(self, url, token_type='playlist', params=None, attempt=0, max_attempts=3):
        """
        Make an API request with automatic failover to backup tokens.
        
        This method will try the request with the current token, and if it fails,
        it will try again with backup tokens if available.
        
        Args:
            url (str): URL to request
            token_type (str): Type of token to use
            params (dict): Request parameters
            attempt (int): Current attempt number (for recursion)
            max_attempts (int): Maximum number of attempts
            
        Returns:
            dict: Response data or None if all attempts failed
        """
        if attempt >= max_attempts:
            logger.error(f"Max attempts reached for {url}")
            return None
        
        # Ensure we have a valid token
        if not await self._ensure_auth(token_type):
            logger.error(f"Failed to get valid {token_type} token")
            return None
        
        if not self.session:
            self.session = aiohttp.ClientSession()
        
        # Add client token to headers if available
        headers = {'Authorization': f'Bearer {self.access_token}'}
        if self.client_token:
            headers['client-token'] = self.client_token
        
        try:
            async with self.session.get(
                url,
                params=params,
                headers=headers,
                proxy=self.proxy.url,
                proxy_auth=self.proxy.auth,
                timeout=10
            ) as response:
                if response.status == 200:
                    # Update token health for success
                    await self.cache.update_token_health(token_type, 0, True)
                    return await response.json()
                
                # Update token health for failure
                await self.cache.update_token_health(token_type, 0, False)
                
                # If token is invalid, clear it and try a backup
                if response.status in {401, 403, 407}:
                    logger.warning(f"Token failed with status {response.status} for {url}")
                    self.access_token = None
                    self.client_token = None
                    self.proxy = None
                    
                    # Try again with a fresh token (potentially from backup)
                    return await self.make_api_request(url, token_type, params, attempt + 1, max_attempts)
                
                # For other errors, return None
                logger.warning(f"Request failed with status {response.status} for {url}")
                return None
        
        except asyncio.TimeoutError:
            logger.warning(f"Request timeout for {url}")
            # Update token health for failure
            await self.cache.update_token_health(token_type, 0, False)
            # Try again with potentially different token
            return await self.make_api_request(url, token_type, params, attempt + 1, max_attempts)
        
        except Exception as e:
            logger.error(f"Error making request to {url}: {e}")
            # Update token health for failure
            await self.cache.update_token_health(token_type, 0, False)
            
            # For network errors, try again with potentially different token
            if isinstance(e, (aiohttp.ClientConnectionError, aiohttp.ClientOSError)):
                return await self.make_api_request(url, token_type, params, attempt + 1, max_attempts)
            
            return None

    async def retry_request(self, url, token_type='playlist', params=None, max_retries=3):
        for attempt in range(max_retries):
            if attempt > 0:
                # Exponential backoff
                await asyncio.sleep(0.5 * (2 ** attempt))

            # Try to refresh tokens if previous attempt failed
            if attempt > 0 and not await self._ensure_auth(token_type):
                continue

            # Make the request
            result = await self.make_api_request(url, token_type, params)
            if result:
                return result

        return None

    async def _ensure_auth(self, token_type: str = 'playlist') -> bool:
        """
        Ensure we have a valid token with improved fallback to backup tokens.
        
        This method will try the current token first, then fall back to backup tokens
        if the current token fails. If a backup token works, it will be promoted to
        the current position.
        
        Args:
            token_type (str): Type of token to ensure
                
        Returns:
            bool: True if a valid token is available, False otherwise
        """
        # Check circuit breaker first
        if not await self.circuit_breaker.can_execute():
            logger.warning(f"Circuit breaker open, skipping {token_type} token fetch")
            return False
            
        # Quick check if we already have a valid token
        if self.access_token and self.proxy:
            # Trust tokens for a short period without validation
            if time.time() - self.proxy.created_at < 300:  # 5 minutes
                return True
                    
            # Only validate older tokens
            if await self._verify_proxy():
                # Update token health for success
                await self.cache.update_token_health(token_type, 0, True)
                return True
            else:
                # Proxy is invalid, clear it
                logger.debug("Proxy validation failed, clearing token and proxy")
                self.access_token = None
                self.client_token = None
                self.proxy = None
                    
                # Update token health for failure
                await self.cache.update_token_health(token_type, 0, False)

        # Try each token (current + backups) until we find a valid one
        for backup_index in range(4):  # 0 = current, 1-3 = backups
            try:
                if cached := await self.cache.get_token(token_type, backup_index):
                    # Try using this token
                    self.access_token = cached['access_token']
                    if 'client_token' in cached:
                        self.client_token = cached['client_token']
                    
                    proxy_data = cached['proxy']
                    if isinstance(proxy_data, str):
                        import json
                        proxy_data = json.loads(proxy_data)
                    
                    self.proxy = ProxyConfig(**proxy_data)
                    
                    # Validate the token
                    if await self._verify_proxy():
                        # Update token health for success
                        await self.cache.update_token_health(token_type, backup_index, True)
                        
                        # If this was a backup and it worked, promote it to current
                        if backup_index > 0:
                            logger.info(f"Promoting {token_type} backup {backup_index} to current position")
                            await self.cache.promote_backup_token(token_type, backup_index)
                        
                        await self.circuit_breaker.record_success()
                        return True
                    else:
                        # Update token health for failure
                        await self.cache.update_token_health(token_type, backup_index, False)
            except Exception as e:
                logger.error(f"Error trying {token_type} token backup {backup_index}: {e}")
        
        # All token attempts failed
        await self.circuit_breaker.record_failure()
        return False

    async def _verify_proxy(self) -> bool:
        """
        Verify that the current proxy is still valid, with caching to reduce overhead.
        Modified to trust recently acquired proxies without additional verification.
        """
        if not self.proxy:
            return False
            
        proxy_id = f"{self.proxy.address}:{self.proxy.port}"
        current_time = time.time()
        
        # Calculate age of the proxy
        proxy_age = current_time - self.proxy.created_at
        
        # If proxy is very fresh (less than 2 minutes old), trust it without verification
        # This assumes the token worker already verified it
        if proxy_age < 240:
            return True
        
        # If we have a recent validation result, use it
        if proxy_id in self._proxy_validation_cache:
            last_validation, is_valid = self._proxy_validation_cache[proxy_id]
            # If validated within TTL, return cached result
            if current_time - last_validation < self._proxy_validation_ttl:
                return is_valid
        
        # Need to actually validate the proxy
        if not self.session:
            self.session = aiohttp.ClientSession()
            
        try:
            # Use a very simple request to verify proxy
            test_url = "https://httpbin.org/ip"
            async with self.session.get(
                test_url,
                proxy=self.proxy.url,
                proxy_auth=self.proxy.auth,
                timeout=5  # Increased timeout
            ) as test_response:
                is_valid = test_response.status == 200
                
                # Cache the result
                self._proxy_validation_cache[proxy_id] = (current_time, is_valid)
                
                if not is_valid:
                    logger.warning(f"Proxy validation failed with status: {test_response.status}")
                
                return is_valid
        except Exception as e:
            # Cache the failure
            self._proxy_validation_cache[proxy_id] = (current_time, False)
            logger.warning(f"Proxy validation error: {e}")
            return False

    async def capture_tokens_and_hashes(self, proxy=None, use_stealth=False) -> Tuple[Optional[str], Optional[str], Dict[str, str]]:
        """
        Extract Spotify tokens and GraphQL operation hashes by monitoring network requests
        
        Returns:
            Tuple[str, str, Dict[str, str]]: (access_token, client_token, operation_hashes)
        """
        access_token = None
        client_token = None
        operation_hashes = {}
        token_event = asyncio.Event()
        operations_seen = set()  # Moved here from nonlocal declaration
        
        # Track which operations we've seen
        operations_to_capture = {
            'fetchPlaylist', 
            'queryArtistOverview', 
            'getTrack', 
            'queryArtistDiscoveredOn'
        }

        try:
            self.logger.info(f"Starting token capture with stealth={use_stealth}")
            async with async_playwright() as p:
                browser_args = [
                    '--no-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-blink-features=AutomationControlled',
                    '--disable-extensions',
                    '--disable-features=IsolateOrigins,site-per-process',
                    '--disable-site-isolation-trials'
                ]
                
                # Add stealth mode args
                if use_stealth:
                    browser_args.extend([
                        '--disable-web-security',
                        '--disable-features=IsolateOrigins',
                        '--disable-site-isolation-trials',
                        '--disable-setuid-sandbox',
                        '--no-default-browser-check',
                        '--no-first-run',
                        '--no-zygote',
                        '--disable-accelerated-2d-canvas',
                        '--disable-accelerated-video-decode',
                        '--disable-gpu'
                    ])
                
                browser = await p.chromium.launch(
                    proxy=proxy.to_playwright_config() if proxy else None,
                    headless=True,
                    args=browser_args
                )

                try:
                    # Create a context with a credible user agent
                    context = await browser.new_context(
                        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
                        viewport={"width": 1280, "height": 800},
                        locale="en-US",
                        timezone_id="America/New_York",
                        permissions=["geolocation"],
                        java_script_enabled=True
                    )
                    
                    page = await context.new_page()
                    
                    # Intercept network requests to extract tokens and hashes
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
                                    self.logger.info(f"Found access token: {access_token[:15]}...")
                            
                            if "client-token" in headers:
                                current_client_token = headers["client-token"]
                                
                                if not client_token or client_token != current_client_token:
                                    client_token = current_client_token
                                    self.logger.info(f"Found client token: {client_token[:15]}...")
                            
                            # Extract GraphQL operation hashes from URL
                            try:
                                if "operationName=" in url and "extensions=" in url:
                                    # Parse URL to get operationName
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
                                                        endpoint = self.hash_manager.operation_to_endpoint.get(operation_name, operation_name)
                                                        self.logger.info(f"Found hash for {endpoint}: {hash_value}")
                                                except json.JSONDecodeError:
                                                    pass
                            except Exception as hash_e:
                                self.logger.error(f"Error extracting hash: {hash_e}")
                            
                            # Signal that we've found both tokens
                            if access_token and client_token:
                                token_event.set()
                        
                        # Continue the request
                        await route.continue_()
                    
                    # Set up the route handler
                    await page.route("**/*", intercept_request)
                    
                    # Navigate to Spotify
                    self.logger.info("Navigating to Spotify main page...")
                    #await page.goto('https://open.spotify.com', timeout=60000)
                    try:
                        await page.goto('https://open.spotify.com',
                                       timeout=30000,  # Shorter timeout
                                       wait_until='domcontentloaded')  # Don't wait for full page load
                    except Exception as e:
                        self.logger.warning(f"Initial navigation error: {e}, trying simplified approach")
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
                        self.logger.info("Successfully extracted tokens")
                    except asyncio.TimeoutError:
                        self.logger.warning("Timeout waiting for tokens")
                    
                    # Now try to capture hashes for each type of operation that we're missing
                    # We'll visit specific pages to trigger the API calls we need
                    
                    # Try to capture playlist hash if missing
                    #if 'fetchPlaylistMetadata' not in operations_seen:
                    if 'fetchPlaylist' not in operations_seen:
                        self.logger.info("Navigating to playlist page to capture hash...")
                        await page.goto('https://open.spotify.com/playlist/37i9dQZEVXcJZyENOWUFo7', timeout=60000)
                        await page.wait_for_load_state('networkidle', timeout=30000)
                        await asyncio.sleep(3)
                    
                    # Try to capture track hash if missing
                    if 'getTrack' not in operations_seen:
                        self.logger.info("Navigating to track page to capture hash...")
                        await page.goto('https://open.spotify.com/track/4cOdK2wGLETKBW3PvgPWqT', timeout=60000)
                        await page.wait_for_load_state('networkidle', timeout=30000)
                        await asyncio.sleep(3)
                    
                    # Try to capture artist hash if missing
                    if 'queryArtistOverview' not in operations_seen:
                        self.logger.info("Navigating to artist page to capture hash...")
                        await page.goto('https://open.spotify.com/artist/3GBPw9NK25X1Wt2OUvOwY3', timeout=60000)
                        await page.wait_for_load_state('networkidle', timeout=30000)
                        await asyncio.sleep(3)
                    
                    # Try to capture discovered-on hash if missing
                    if 'queryArtistDiscoveredOn' not in operations_seen:
                        self.logger.info("Navigating to discovered-on page to capture hash...")
                        await page.goto('https://open.spotify.com/artist/3GBPw9NK25X1Wt2OUvOwY3/discovered-on', timeout=60000)
                        await page.wait_for_load_state('networkidle', timeout=30000)
                        await asyncio.sleep(3)
                    
                    # Return what we found
                    return access_token, client_token, operation_hashes
                    
                finally:
                    await browser.close()
                    self.logger.info("Browser closed")
                    
        except Exception as e:
            self.logger.error(f"Error in token and hash extraction: {e}")
            import traceback
            self.logger.error(f"Traceback: {traceback.format_exc()}")
        
        return access_token, client_token, operation_hashes

    async def _get_proxy(self) -> Optional[ProxyConfig]:
        if self.proxy and time.time() - self.proxy.created_at < 1800:  # 30-minute reuse
            return self.proxy

        # Try to get a new proxy with retries
        for attempt in range(3):
            try:
                if not self.session:
                    self.session = aiohttp.ClientSession()

                async with self.session.get('https://api.submithub.com/api/proxy', timeout=15) as response:
                    if response.status == 200:
                        proxy_data = await response.json()
                        # Test the proxy before returning it
                        proxy = ProxyConfig.from_response(proxy_data)
                        
                        # Verify proxy works with a simple connection test
                        test_url = "https://httpbin.org/ip"
                        try:
                            async with self.session.get(
                                test_url,
                                proxy=proxy.url,
                                proxy_auth=proxy.auth,
                                timeout=5
                            ) as test_response:
                                if test_response.status == 200:
                                    logger.info(f"Proxy test successful: {proxy}")
                                    self.proxy = proxy
                                    return proxy
                                else:
                                    logger.error(f"Proxy test failed with status: {test_response.status}")
                        except Exception as e:
                            logger.error(f"Proxy test connection error: {e}")
                            await asyncio.sleep(1)  # Brief pause before retry
                            continue
                    logger.error(f"Failed to get proxy, status: {response.status}")
            except Exception as e:
                logger.error(f"Proxy error: {e}")
                await asyncio.sleep(1)
                
        # If we reach here, all attempts failed
        # Use a fallback from the cache if available
        try:
            # Try to find any valid token in cache and use its proxy
            for token_type in ['playlist', 'artist', 'track']:
                if cached := await self.cache.get_token(token_type):
                    proxy_data = cached['proxy']
                    if isinstance(proxy_data, str):
                        import json
                        proxy_data = json.loads(proxy_data)
                    
                    # Create proxy object
                    proxy = ProxyConfig(**proxy_data)
                    logger.warning(f"Using fallback proxy from cache: {proxy}")
                    self.proxy = proxy
                    return proxy
        except Exception as e:
            logger.error(f"Error using fallback proxy: {e}")
            
        return None

    async def get_playlists(
            self,
            playlist_ids: List[str],
            with_tracks: bool = False,
            skip_cache: bool = False,
            raw_data: bool = False,
            ) -> Dict[str, Optional[Dict]]:
        await self._ensure_initialized()
        # Get unique playlist IDs while preserving order
        unique_ids = list(dict.fromkeys(playlist_ids))

        if skip_cache:
            # If skipping cache, fetch all playlists directly
            fetch_tasks = [
                    self._fetch_playlist(pid, with_tracks)
                    for pid in unique_ids
                    ]
            fetched_playlists = await asyncio.gather(*fetch_tasks, return_exceptions=True)
            valid_playlists = [
                    p for p in fetched_playlists
                    if isinstance(p, dict) and p.get('id')
                    ]
            # Still save to cache for future requests
            if valid_playlists:
                await self.db.save_playlists(valid_playlists)

            if raw_data:
                return {p['id']: p for p in valid_playlists}
            else:
                return {p['id']: self._format_playlist(p, with_tracks) for p in valid_playlists}

        # Get all cached playlists
        cached_playlists = await self.db.get_playlists(unique_ids)
        # Determine which playlists need to be fetched
        to_fetch = []
        for pid in unique_ids:
            cached = cached_playlists.get(pid)
            needs_fetch = (
                    not cached or  # Not cached
                    not isinstance(cached, dict) or  # Cached data is invalid
                    (with_tracks and not self._has_track_data(cached))  # Cached but missing track data
                    )
            if needs_fetch:
                to_fetch.append(pid)
        # Fetch missing or outdated playlists
        if to_fetch:
            fetch_tasks = [
                    self._fetch_playlist(pid, with_tracks)
                    for pid in to_fetch
                    ]
            fetched_playlists = await asyncio.gather(*fetch_tasks, return_exceptions=True)
            valid_playlists = [
                    p for p in fetched_playlists
                    if isinstance(p, dict) and p.get('id')
                    ]
            if valid_playlists:
                # Save new playlists to cache
                await self.db.save_playlists(valid_playlists)
                # Update our working set with new data
                cached_playlists.update({p['id']: p for p in valid_playlists})

        # Format and return results, filtering out invalid cached entries
        if raw_data:
            return {
                    pid: data
                    for pid, data in cached_playlists.items()
                    if data and isinstance(data, dict) and not (with_tracks and not self._has_track_data(data))
                    }
        else:
            return {
                    pid: self._format_playlist(data, with_tracks) if data and isinstance(data, dict) else None
                    for pid, data in cached_playlists.items()
                    if not (with_tracks and data and not self._has_track_data(data))
                    }

    @staticmethod
    def _has_track_data(playlist: Optional[Dict]) -> bool:
        """
        Check if a playlist has track data cached.

        Args:
            playlist (Optional[Dict]): The playlist data to check

        Returns:
            bool: True if the playlist has track data, False otherwise
        """
        # Handle None case
        if not playlist:
            return False

        # Handle case where playlist might be a string
        if not isinstance(playlist, dict):
            return False

        tracks = playlist.get('tracks', {})
        # Handle case where tracks might be a number (total) instead of dict
        if not isinstance(tracks, dict):
            return False

        return bool(tracks.get('items', []))

    async def _fetch_playlist(self, playlist_id: str, with_tracks: bool = False) -> Optional[Dict]:
        """Fetch playlist data using Spotify's REST API with improved reliability"""
        try:
            await self.rate_limiter.acquire()

            # REST API for playlists
            url = f"https://api.spotify.com/v1/playlists/{playlist_id}"
            
            if with_tracks:
                fields = "id,name,description,owner(id,display_name),followers(total),images(url),collaborative,tracks(total,items(added_at,track(id,name,duration_ms,preview_url,artists(id,name),album(images(url)))))"
            else:
                fields = "id,name,description,owner(id,display_name),followers(total),images(url),tracks(total),collaborative"

            params = {'fields': fields}

            # Use the make_api_request helper instead of direct API call
            playlist_data = await self.retry_request(
                url=url,
                token_type='playlist',
                params=params
            )
            
            if not playlist_data:
                logger.warning(f"Failed to fetch playlist {playlist_id}")
                return None

            # If tracks are requested, fetch them with pagination
            if with_tracks:
                all_tracks = []
                # Calculate how many tracks to fetch (200)
                total_tracks = min(playlist_data.get('tracks', {}).get('total', 0), 200)
                offset = 0
                limit = 100  # Spotify's max limit per request

                while offset < total_tracks:
                    tracks_url = f"https://api.spotify.com/v1/playlists/{playlist_id}/tracks"
                    tracks_params = {
                        'offset': offset,
                        'limit': limit,
                        'fields': 'items(added_at,track(id,artists(id,name),name,preview_url,duration_ms,album(images(url))))'
                    }

                    tracks_data = await self.retry_request(
                        url=tracks_url,
                        token_type='playlist',
                        params=tracks_params
                    )
                    
                    if not tracks_data:
                        logger.warning(f"Failed to fetch tracks for playlist {playlist_id} at offset {offset}")
                        break
                    
                    all_tracks.extend(tracks_data.get('items', []))
                    offset += limit

                # Update the playlist data with all fetched tracks
                playlist_data['tracks']['items'] = all_tracks

            return playlist_data

        except Exception as e:
            logger.error(f"Error fetching playlist {playlist_id}: {e}")
            return None
        finally:
            self.rate_limiter.release()

    @staticmethod
    def _format_playlist(playlist: Dict, with_tracks: bool = False) -> Dict:
        """
        Format a playlist response from Spotify API into our standard format.
        If the playlist is already formatted, return it as is.
        """
        # If the playlist is already formatted (has our standard fields), return it
        if isinstance(playlist, dict):
            # Check if this is already in our format
            standard_fields = {'id', 'name', 'owner', 'owner_id', 'description', 'followers', 'images'}
            if standard_fields.issubset(playlist.keys()):
                return playlist

        # If not formatted, format it from Spotify API response
        base_data = {
                'id': playlist.get('id'),
                'name': playlist.get('name'),
                'owner': playlist.get('owner', {}).get('display_name'),
                'owner_id': playlist.get('owner', {}).get('id'),
                'description': playlist.get('description'),
                'followers': playlist.get('followers', {}).get('total'),
                'images': playlist.get('images', []),
                'collaborative': playlist.get('collaborative', False)
                }

        if not with_tracks:
            base_data['tracks'] = playlist.get('tracks', {}).get('total')
            return base_data

        # Format track data when requested
        tracks_data = playlist.get('tracks', {})
        formatted_tracks = []

        for item in tracks_data.get('items', []):
            track = item.get('track', {})
            if not track:
                continue

            artists = [
                    {
                        'id': artist.get('id'),
                        'name': artist.get('name')
                        }
                    for artist in track.get('artists', [])
                    if artist.get('id') and artist.get('name')
                    ]

            formatted_tracks.append({
                'added_at': item.get('added_at'),
                'track': {
                    'id': track.get('id'),
                    'name': track.get('name'),
                    'album': track.get('album'),
                    'duration_ms': track.get('duration_ms'),
                    'preview_url': track.get('preview_url'),
                    'artists': artists
                    }
                })

        base_data['tracks'] = {
                'total': tracks_data.get('total'),
                'items': formatted_tracks
                }

        return base_data

    async def refresh_token(self, token_type: str = 'playlist') -> bool:
        """Background task to refresh token if existing token is > 5 minutes old"""
        if cached := await self.cache.get_token(token_type):
            proxy_data = cached['proxy']
            if isinstance(proxy_data, str):
                proxy_data = json.loads(proxy_data)

            # If token is less than 5 minutes old, skip refresh
            if int(time.time()) - proxy_data.get('created_at', 0) < 300:
                return True

        # Use appropriate token getter based on type
        if token_type == 'track':
            return await self._get_track_token()
        else:
            return await self._get_token(token_type)

    async def get_artists(
            self,
            artist_ids: List[str],
            skip_cache: bool = False,
            detail: bool = False,
            official: bool = False
            ) -> Dict[str, Optional[Dict]]:
        await self._ensure_initialized()
        unique_ids = list(dict.fromkeys(artist_ids))

        # Add prefix to IDs for cache lookup
        prefix = "official_" if official else "partner_"
        cache_ids = [f"{prefix}{aid}" for aid in unique_ids]

        if skip_cache:
            fetch_method = self._fetch_artist_official if official else self._fetch_artist
            format_method = self._format_artist_official if official else self._format_artist

            fetch_tasks = [fetch_method(aid) for aid in unique_ids]
            fetched_artists = await asyncio.gather(*fetch_tasks, return_exceptions=True)

            valid_artists = []
            formatted_results = {}

            for i, result in enumerate(fetched_artists):
                if isinstance(result, dict):
                    formatted = format_method(result, detail)
                    if formatted:
                        artist_id = unique_ids[i]
                        cache_id = f"{prefix}{artist_id}"
                        valid_artists.append((cache_id, formatted))
                        formatted_results[artist_id] = formatted

            # Save to cache
            if valid_artists:
                await self.db.save_artists(valid_artists)

            return formatted_results

        # Get all cached artists
        cached_artists = await self.db.get_artists(cache_ids)

        # Determine which artists need to be fetched
        to_fetch = [
                aid for aid in unique_ids
                if not cached_artists.get(f"{prefix}{aid}") or not isinstance(cached_artists.get(f"{prefix}{aid}"), dict)
                ]

        # Fetch missing artists
        if to_fetch:
            fetch_method = self._fetch_artist_official if official else self._fetch_artist
            format_method = self._format_artist_official if official else self._format_artist

            fetch_tasks = [fetch_method(aid) for aid in to_fetch]
            fetched_artists = await asyncio.gather(*fetch_tasks, return_exceptions=True)
            valid_artists = []

            for i, result in enumerate(fetched_artists):
                if isinstance(result, dict):
                    formatted = format_method(result, detail)
                    if formatted:
                        artist_id = to_fetch[i]
                        cache_id = f"{prefix}{artist_id}"
                        valid_artists.append((cache_id, formatted))
                        cached_artists[cache_id] = formatted

            # Save to cache
            if valid_artists:
                await self.db.save_artists(valid_artists)

        # Return results without prefix
        return {
                aid: cached_artists.get(f"{prefix}{aid}")
                for aid in unique_ids
                if cached_artists.get(f"{prefix}{aid}")
                }

    async def _fetch_artist_official(self, artist_id: str) -> Optional[Dict]:
        """Fetch artist data from Spotify's public API with client token"""
        try:
            await self.rate_limiter.acquire()

            if not await self._ensure_auth():
                logger.error("Failed to get auth token")
                return None

            if not self.session:
                self.session = aiohttp.ClientSession()

            if not self.proxy:
                logger.debug("No proxy available")
                return None

            url = f"https://api.spotify.com/v1/artists/{artist_id}"

            # Add client token to headers if available
            headers = {'Authorization': f'Bearer {self.access_token}'}
            
            if hasattr(self, 'client_token') and self.client_token:
                headers['client-token'] = self.client_token

            async with self.session.get(
                    url,
                    headers=headers,
                    proxy=self.proxy.url,
                    proxy_auth=self.proxy.auth,
                    timeout=10
                    ) as response:
                if response.status == 200:
                    return await response.json()
                elif response.status in {401, 407}:
                    logger.error(f"Authorization error {response.status}")
                    self.access_token = None
                    self.client_token = None
                    self.proxy = None
                    return None

                logger.warning(f"Failed to fetch artist {artist_id}: {response.status}")
                return None

        except Exception as e:
            logger.error(f"Error fetching artist {artist_id}: {e}")
            return None
        finally:
            self.rate_limiter.release()

    @staticmethod
    def _format_artist_official(artist_data: Dict, detail: bool = False) -> Optional[Dict]:
        """Format an artist response from Spotify's public API"""
        # If detail is True, return the full response
        if detail:
            return artist_data

        if not artist_data:
            return None

        formatted_data = {
                'id': artist_data.get('id'),
                'profile': {
                    'name': artist_data.get('name'),
                    },
                'stats': {
                    'followers': artist_data.get('followers', {}).get('total'),
                    'popularity': artist_data.get('popularity'),  # Popularity score from official API
                    }
                }

        # Add images if available
        if artist_data.get('images'):
            formatted_data['image'] = artist_data['images'][0].get('url')

        # Add genres if available
        if artist_data.get('genres'):
            formatted_data['genres'] = artist_data['genres']

        # Make sure we have at least an ID and name
        if not formatted_data.get('id') or not formatted_data.get('profile', {}).get('name'):
            return None

        return formatted_data

    async def _fetch_artist(self, artist_id: str) -> Optional[Dict]:
        """Fetch artist data using dynamically captured hashes with improved reliability"""
        try:
            await self.rate_limiter.acquire()

            url = "https://api-partner.spotify.com/pathfinder/v1/query"

            variables = {
                'uri': f'spotify:artist:{artist_id}',
                'locale': '',
                'includePrerelease': False,
                'enableAssociatedVideos': False,
                'withSaved': True
            }

            # Get hash from hash manager
            artist_hash = await self.hash_manager.get_hash('queryArtistOverview')
            if not artist_hash:
                logger.error("No artist hash available")
                return None

            params = {
                'operationName': 'queryArtistOverview',
                'variables': json.dumps(variables),
                'extensions': json.dumps({
                    'persistedQuery': {
                        'version': 1,
                        'sha256Hash': artist_hash
                    }
                })
            }

            # Use the make_api_request helper
            data = await self.retry_request(
                url=url,
                token_type='artist',
                params=params
            )
            
            if not data:
                return None

            # Check for GraphQL errors
            if 'errors' in data:
                error_msg = data.get('errors', [{}])[0].get('message', '')
                
                if 'PersistedQueryNotFound' in error_msg:
                    # Clear hash to force refresh
                    await self.hash_manager.save_hash('queryArtistOverview', None)
                    logger.warning("Artist hash invalidated, cleared from cache")
                    return None
                
                elif 'NullValueInNonNullableField' in error_msg or 'Server error' in error_msg:
                    # Handle the null field error gracefully
                    if 'data' in data and 'artistUnion' in data['data']:
                        # Fix relatedContent null value
                        if data['data']['artistUnion'] is not None and 'relatedContent' not in data['data']['artistUnion']:
                            logger.info(f"Adding missing relatedContent for artist {artist_id}")
                            data['data']['artistUnion']['relatedContent'] = {
                                "discoveredOnV2": {"items": []},
                                "featuring": {"items": []},
                                "featuringV2": {"items": []},
                                "appearsOn": {"items": []},
                                "appearsOnV2": {"items": []},
                                "playlists": {"items": []}
                            }
                        
                        # If artistUnion is present but relatedContent is null
                        if data['data']['artistUnion'] is not None and data['data']['artistUnion'].get('relatedContent') is None:
                            logger.info(f"Fixing null relatedContent for artist {artist_id}")
                            data['data']['artistUnion']['relatedContent'] = {
                                "discoveredOnV2": {"items": []},
                                "featuring": {"items": []},
                                "featuringV2": {"items": []},
                                "appearsOn": {"items": []},
                                "appearsOnV2": {"items": []},
                                "playlists": {"items": []}
                            }
                        
                        # Fix null items in featuringV2
                        if data['data']['artistUnion'] is not None and 'relatedContent' in data['data']['artistUnion']:
                            related = data['data']['artistUnion']['relatedContent']
                            
                            # Fix featuringV2 items
                            if related and 'featuringV2' in related and related.get('featuringV2') is not None:
                                featuring = related['featuringV2']
                                if 'items' in featuring:
                                    for i, item in enumerate(featuring['items']):
                                        if item is None or 'data' not in item or item['data'] is None:
                                            featuring['items'][i] = {'data': {'__placeholder': True}}
                            elif related and ('featuringV2' not in related or related.get('featuringV2') is None):
                                related['featuringV2'] = {"items": []}
                            
                            # Fix discoveredOnV2 field if missing or null
                            if related and ('discoveredOnV2' not in related or related.get('discoveredOnV2') is None):
                                related['discoveredOnV2'] = {"items": []}
                            
                            # Ensure other sections exist to prevent potential null issues
                            sections = ['featuring', 'appearsOn', 'appearsOnV2', 'playlists']
                            for section in sections:
                                if related and (section not in related or related.get(section) is None):
                                    related[section] = {"items": []}
                        
                        # Set default saved value if missing
                        if data['data']['artistUnion'] is not None and 'saved' not in data['data']['artistUnion']:
                            data['data']['artistUnion']['saved'] = False
                        
                        # The modified response should now be usable
                        return data
                
                # For other types of errors, log them in detail
                logger.error(f"GraphQL errors in response for artist {artist_id}: {data['errors']}")
                return None

            return data

        except Exception as e:
            logger.error(f"Error fetching artist {artist_id}: {e}")
            return None
        finally:
            self.rate_limiter.release()

    @staticmethod
    def _format_artist(artist_data: Dict, detail: bool = False) -> Optional[Dict]:
        """Format an artist response from Spotify Partner API"""
        # If detail is True, return the full response
        if detail:
            return artist_data.get('data', {}).get('artistUnion', {})

        # Extract artist data
        artist = artist_data.get('data', {}).get('artistUnion', {})
        if not artist:
            return None

        # Construct base profile
        formatted_data = {
                'id': artist.get('id'),
                'profile': {
                    'name': artist.get('profile', {}).get('name'),
                    }
                }

        # Add stats if available
        if 'stats' in artist:
            formatted_data['stats'] = {
                    'followers': artist.get('stats', {}).get('followers'),
                    'monthlyListeners': artist.get('stats', {}).get('monthlyListeners'),
                    'topCities': artist.get('stats', {}).get('topCities', [])
                    }

        # Add visuals if available
        if 'visuals' in artist:
            avatar_image = artist.get('visuals', {}).get('avatarImage', {})
            if avatar_image and avatar_image.get('sources'):
                formatted_data['image'] = avatar_image.get('sources')[0].get('url')

        # Add genres if available
        if artist.get('profile', {}).get('genres', {}).get('items'):
            formatted_data['genres'] = [
                    genre.get('name')
                    for genre in artist['profile']['genres']['items']
                    if genre.get('name')
                    ]

        # Make sure we have at least an ID and name
        if not formatted_data.get('id') or not formatted_data.get('profile', {}).get('name'):
            return None

        return formatted_data

    async def _fetch_discovered_on(self, artist_id: str) -> Optional[Dict]:
        """Fetch discovered-on data with improved reliability"""
        try:
            await self.rate_limiter.acquire()

            # Use the helper method to get the hash
            discovered_hash = await self._ensure_discovered_hash()
            if not discovered_hash:
                logger.error("Could not get discovered-on hash")
                return None

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

            # Use the make_api_request helper
            data = await self.retry_request(
                url=url,
                token_type='artist',  # Use artist token type for discovered-on
                params=params
            )
            
            if not data:
                await self.discovered_hash_circuit_breaker.record_failure(self, self.cache)
                return None

            # Check for GraphQL errors
            if 'errors' in data:
                logger.error(f"GraphQL errors in response: {data['errors']}")
                # If we get PersistedQueryNotFound, clear the cached hash
                if any(error.get('message') == 'PersistedQueryNotFound' for error in data.get('errors', [])):
                    await self.cache.redis.delete('spotify_discovered_hash')
                    # Trigger the circuit breaker
                    await self.discovered_hash_circuit_breaker.record_failure(self, self.cache)
                return None

            # Record success
            self.discovered_hash_circuit_breaker.record_success()
            return data

        except Exception as e:
            logger.error(f"Error fetching discovered-on {artist_id}: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            # Track failures on exceptions
            await self.discovered_hash_circuit_breaker.record_failure(self, self.cache)
            return None
        finally:
            self.rate_limiter.release()

    async def _get_discovered_hash(self) -> Optional[str]:
        """Get the discovered-on hash by monitoring actual Spotify web requests"""
        logger.info("Getting discovered-on hash from live request")

        try:
            if not self.proxy:
                self.proxy = await self._get_proxy()
                if not self.proxy:
                    logger.warning("Failed to get proxy for hash refresh - using cached hash if available")
                    # Try to use existing hash
                    return await self.hash_manager.get_hash('queryArtistDiscoveredOn')

            # Use a separate variable for the browser to avoid context issues
            discovered_hash = None
            
            try:
                async with async_playwright() as p:
                    browser = await p.chromium.launch(
                        proxy=self.proxy.to_playwright_config() if self.proxy else None,
                        headless=True,
                        args=[
                            '--no-sandbox',
                            '--disable-dev-shm-usage',
                            '--disable-gpu',
                        ]
                    )
                    
                    # Create a new context explicitly
                    context = await browser.new_context()
                    page = await context.new_page()
                    
                    hash_event = asyncio.Event()

                    async def handle_request(route):
                        nonlocal discovered_hash
                        request = route.request
                        if 'queryArtistDiscoveredOn' in request.url:
                            logger.info(f"Found queryArtistDiscoveredOn request: {request.url}")
                            try:
                                parts = request.url.split('=')
                                for part in parts:
                                    try:
                                        data = json.loads(urllib.parse.unquote(part))
                                        if data.get('persistedQuery', {}).get('sha256Hash'):
                                            discovered_hash = data['persistedQuery']['sha256Hash']
                                            logger.info(f"Found hash: {discovered_hash}")
                                            hash_event.set()
                                    except json.JSONDecodeError:
                                        continue
                            except Exception as e:
                                logger.error(f"Error parsing URL: {e}")
                        await route.continue_()

                    # Listen to network requests
                    await page.route("**/*", handle_request)

                    # Visit a known artist page
                    artist_id = "3GBPw9NK25X1Wt2OUvOwY3"
                    await page.goto(f'https://open.spotify.com/artist/{artist_id}/discovered-on', timeout=60000)

                    # Wait for hash with timeout
                    try:
                        await asyncio.wait_for(hash_event.wait(), timeout=30)
                    except asyncio.TimeoutError:
                        logger.error("Timeout waiting for discovered hash")
                        return None

                    # Cleanup routes before closing
                    try:
                        await page.unroute_all(behavior='ignoreErrors')
                        await page.close()  # Remove timeout parameter
                    except Exception as e:
                        logger.warning(f"Error during page cleanup: {e}")

                    # Cleanup context and browser with proper error handling
                    try:
                        await context.close()  # Remove timeout parameter
                    except Exception as e:
                        logger.warning(f"Error closing context: {e}")
 
                    try:
                        await browser.close()
                    except Exception as e:
                        logger.warning(f"Error closing browser: {e}")

                    if discovered_hash:
                        # Save the hash in Redis for future use
                        await self.cache.save_discovered_hash(discovered_hash)
                        return discovered_hash
                    else:
                        logger.error("Failed to capture discovered-on hash")
                        return None
                        
            except Exception as e:
                logger.error(f"Error in browser initialization: {e}")
                return None

        except Exception as e:
            logger.error(f"Error getting discovered-on hash: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return None

    async def _ensure_discovered_hash(self) -> Optional[str]:
        """Ensures we have a valid discovered-on hash, waiting for background refresh if needed"""
        for retry in range(3):
            if discovered_hash := await self.cache.get_discovered_hash():
                return discovered_hash
            await asyncio.sleep(2)
        return None

    @staticmethod
    def _format_discovered_on(data: Dict) -> Optional[Dict]:
        """Format discovered-on response from Spotify Partner API"""
        # Extract data
        artist_data = data.get('data', {}).get('artistUnion', {})
        if not artist_data:
            return None

        # Get the discovered on items
        discovered_items = artist_data.get('relatedContent', {}).get('discoveredOnV2', {}).get('items', [])
        if not discovered_items:
            return None

        # Format playlists
        clean_items = []
        for item in discovered_items:
            playlist_data = item.get('data', {})
            if playlist_data.get('__typename') != 'Playlist':
                continue

            image_url = None
            if playlist_data.get('images', {}).get('items'):
                image_sources = playlist_data['images']['items'][0].get('sources', [])
                if image_sources:
                    image_url = image_sources[0].get('url')

            clean_items.append({
                'id': playlist_data['uri'].split(':')[-1],
                'name': playlist_data.get('name'),
                'owner': playlist_data.get('ownerV2', {}).get('data', {}).get('name'),
                'position': len(clean_items),  # Maintain order
                'image': image_url
                })

        return {
                'data': {
                    'artist': {
                        'id': artist_data.get('id'),
                        'relatedContent': {
                            'discoveredOn': {
                                'items': clean_items
                                }
                            }
                        }
                    }
                }

    async def get_discovered_on(
            self,
            artist_ids: List[str],
            skip_cache: bool = False,
            ) -> Dict[str, Optional[Dict]]:
        await self._ensure_initialized()

        # Get unique artist IDs while preserving order
        unique_ids = list(dict.fromkeys(artist_ids))

        if skip_cache:
            fetch_tasks = [
                    self._fetch_discovered_on(aid)
                    for aid in unique_ids
                    ]
            fetched_data = await asyncio.gather(*fetch_tasks, return_exceptions=True)

            results = {}
            for i, data in enumerate(fetched_data):
                if isinstance(data, dict):
                    formatted = self._format_discovered_on(data)
                    if formatted:
                        results[unique_ids[i]] = formatted

            return results

        # Get all cached data
        cached_data = await self.db.get_discovered_on(unique_ids)

        # Determine which need to be fetched
        to_fetch = [
                aid for aid in unique_ids
                if not cached_data.get(aid) or not isinstance(cached_data.get(aid), dict)
                ]

        # Fetch missing data
        if to_fetch:
            fetch_tasks = [
                    self._fetch_discovered_on(aid)
                    for aid in to_fetch
                    ]
            fetched_data = await asyncio.gather(*fetch_tasks, return_exceptions=True)

            valid_data = []
            for i, data in enumerate(fetched_data):
                if isinstance(data, dict):
                    formatted = self._format_discovered_on(data)
                    if formatted:
                        formatted['id'] = to_fetch[i]  # Add ID for caching
                        valid_data.append(formatted)

            # Save to cache and update results
            if valid_data:
                await self.db.save_discovered_on(valid_data)
                cached_data.update({d['id']: d for d in valid_data})

        return {
                aid: cached_data.get(aid)
                for aid in unique_ids
                if cached_data.get(aid)
                }

    async def _get_track_token(self) -> bool:
        """Get a new track token using network monitoring approach."""
        return await self._get_token('track')  # Just reuse the common method

    async def get_tracks(
            self,
            track_ids: List[str],
            skip_cache: bool = False,
            detail: bool = False,
            official: bool = False  # Add official parameter
            ) -> Dict[str, Optional[Dict]]:
        """Get track data for multiple track IDs, optionally using Spotify's official API"""
        await self._ensure_initialized()
        unique_ids = list(dict.fromkeys(track_ids))

        # When using official API, skip cache for simplicity
        if official or skip_cache:
            # Choose the appropriate fetch method based on official flag
            fetch_method = self._fetch_track_official if official else self._fetch_track
            
            # For official API, we'll return raw data rather than formatting it
            format_method = (lambda x, _: x) if official else self._format_track

            fetch_tasks = [
                    fetch_method(tid)
                    for tid in unique_ids
                    ]
            fetched_data = await asyncio.gather(*fetch_tasks, return_exceptions=True)

            results = {}
            for i, data in enumerate(fetched_data):
                if isinstance(data, dict):
                    formatted = format_method(data, detail)
                    if formatted:
                        results[unique_ids[i]] = formatted

            # Only cache non-official data
            if not official and results:
                valid_tracks = []
                for track_id, track_data in results.items():
                    if track_data:
                        track_copy = track_data.copy()
                        track_copy['id'] = track_id
                        valid_tracks.append(track_copy)
                
                if valid_tracks:
                    await self.db.save_tracks(valid_tracks)

            return results

        # Standard non-official cached fetch (existing code)
        # Get all cached data
        cached_data = await self.db.get_tracks(unique_ids)

        # Determine which need to be fetched
        to_fetch = [
                tid for tid in unique_ids
                if not cached_data.get(tid) or not isinstance(cached_data.get(tid), dict)
                ]

        # Fetch missing data
        if to_fetch:
            fetch_tasks = [
                    self._fetch_track(tid, detail)
                    for tid in to_fetch
                    ]
            fetched_data = await asyncio.gather(*fetch_tasks, return_exceptions=True)

            valid_data = []
            for i, data in enumerate(fetched_data):
                if isinstance(data, dict):
                    formatted = self._format_track(data, detail)
                    if formatted:
                        formatted['id'] = to_fetch[i]  # Add ID for caching
                        valid_data.append(formatted)

            # Save to cache and update results
            if valid_data:
                await self.db.save_tracks(valid_data)
                cached_data.update({d['id']: d for d in valid_data})

        return {
                tid: cached_data.get(tid)
                for tid in unique_ids
                if cached_data.get(tid)
                }

    async def _fetch_track(self, track_id: str, detail: bool = False) -> Optional[Dict]:
        """Fetch track data using dynamically captured hashes with improved reliability"""
        try:
            await self.rate_limiter.acquire()

            url = "https://api-partner.spotify.com/pathfinder/v1/query"

            variables = {
                'uri': f'spotify:track:{track_id}'
            }

            # Get the hash from the hash manager
            track_hash = await self.hash_manager.get_hash('getTrack')
            
            if not track_hash:
                logger.error("No hash available for track query")
                return None

            params = {
                'operationName': 'getTrack',
                'variables': json.dumps(variables),
                'extensions': json.dumps({
                    'persistedQuery': {
                        'version': 1,
                        'sha256Hash': track_hash
                    }
                })
            }

            # Use the make_api_request helper
            data = await self.retry_request(
                url=url,
                token_type='track',
                params=params
            )
            
            if not data:
                return None

            # Check for GraphQL errors
            if 'errors' in data:
                error_msg = str(data.get('errors', [{}])[0].get('message', ''))
                
                # Handle the null trackUnion field error
                if 'NullValueInNonNullableField' in error_msg and '/trackUnion' in error_msg:
                    logger.warning(f"Track data missing for {track_id}, likely deleted or region-restricted")
                    # Return a minimal placeholder to avoid crashes
                    return {
                        'data': {
                            'trackUnion': {
                                'id': track_id,
                                'name': "Unavailable Track",
                                'playcount': 0,
                                'duration': {'totalMilliseconds': 0},
                                'firstArtist': {'items': []},
                                'otherArtists': {'items': []},
                                '__placeholder': True  # Mark this as placeholder
                            }
                        }
                    }
                
                logger.error(f"GraphQL errors in track response: {data['errors']}")
                return None

            # Check if track data is present
            if data.get('data', {}).get('trackUnion') is None:
                logger.error(f"No track data in response for {track_id}")
                return None

            return data

        except Exception as e:
            logger.error(f"Error fetching track {track_id}: {e}")
            return None
        finally:
            self.rate_limiter.release()

    @staticmethod
    def _format_track(track_data: Dict, detail: bool = False) -> Optional[Dict]:
        # For detail view, return the trackUnion data directly
        if detail:
            return track_data.get('data', {}).get('trackUnion', {})

        # Extract track data
        track = track_data.get('data', {}).get('trackUnion', {})
        if not track:
            logger.error("No trackUnion in response")
            return None

        # Check if this is a placeholder
        if track.get('__placeholder', False):
            # Return basic data for the placeholder
            return {
                'id': track.get('id', ''),
                'name': track.get('name', 'Unavailable Track'),
                'playcount': 0,
                'artistIds': [],
                'duration': 0,
            }

        # Get artist IDs from otherArtists and firstArtist
        artist_ids = []

        # Get artists from firstArtist
        if track.get('firstArtist', {}).get('items'):
            for artist in track['firstArtist']['items']:
                if artist.get('id'):
                    artist_ids.append(artist['id'])
                    #logger.info(f"Found artist ID from firstArtist: {artist['id']}")

        # Get artists from otherArtists
        if track.get('otherArtists', {}).get('items'):
            for artist in track['otherArtists']['items']:
                if artist.get('id'):
                    artist_ids.append(artist['id'])
                    #logger.info(f"Found artist ID from otherArtists: {artist['id']}")

        # Basic track data
        formatted_data = {
                'id': track.get('id'),
                'playcount': track.get('playcount'),
                'artistIds': artist_ids,
                'name': track.get('name'),
                'duration': track.get('duration', {}).get('totalMilliseconds'),
                }

        #logger.info(f"Formatted track data: {formatted_data}")
        if not formatted_data.get('id'):
            logger.warning(f"Track missing ID: {track.get('name', 'Unknown')}")
            # Try alternate ID sources
            if track.get('uri'):
                formatted_data['id'] = track['uri'].split(':')[-1]
                logger.info(f"Used URI as fallback ID: {formatted_data['id']}")

        return formatted_data

    async def _fetch_track_official(self, track_id: str) -> Optional[Dict]:
        """Fetch track data from Spotify's public API with client token"""
        try:
            await self.rate_limiter.acquire()

            if not await self._ensure_auth('track'):
                logger.error("Failed to get auth token for track API")
                return None

            if not self.session:
                self.session = aiohttp.ClientSession()

            if not self.proxy:
                logger.debug("No proxy available")
                return None

            url = f"https://api.spotify.com/v1/tracks/{track_id}"

            # Add client token to headers if available
            headers = {'Authorization': f'Bearer {self.access_token}'}

            if hasattr(self, 'client_token') and self.client_token:
                headers['client-token'] = self.client_token

            async with self.session.get(
                    url,
                    headers=headers,
                    proxy=self.proxy.url,
                    proxy_auth=self.proxy.auth,
                    timeout=10
                    ) as response:
                if response.status == 200:
                    return await response.json()
                elif response.status in {401, 407}:
                    logger.error(f"Authorization error {response.status}")
                    self.access_token = None
                    self.client_token = None
                    self.proxy = None
                    return None

                logger.warning(f"Failed to fetch track {track_id} from official API: {response.status}")
                return None

        except Exception as e:
            logger.error(f"Error fetching track {track_id} from official API: {e}")
            return None
        finally:
            self.rate_limiter.release()
